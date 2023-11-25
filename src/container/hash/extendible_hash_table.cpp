//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

#define DEFAULT_HASH_TABLE_SIZE 128

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.reserve(DEFAULT_HASH_TABLE_SIZE);
  dir_.push_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

// template <typename K, typename V>
// auto ExtendibleHashTable<K, V>::LocalIndexOf(const K &key) -> size_t {
//   int index = IndexOf(key);
//   return index & ((1 << GetLocalDepthInternal(index)) - 1);
// }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(dir_[dir_index]->latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_.lock();
  auto index = IndexOf(key);
  auto bucket = dir_[index];

  // TODO(felix): to rwlock
  bucket->latch_.lock();
  latch_.unlock();
  bool ret = bucket->Find(key, value);
  // std::cout << "find, bucket:" << index << ", key:" << key << ", ret:" << ret << std::endl;
  bucket->latch_.unlock();
  return ret;
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  latch_.lock();
  // std::cout << "begin remove, bucket:"
  // << ", key:" << key << std::endl;
  int index = IndexOf(key);
  auto bucket = dir_[index];

  bucket->latch_.lock();
  latch_.unlock();
  bool ret = bucket->Remove(key);
  bucket->latch_.unlock();
  // std::cout << "end remove, bucket:" << index << ", key:" << key << ", ret:" << ret << std::endl;
  return ret;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertNoLock(const K &key, const V &value, int index) {
  std::shared_ptr<Bucket> &buck = dir_[index];
  (void)buck->Insert(key, value);
}

auto GetSufix(int index, int depth) -> int { return index & ((1 << depth) - 1); }

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  // std::cout << "begin insert, bucket:"
  // << ", key:" << key << std::endl;
  auto index = IndexOf(key);
  std::shared_ptr<Bucket> buck = dir_[index];
  buck->latch_.lock();
  auto depth = buck->GetDepth();
  if (buck->IsFull()) {
    int global_depth = GetGlobalDepthInternal();
    // // std::cout << "global depth:" << global_depth << ", depth:" << depth << std::endl;
    // full
    if (depth == global_depth) {
      IncreaseGlobalDepth();
      global_depth++;
    }
    // create new bucket and redistribute
    IncreaseLocalDepth(IndexOf(key), depth, global_depth, buck);
    buck->latch_.unlock();
    latch_.unlock();
    Insert(key, value);
  } else {
    latch_.unlock();
    (void)buck->Insert(key, value);
    buck->latch_.unlock();
  }
  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::IncreaseLocalDepth(int index, int depth, int global_depth,
                                                   std::shared_ptr<Bucket> &old_bucket) {
  {
    // 000 -> bucket1; 010 -> bucket1; 100 -> bucket1; 110 -> bucket1;
    // 000 -> bucket1; 100 -> bucket1; 010 -> bucket1'; 110 -> bucket1';
    std::shared_ptr<Bucket> bucket = std::make_shared<Bucket>(bucket_size_, depth + 1);
    std::scoped_lock<std::mutex> lock(bucket->latch_);

    int new_depth = depth + 1;
    int new_index = (1 << depth) | GetSufix(index, depth);
    int size = (1 << global_depth);
    // // std::cout << "index:" << index << ",new index:" << new_index << ",size:" << size << std::endl;
    for (int i = 0; i < size; i++) {
      int surfix = GetSufix(i, new_depth);
      if (surfix == new_index) {
        // std::scoped_lock<std::mutex> lock(bucket_latch_[i].lock());
        dir_[i] = bucket;
      }
    }
    old_bucket->IncrementDepth();
    RedistributeBucket(old_bucket);
    num_buckets_++;
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  auto list = bucket->GetItems();
  bucket->Reset();
  for (auto it = list.begin(); it != list.end();) {
    auto kv = *it;
    it = list.erase(it);
    auto new_index = IndexOf(kv.first);
    InsertNoLock(kv.first, kv.second, new_index);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ReserveBucket() -> void {
  ++global_depth_;
  int size = 1 << global_depth_;
  int ori_size = dir_.size();
  dir_.resize(size);
  for (int i = 0; i < ori_size; i++) {
    dir_[i + ori_size] = dir_[i];
  }
}
//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  bool res = false;
  for (auto &node : list_) {
    if (node.first == key) {
      value = node.second;
      res = true;
      break;
    }
  }
  return res;

  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  typename std::list<std::pair<K, V>>::iterator it;
  for (it = list_.begin(); it != list_.end(); ++it) {
    if (it->first == key) {
      break;
    }
  }
  if (it != list_.end()) {
    list_.erase(it);
    return true;
  }
  return false;

  // UNREACHABLE("not implemented");
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  if (IsFull()) {
    return false;
  }
  auto it = list_.begin();
  for (; it != list_.end(); ++it) {
    if (key == it->first) {
      it->second = value;
      break;
    }
  }
  if (it == list_.end()) {
    list_.emplace_back(key, value);
  }
  return true;
  // UNREACHABLE("not implemented");
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
