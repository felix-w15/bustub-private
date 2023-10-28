//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : k_evi_list_(k, -1), kless_evi_list_(k, -1), replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto &frame_node_pair : lru_access_map_) {
    (void)DeleteNode(frame_node_pair.second);
  }
}

auto LRUKReplacer::GetCurrentTime() -> size_t {
  // using namespace std::chrono;
  // return duration_cast< milliseconds >(system_clock::now().time_since_epoch()).count();
  return current_timestamp_.fetch_add(1, std::memory_order_relaxed);
}
void UnLinkNode(LRUNode *node) {
  node->pre_->next_ = node->next_;
  node->next_->pre_ = node->pre_;
  node->pre_ = node;
  node->next_ = node;
}

auto LRUKReplacer::EviNode() -> LRUNode * {
  LRUNode *node = GetKlessEviNode() != nullptr ? GetKlessEviNode() : GetKEvitNode();
  if (nullptr != node) {
    UnLinkNode(node);
    lru_access_map_.erase(node->GetFrameId());
    curr_size_--;
  }
  return node;
}

auto LRUKReplacer::DeleteNode(LRUNode *node) -> bool {
  bool res = false;
  if (nullptr != node) {
    delete node;
    node = nullptr;
    res = true;
  }
  return res;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  BUSTUB_ASSERT(frame_id != nullptr, "frame id ptr should not be null!");
  std::lock_guard<std::mutex> lock(latch_);
  bool res = false;
  if (curr_size_ > 0) {
    LRUNode *node = EviNode();
    *frame_id = node->GetFrameId();
    (void)DeleteNode(node);
    res = true;
  }
  return res;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT((size_t)frame_id <= replacer_size_, "frame id should smaller than replacer_size");
  std::lock_guard<std::mutex> lock(latch_);
  size_t current_time = GetCurrentTime();
  LRUNode *node = nullptr;
  if (lru_access_map_.end() == lru_access_map_.find(frame_id)) {
    // first access
    node = new LRUNode(k_, frame_id);
    lru_access_map_[frame_id] = node;
  } else {
    // has accessed
    node = lru_access_map_[frame_id];
  }
  node->Access(current_time);
  if (node->Evitable() && node->AccessCount() == k_) {
    UnLinkNode(node);
    InsertNodeToList(&k_evi_list_, node);
  }
}

void LinkNode(LRUNode *pre, LRUNode *node) {
  node->pre_ = pre;
  node->next_ = pre->next_;
  pre->next_->pre_ = node;
  pre->next_ = node;
}

void InsertNodeToList(LRUNode *list, LRUNode *node) {
  LRUNode *head = list;
  LRUNode *node_ptr = head;
  LRUNode *next = nullptr;
  // circle to head, stop
  while (head != next) {
    next = node_ptr->next_;
    if (head == next || node->AccessTime() < next->AccessTime()) {
      // tail || ealier than next
      LinkNode(node_ptr, node);
    }
    node_ptr = next;
  }
}

void LRUKReplacer::AddNodeToList(LRUNode *node) {
  if (node->AccessCount() >= k_) {
    InsertNodeToList(&k_evi_list_, node);
  } else {
    InsertNodeToList(&kless_evi_list_, node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  LRUNode *node = lru_access_map_[frame_id];

  BUSTUB_ASSERT(nullptr != node, "node should have accessed before evict!");
  bool last_evi_state = node->Evitable();
  bool new_evi_state = set_evictable;
  node->SetEvitable(new_evi_state);

  if (last_evi_state && !new_evi_state) {
    // evi -> not evi
    UnLinkNode(node);
    curr_size_--;
  } else if (!last_evi_state && new_evi_state) {
    // not evi -> evi
    AddNodeToList(node);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  LRUNode *node = lru_access_map_[frame_id];
  if (nullptr != node) {
    UnLinkNode(node);
    (void)DeleteNode(node);
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_.load(std::memory_order_relaxed); }

}  // namespace bustub
