//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::GetPage(frame_id_t frame_id) -> Page * { return &pages_[frame_id]; }

void BufferPoolManagerInstance::AllocPageHelper(frame_id_t frame_id, page_id_t *page_id, Page *&page) {
  *page_id = AllocatePage();
  // flush old page
  page = GetPage(frame_id);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  if (page->GetPageId() != INVALID_PAGE_ID) {
    page_table_->Insert(page->GetPageId(), static_cast<frame_id_t>(-1));
  }
  page->Reset();

  // set new page
  page->SetPageId(*page_id);
  page->IncPinCount();

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(*page_id, frame_id);
}

auto BufferPoolManagerInstance::GetReplacementFrame(frame_id_t &frame_id) -> bool {
  bool res = false;
  if (!free_list_.empty()) {
    res = true;
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    res = true;
  } else {
    frame_id = -1;
  }
  return res;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (GetReplacementFrame(frame_id)) {
    AllocPageHelper(frame_id, page_id, page);
  }
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // find in buffer pool
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    // in page table
    if (-1 != frame_id) {
      page = GetPage(frame_id);
    } else if (GetReplacementFrame(frame_id)) {
      // flush a dirty frame
      page = GetPage(frame_id);
      // free list page need be clean
      if (page->IsDirty()) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
      }
      if (page->GetPageId() != INVALID_PAGE_ID) {
        page_table_->Insert(page->GetPageId(), static_cast<frame_id_t>(-1));
      }

      page->Reset();
      disk_manager_->ReadPage(page_id, page->GetData());
      page->SetPageId(page_id);
      page->IncPinCount();
      page_table_->Insert(page_id, frame_id);

      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
    }
  }
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool ret = false;
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    page = GetPage(frame_id);
    if (page->GetPinCount() > 0) {
      page->DecPinCount();
      page->SetIsDirty(is_dirty);
      if (0 == page->GetPinCount()) {
        replacer_->SetEvictable(frame_id, true);
      }
      ret = true;
    }
  }
  return ret;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool ret = FlushPgImpLockFree(page_id);
  return ret;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);
  Page *page = nullptr;
  for (size_t i = 0; i < pool_size_; i++) {
    page = GetPage(i);
    if (page->GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->SetIsDirty(false);
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool ret = true;
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    if (!page->IsPinned()) {
      if (page->IsDirty()) {
        disk_manager_->WritePage(page_id, page->GetData());
      }
      page->Reset();
      replacer_->Remove(frame_id);
      page_table_->Remove(page_id);
      DeallocatePage(page_id);
      free_list_.push_back(frame_id);
      ret = true;
    } else {
      ret = false;
    }
  }
  return ret;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::FlushPgImpLockFree(page_id_t page_id) -> bool {
  bool ret = false;
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    page = GetPage(frame_id);
    disk_manager_->WritePage(page_id, page->GetData());
    page->SetIsDirty(false);
    ret = true;
  }
  return ret;
}
}  // namespace bustub
