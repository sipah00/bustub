//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  
  frame_id_t frame_id;
  Page *page = GetFreePage(&frame_id);
  if(page == nullptr) {
    return nullptr;
  }
  
  page_id_t disk_page_id = AllocatePage();

  page->page_id_ = disk_page_id;
  *page_id = disk_page_id;

  page_table_[disk_page_id] = frame_id;
  replacer_->RecordAccess(frame_id);

  // disable eviction why here??
  replacer_->SetEvictable(frame_id, false);

  return page;
  }

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  if(page_table_.find(page_id) != end(page_table_)) {
    replacer_->RecordAccess(page_table_[page_id]);
    return &pages_[page_table_[page_id]];
  }
  frame_id_t frame_id;
  Page *page = GetFreePage(&frame_id);
  if(page == nullptr) {
    return nullptr;
  }
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, page->data_);

  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);

  // disable eviction why here??
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if(page_table_.find(page_id) != end(page_table_)) {
    Page *page = &pages_[page_table_[page_id]];
    if(page->pin_count_ == 0) {
      return false;
    } else {
      page->pin_count_--;
      if(page->pin_count_ == 0) {
        replacer_->SetEvictable(page_table_[page_id], true);
      }
      page->is_dirty_ = is_dirty;
      return true;
    }
  }
  return false;
}

auto BufferPoolManager::GetFreePage(frame_id_t *frame_id) -> Page * {
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id_t f_id = free_list_.front();
    page = &pages_[f_id];
    *frame_id = f_id;
  } else {
    frame_id_t *p;
    if(replacer_->Evict(p)) {
      page = &pages_[*p];
      if(page->IsDirty()) {
        disk_manager_->WritePage(page->page_id_, page->GetData());
      }
      page_table_.erase(page->GetPageId());
      *frame_id = (*p);
      page->ResetMemory();
      
    }
  }
  return page;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
    assert(page_id != INVALID_PAGE_ID);

    if(page_table_.find(page_id) == end(page_table_)) {
      return false;
    }

    Page *page = &pages_[page_table_[page_id]];
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;

    return true;

  }

void BufferPoolManager::FlushAllPages() {
  for(auto [page_id, frame_id] : page_table_) {
    FlushPage(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  if(page_table_.find(page_id) == end(page_table_)) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if(page->GetPinCount() > 0) {
    return false;
  }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);
  page->Reset();

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
