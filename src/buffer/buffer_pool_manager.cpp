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
  
  Page *page = GetFreePage();
  if(page == nullptr) {
    page_id = nullptr;
    return nullptr;
  }
  
  page_id_t disk_page_id = AllocatePage();

  page->page_id_ = disk_page_id;
  *page_id = disk_page_id;

  return page;
  }

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  for(int i = 0; i < pool_size_; i++) {  // may be till only next_page id
    if(pages_[i].page_id_ == page_id) {
      return &pages_[i];
    }
  }
  Page *page = GetFreePage();
  if(page == nullptr) {
    return page;
  }
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, page->data_);

  replacer_->RecordAccess(page_table_[page_id]);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  for(int i = 0; i < pool_size_; i++) {
    Page *page = &pages_[i];
    if(page->page_id_ == page_id) {
      if(page->pin_count_ == 0) {
        return false;
      } else {
        page->pin_count_--;
        if(page->pin_count_ == 0) {
          
        }
        page->is_dirty_ = is_dirty;
        return true;
      }
    }
  }
  return false;
}

auto BufferPoolManager::GetFreePage() -> Page * {
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id_t f_id = free_list_.front();
    page = &pages_[f_id];
  } else {
    frame_id_t *p;
    if(replacer_->Evict(p)) {
      page = &pages_[*p];
      if(page->IsDirty()) {
        disk_manager_->WritePage(page->page_id_, page->GetData());
        page->is_dirty_ = false;
        page->pin_count_ = 0;
      }
    }
  }
  return page;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
    assert(page_id != INVALID_PAGE_ID);

    frame_id_t frame_id = page_table_[page_id];
    if(frame_id == -1) {
      return false;
    }

    Page *page = &pages_[frame_id];
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
