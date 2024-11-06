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
#include <future>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// 将一个page加入到缓存池中，如果无空闲或都不可驱逐返回nullptr。
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> ulock(latch_);
  // 先找出page在缓存池中地址
  Page *page_add = nullptr;
  frame_id_t frame_id = 0;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();

  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  page_add = pages_ + frame_id;  // 首地址加num会自动变成加num*sizeof(Page)

  if (page_add->IsDirty()) {
    std::promise<bool> pr = disk_scheduler_->CreatePromise();
    std::future<bool> fu = pr.get_future();
    disk_scheduler_->Schedule({true, page_add->GetData(), page_add->GetPageId(), std::move(pr)});
    fu.get();  // 阻塞到将写回脏页请求加入队列才将脏页改成不脏
    page_add->is_dirty_ = false;
  }

  // 清除原本page，插入新page
  *page_id = AllocatePage();
  page_table_.erase(page_add->GetPageId());
  page_table_.insert(std::pair<page_id_t, frame_id_t>(*page_id, frame_id));
  // 更新page_add内容，page_add->data_未要求
  page_add->page_id_ = *page_id;
  page_add->pin_count_++;
  page_add->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page_add;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> ulock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  Page *fetched_page = nullptr;
  frame_id_t frame_id = -1;
  // 缓存区已有
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    fetched_page = pages_ + frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    fetched_page->pin_count_++;
    return fetched_page;
  }

  // 缓存池中没有，类似NewPage,只是page_id要用给的
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }
  fetched_page = pages_ + frame_id;  // 首地址加num会自动变成加num*sizeof(Page)

  if (fetched_page->IsDirty()) {
    std::promise<bool> pr1 = disk_scheduler_->CreatePromise();
    std::future<bool> fu1 = pr1.get_future();
    disk_scheduler_->Schedule({true, fetched_page->GetData(), fetched_page->GetPageId(), std::move(pr1)});
    fu1.get();  // 阻塞到将写回脏页请求加入队列才将脏页改成不脏
    fetched_page->is_dirty_ = false;
  }

  // 清除原本page，插入新page
  page_table_.erase(fetched_page->GetPageId());
  page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, frame_id));
  // 更新page_add内容，page_add->data_未要求
  fetched_page->page_id_ = page_id;
  fetched_page->pin_count_++;
  fetched_page->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 读入内存
  std::promise<bool> pr2 = disk_scheduler_->CreatePromise();
  std::future<bool> fu2 = pr2.get_future();
  disk_scheduler_->Schedule({false, fetched_page->GetData(), fetched_page->GetPageId(), std::move(pr2)});
  fu2.get();

  return fetched_page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> sclock(latch_);
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].GetPinCount() == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *unpinned_page = pages_ + frame_id;
  unpinned_page->pin_count_--;
  if (unpinned_page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  unpinned_page->is_dirty_ = unpinned_page->is_dirty_ || is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> sclock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *flushed_page = pages_ + frame_id;
  std::promise<bool> pr = disk_scheduler_->CreatePromise();
  std::future<bool> fu = pr.get_future();
  disk_scheduler_->Schedule({true, flushed_page->GetData(), flushed_page->GetPageId(), std::move(pr)});
  fu.get();
  flushed_page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> sclock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page *flushed_page = pages_ + i;
    if (flushed_page->GetPageId() == INVALID_PAGE_ID) {
      continue;
    }
    std::promise<bool> pr = disk_scheduler_->CreatePromise();
    std::future<bool> fu = pr.get_future();
    disk_scheduler_->Schedule({true, flushed_page->GetData(), flushed_page->GetPageId(), std::move(pr)});
    fu.get();
    flushed_page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> sc(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *delete_page = pages_ + frame_id;
  if (delete_page->GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(delete_page->GetPageId());
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  delete_page->ResetMemory();
  delete_page->page_id_ = INVALID_PAGE_ID;
  delete_page->pin_count_ = 0;
  delete_page->is_dirty_ = false;

  DeallocatePage(delete_page->GetPageId());
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
