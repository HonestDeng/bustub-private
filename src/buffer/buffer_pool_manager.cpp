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
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  LOG_DEBUG("Create a buffer pool manager with: pool_size = %zu, replacer_k = %zu", pool_size, replacer_k);
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  LOG_DEBUG("Destroy Buffer Pool Manager.");
  delete[] pages_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  auto t = std::this_thread::get_id();
  uint32_t a = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %u, Enter NewPage", a);
  latch_.lock();
  // 如果free_list中有空闲的frame
  Page *ret = nullptr;
  if (!free_list_.empty()) {
    auto frame_id = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    pages_[frame_id].page_id_ = *page_id;
    // 记录page存放在哪一个frame中
    page_table_[*page_id] = frame_id;

    ret = pages_ + frame_id;
  } else if (replacer_->Size() != 0) {
    // 如果bufferpool中有evictable，那么就将这个evitable页写回内存
    // 然后将腾出来的frame放新的page
    // 腾出空间
    frame_id_t frame_id;
    replacer_->Evict(&frame_id);
    BUSTUB_ASSERT(pages_[frame_id].pin_count_ == 0, "pin_count not equal 0");
    // 如果page被修改过，则写回到磁盘中
    if (pages_[frame_id].is_dirty_ && pages_[frame_id].pin_count_ == 0) {
      // 因为这是evictable的，pin_count应该等于0
      //      latch_.unlock();
      FlushPage(pages_[frame_id].page_id_);
      //      latch_.lock();
    }
    page_table_.erase(pages_[frame_id].page_id_);  // 删除原本的映射

    // allocate new page
    *page_id = AllocatePage();
    pages_[frame_id].page_id_ = *page_id;
    page_table_[*page_id] = frame_id;

    ret = pages_ + frame_id;
  }
  if (ret != nullptr) {
    // reset memory and metadata
    ret->ResetMemory();
    ret->pin_count_ = 1;  // the current thread pin this page initially
    ret->is_dirty_ = false;
    ret->page_id_ = *page_id;
    // set the replacer
    replacer_->RecordAccess(page_table_[*page_id]);  // 先记录访问，然后再设置non-evictable
    replacer_->SetEvictable(page_table_[*page_id], false);
    BUSTUB_ASSERT(*page_id == ret->page_id_, "Page ID not match");
  }
  latch_.unlock();
  LOG_DEBUG("new a page with id = %d", *page_id);
  // 没有空余的frame可以存放page了
  return ret;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto t = std::this_thread::get_id();
  uint32_t a = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %u, Enter FetchPage with: page_id = %d", a, page_id);
  latch_.lock();
  // if the page requested in the pool
  if (page_table_.count(page_id) > 0) {
    auto frame_id = page_table_[page_id];
    // set the replacer
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    pages_[frame_id].pin_count_++;
    BUSTUB_ASSERT(page_id == (pages_ + frame_id)->page_id_, "Page ID not match");
    latch_.unlock();
    return pages_ + frame_id;
  }

  Page *ret = nullptr;
  // if there is a free frame
  if (!free_list_.empty()) {
    auto frame_id = free_list_.front();
    free_list_.pop_front();
    ret = pages_ + frame_id;
    ret->page_id_ = page_id;
    // 记录page存放在哪一个frame中
    page_table_[page_id] = frame_id;

    // 从磁盘读取数据
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({/*is_write=*/false, ret->GetData(), /*page_id=*/page_id, std::move(promise1)});
    // if fail to read
    BUSTUB_ASSERT(future1.get() == true, "fail to read page");
  } else if (replacer_->Size() > 0) {  // if there is a evictable page
    // 腾出空间
    frame_id_t frame_id;
    replacer_->Evict(&frame_id);
    ret = pages_ + frame_id;
    page_table_[page_id] = frame_id;
    BUSTUB_ASSERT(pages_[frame_id].pin_count_ == 0, "pin_count not equal 0");
    // 如果page被修改过，则写回到磁盘中
    if (ret->is_dirty_ && ret->pin_count_ == 0) {
      // 因为这是evictable的，pin_count应该等于0
      //      latch_.unlock();
      FlushPage(ret->page_id_);
      //      latch_.lock();
    }
    page_table_.erase(pages_[frame_id].page_id_);  // 删除原本的映射

    // 从磁盘读取数据
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({/*is_write=*/false, ret->GetData(), /*page_id=*/page_id, std::move(promise1)});
    // if fail to read
    BUSTUB_ASSERT(future1.get() == true, "fail to read page");
  }

  if (ret != nullptr) {
    // reset memory and metadata
    ret->pin_count_ = 1;
    ret->is_dirty_ = false;
    ret->page_id_ = page_id;
    // set the replacer
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    LOG_DEBUG("Fetch Page with page_id = %d", ret->page_id_);
    BUSTUB_ASSERT(page_id == ret->page_id_, "Page ID not match");
    latch_.unlock();
    return ret;
  }
  LOG_DEBUG("fail to fetch page.");
  latch_.unlock();
  return ret;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto t = std::this_thread::get_id();
  uint32_t a = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %u, Enter UnpinPage with page_id = %d, is_dirty = %d", a, page_id, is_dirty);
  latch_.lock();
  // page not in the pool
  if (page_table_.count(page_id) == 0) {
    LOG_DEBUG("page id not found in the page table");
    latch_.unlock();
    return false;
  }

  Page *page = pages_ + page_table_[page_id];
  // pin_count is already 0
  if (page->pin_count_ == 0) {
    LOG_DEBUG("pin_count already zero");
    latch_.unlock();
    return false;
  }

  page->pin_count_--;
  if (page->pin_count_ == 0) {
    // set the page evictable
    LOG_DEBUG("pin_count reach zero");
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  page->is_dirty_ = is_dirty || page->is_dirty_;
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  LOG_DEBUG("Enter FlushPage with page_id = %uz", page_id);
  //  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    //    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  // 将数据写入磁盘
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, page->GetData(), /*page_id=*/page_id, std::move(promise1)});
  // if fail to read
  BUSTUB_ASSERT(future1.get() == true, "fail to read page");

  // clean meta data
  page->is_dirty_ = false;

  //  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  LOG_DEBUG("Enter FlushAllPages");
  //  latch_.lock();
  for (const auto &item : page_table_) {
    auto &page_id = item.first;
    FlushPage(page_id);
  }
  //  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();

  // 1.
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  // 2.
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  if (page->is_dirty_) {
    FlushPage(page_id);
  }
  // delete in disk in here
  DeallocatePage(page_id);

  page_table_.erase(page_id);
  // reset metadata
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  // return it to the free list

  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  LOG_DEBUG("Enter AllocatePage");
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  LOG_DEBUG("Enter FetchPageBasic");
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  LOG_DEBUG("Enter FetchPageRead");
  auto ret = FetchPage(page_id);
  ret->RLatch();
  return {this, ret};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  LOG_DEBUG("Enter FetchPageWrite");
  auto ret = FetchPage(page_id);
  ret->WLatch();
  return {this, ret};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  LOG_DEBUG("Enter NewPageGuarded");
  return {this, NewPage(page_id)};
}

}  // namespace bustub