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
#define TID() syscall(__NR_gettid)

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
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %u, Enter NewPage", tid);
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
    frame_id_t frame_id;
    replacer_->Evict(&frame_id);
    BUSTUB_ASSERT(pages_[frame_id].pin_count_ == 0, "pin_count not equal 0");
    // 如果page被修改过，则写回到磁盘中
    if (pages_[frame_id].is_dirty_ && pages_[frame_id].pin_count_ == 0) {
      // 因为这是evictable的，pin_count应该等于0
      FlushPageNoLock(pages_[frame_id].page_id_);
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
  }
  latch_.unlock();
  LOG_DEBUG("tid = %u, new a page with id = %d", tid, *page_id);
  // 没有空余的frame可以存放page了
  return ret;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %u, Enter FetchPage with: page_id = %d", tid, page_id);
  latch_.lock();
  // if the page requested in the pool
  if (page_table_.count(page_id) > 0) {
    auto frame_id = page_table_[page_id];
    // set the replacer
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    pages_[frame_id].pin_count_++;

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
    bool c = future1.get();
    if (!c) {
      std::cout << "Fail to read a page with page id = from disk" << page_id << std::endl;
      std::terminate();
    }
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
      FlushPageNoLock(ret->page_id_);
    }
    page_table_.erase(pages_[frame_id].page_id_);  // 删除原本的映射

    // 从磁盘读取数据
    auto promise1 = disk_scheduler_->CreatePromise();
    auto future1 = promise1.get_future();
    disk_scheduler_->Schedule({/*is_write=*/false, ret->GetData(), /*page_id=*/page_id, std::move(promise1)});
    // if fail to read
    bool c = future1.get();
    if (!c) {
      std::cout << "Fail to read a page with page id = from disk" << page_id << std::endl;
      std::terminate();
    }
  }

  if (ret != nullptr) {
    // reset memory and metadata
    ret->pin_count_ = 1;
    ret->is_dirty_ = false;
    ret->page_id_ = page_id;
    // set the replacer
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    LOG_DEBUG("tid = %u, Fetch Page with page_id = %d", tid, ret->page_id_);

    latch_.unlock();
    return ret;
  }
  LOG_DEBUG("tid = %d, fail to fetch page.", tid);
  latch_.unlock();
  return ret;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %u, Enter UnpinPage with page_id = %d, is_dirty = %d", tid, page_id, is_dirty);
  latch_.lock();
  // 1. 如果page_table中就没有
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  // 2. 找到要被unpin的page
  frame_id_t unpinned_fid = iter->second;
  Page *unpinned_page = &pages_[unpinned_fid];
  if (is_dirty) {
    unpinned_page->is_dirty_ = true;
  }
  // if page的pin_count == 0 则直接return
  if (unpinned_page->pin_count_ == 0) {
    latch_.unlock();
    return false;
  }
  unpinned_page->pin_count_--;
  if (unpinned_page->GetPinCount() == 0) {
    replacer_->SetEvictable(unpinned_fid, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FlushPage with page_id = %uz", tid, page_id);
  latch_.lock();
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  // 将数据写入磁盘
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, page->GetData(), /*page_id=*/page_id, std::move(promise1)});
  // if fail to read
  bool c = future1.get();
  if (!c) {
    std::cout << "Fail to read a page with page id = from disk" << page_id << std::endl;
    latch_.unlock();
    std::terminate();
  }

  // clean meta data
  page->is_dirty_ = false;

  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPageNoLock(page_id_t page_id) -> bool {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FlushPageNoLock with page_id = %uz", tid, page_id);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;

  // 将数据写入磁盘
  auto promise1 = disk_scheduler_->CreatePromise();
  auto future1 = promise1.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, page->GetData(), /*page_id=*/page_id, std::move(promise1)});
  // if fail to read
  bool c = future1.get();
  if (!c) {
    std::cout << "Fail to read a page with page id = from disk" << page_id << std::endl;
    std::terminate();
  }

  // clean meta data
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FlushAllPage", tid);
  latch_.lock();
  for (const auto &item : page_table_) {
    auto &page_id = item.first;
    FlushPageNoLock(page_id);
  }
  latch_.unlock();
}

void BufferPoolManager::FlushAllPagesNoLock() {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FlushAllPagesNoLock", tid);
  for (const auto &item : page_table_) {
    auto &page_id = item.first;
    FlushPageNoLock(page_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter DeletePage with page_id = %uz", tid, page_id);

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
    FlushPageNoLock(page_id);
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
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter AllocatePage", tid);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FetchPageBasic with page_id = %uz", tid, page_id);
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FetchPageRead with page_id = %uz", tid, page_id);
  auto ret = FetchPage(page_id);
  ret->RLatch();
  return {this, ret};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter FetchPageWrite with page_id = %uz", tid, page_id);
  auto ret = FetchPage(page_id);
  ret->WLatch();
  return {this, ret};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto t = std::this_thread::get_id();
  uint32_t tid = *reinterpret_cast<uint32_t *>(&t);
  LOG_DEBUG("tid = %d, Enter NewPageGuarded", tid);
  return {this, NewPage(page_id)};
}

}  // namespace bustub