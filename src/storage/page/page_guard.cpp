#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (this->page_ == nullptr) {
    return;
  }
  this->bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  // 用新的Page代替久的Page
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  // 使that不可用
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // These functions need to guarantee that the protected page is not evicted from the buffer pool during the upgrade.
  // 怎么保证这一页不会被弹出？或者说，在或者page_的时候，我们不就已经把这个page_固定在buffer
  // pool中了，为什么还需要在这里保证呢？
  this->page_->RLatch();

  auto b = this->bpm_;
  auto p = this->page_;

  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
  return {b, p};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();

  auto b = this->bpm_;
  auto p = this->page_;

  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
  return {b, p};
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  auto p = guard_.page_;
  this->guard_.Drop();
  p->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);  // 这里会调用上面定义的移动赋值函数
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  auto p = guard_.page_;
  this->guard_.Drop();
  p->WUnlatch();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
