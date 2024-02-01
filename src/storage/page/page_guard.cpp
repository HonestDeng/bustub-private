#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (that.page_ != nullptr) {
    LOG_DEBUG("Enter BasicPageGuard with this.page_id = %d", that.page_->GetPageId());
  } else {
    LOG_DEBUG("Enter BasicPageGuard function");
  }
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (this->page_ != nullptr) {
    LOG_DEBUG("Enter Drop with this.page_id = %d", page_->GetPageId());
  } else {
    LOG_DEBUG("Enter Drop function");
  }

  if (this->page_ == nullptr) {
    LOG_DEBUG("Leave Drop with this->page = nullptr");
    return;
  }
  this->bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this->page_ != nullptr) {
    LOG_DEBUG("Enter assignment function with this.page_id = %d that.pid = %d", page_->GetPageId(),
              that.page_->GetPageId());
  } else {
    LOG_DEBUG("Enter assignment function");
  }

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

BasicPageGuard::~BasicPageGuard() {
  if (this->page_ != nullptr) {
    LOG_DEBUG("Enter Deconstructor with this.page_id = %d", page_->GetPageId());
  } else {
    LOG_DEBUG("Enter Deconstructor");
  }
  Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  // These functions need to guarantee that the protected page is not evicted from the buffer pool during the upgrade.
  // 怎么保证这一页不会被弹出？或者说，在或者page_的时候，我们不就已经把这个page_固定在buffer
  // pool中了，为什么还需要在这里保证呢？
  if (this->page_ != nullptr) {
    LOG_DEBUG("Enter UpgradeReade with this.page_id = %d", page_->GetPageId());
  } else {
    LOG_DEBUG("Enter UpgradeRead");
  }

  this->page_->RLatch();

  auto b = this->bpm_;
  auto p = this->page_;

  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
  return {b, p};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (this->page_ != nullptr) {
    LOG_DEBUG("Enter UpgradeWrite with this.page_id = %d", page_->GetPageId());
  } else {
    LOG_DEBUG("Enter UpgradeWrite");
  }
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
  if (guard_.page_ != nullptr) {
    LOG_DEBUG("Enter assignment function with this.page_id = %d that.pid = %d", guard_.PageId(), that.guard_.PageId());
  } else {
    LOG_DEBUG("Enter assignment function");
  }
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    LOG_DEBUG("Enter Drop with this.page_id = %d", guard_.page_->GetPageId());
  } else {
    LOG_DEBUG("Enter Drop function");
  }

  if (guard_.page_ == nullptr) {
    LOG_DEBUG("Leave Drop with this->page = nullptr");
    return;
  }
  auto p = guard_.page_;
  this->guard_.Drop();
  p->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {
  LOG_DEBUG("Enter Deconstructor of ReadPageGuard");
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (guard_.page_ != nullptr) {
    LOG_DEBUG("Enter assignment function with this.page_id = %d that.pid = %d", guard_.PageId(), that.PageId());
  } else {
    LOG_DEBUG("Enter assignment function");
  }
  this->Drop();
  this->guard_ = std::move(that.guard_);  // 这里会调用上面定义的移动赋值函数
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    LOG_DEBUG("Enter Drop with this.page_id = %d", guard_.page_->GetPageId());
  } else {
    LOG_DEBUG("Enter Drop function");
  }
  if (guard_.page_ == nullptr) {
    LOG_DEBUG("Leave Drop with this->page = nullptr");
    return;
  }
  auto p = guard_.page_;
  this->guard_.Drop();
  p->WUnlatch();
}

WritePageGuard::~WritePageGuard() {
  LOG_DEBUG("Enter Deconstructor of WritePageGuard");
  Drop();
}  // NOLINT

}  // namespace bustub
