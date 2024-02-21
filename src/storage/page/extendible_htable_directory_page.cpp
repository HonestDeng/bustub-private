//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

int ExtendibleHTableDirectoryPage::page_id;

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) { this->max_depth_ = max_depth; }

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  auto index = hash & GetGlobalDepthMask();
  return index;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

auto ExtendibleHTableDirectoryPage::HashToBucketPageId(uint32_t hash) const -> page_id_t {
  auto index = hash & ~(0xffffffff << global_depth_);
  return bucket_page_ids_[index];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

// split_image_index的定义是bucket_idx指向的bucket分裂之后指向新bucket的entry的idx
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << (local_depths_[bucket_idx]));  // 001的split image是101
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndexNoOver(uint32_t bucket_idx) const -> uint32_t {
  if(local_depths_[bucket_idx] >= global_depth_ && global_depth_ > 0) {
    return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));  // 001的split image是101
  }
  return bucket_idx ^ (1 << (local_depths_[bucket_idx]));  // 001的split image是101
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
  return global_depth_;
}

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  int offset = 1 << global_depth_;
  // memcpy拷贝的单位是字节(byte)，因此拷贝的长度是需要拷贝的单个item的大小*item的个数
  std::memcpy(bucket_page_ids_ + offset, bucket_page_ids_, offset * sizeof(bucket_page_ids_[0]));
  std::memcpy(local_depths_ + offset, local_depths_, offset * sizeof(local_depths_[0]));
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  uint32_t len = (1 << global_depth_);
  for (uint32_t i = 0; i < len; i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (1 << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // 调用时需要保证local_depths[bucket_idx] < global_depth
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return ~(0xffffffff << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return ~(0xffffffff << local_depths_[bucket_idx]);
}
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

void ExtendibleHTableDirectoryPage::PrintDirectory1(uint page_id_, BufferPoolManager *bpm) {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u, page id = %u) ========", global_depth_, page_id_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth | size");
  bool do_exit = true;
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    auto bucket_guard = bpm->FetchPageBasic(bucket_page_ids_[idx]);
    auto bucket = bucket_guard.template As<ExtendibleHTableBucketPage<int, int, IntComparator>>();
    if(bucket->Size() > 0) {
      do_exit = false;
    }
    LOG_DEBUG("|    %u    |    %u    |    %u    |    %u    |", idx, bucket_page_ids_[idx], local_depths_[idx], bucket->Size());
  }
  LOG_DEBUG("================ END DIRECTORY ================");
  if(do_exit && global_depth_ > 0) {
    exit(111);
  }
}


}  // namespace bustub
