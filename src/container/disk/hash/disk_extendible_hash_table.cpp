//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {
void Print(const RID x) {
  std::cout << x << std::endl;
}

void Print(const unsigned int x) {
  std::cout << x << std::endl;
}
template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  LOG_DEBUG("create a htable with buffer size = %zu, header_max_depth = %d, directory_max_depth = %d, bucket_max_size = %d",
            bpm_->GetPoolSize(), header_max_depth, directory_max_depth, bucket_max_size);
  header_page_id_ = INVALID_PAGE_ID;
  bpm_->NewPageGuarded(&header_page_id_);
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  LOG_DEBUG("Enter GatValue with key = %u", Hash(key));
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto dir_page_id = header_page->HashToDirectoryPageId(hash);
  if (dir_page_id == 0 || dir_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }

  guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = guard.template As<ExtendibleHTableDirectoryPage>();
  auto bucket_page_id = dir_page->HashToBucketPageId(hash);
  if (bucket_page_id == 0 || bucket_page_id == static_cast<page_id_t>(INVALID_PAGE_ID)) {
    return false;
  }

  auto guard_bucket = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = guard_bucket.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  result->resize(1);
  if (bucket_page->Lookup(key, result->at(0), cmp_)) {
    return true;
  }
  result->pop_back();
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  LOG_DEBUG("Enter Insert with key = %u, value = ", Hash(key));
  Print(value);
  // 一个问题是，如果key已经出现过了应该怎么办？是拒绝插入，还是覆盖原来的value？
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);

  // 怎么知道hash对应的那个directory page存在
  auto hash = Hash(key);
  auto dir_page_id = header_page->HashToDirectoryPageId(hash);
  if (dir_page_id == 0 || dir_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    // hash对应的那个directory page还不存在，生成一张directory page
    auto idx = header_page->HashToDirectoryIndex(hash);
    return InsertToNewDirectory(header_page, idx, hash, key, value);
  }

  // hash对应的那个directory page已经存在了
  guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = guard.template AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);
  auto bucket_page_id = dir_page->HashToBucketPageId(hash);
  if (bucket_page_id == 0 || bucket_page_id == static_cast<page_id_t>(INVALID_PAGE_ID)) {
    auto idx = dir_page->HashToBucketIndex(hash);
    return InsertToNewBucket(dir_page, idx, key, value);
  }

  auto guard_bucket = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = guard_bucket.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if(bucket_page->IsExist(key, cmp_)) {
    return false;
  }
  while (bucket_page->IsFull()) {
    auto bucket_idx = dir_page->HashToBucketIndex(hash);
    if (dir_page->GetLocalDepth(bucket_idx) >= dir_page->GetMaxDepth()) {
      // 如果bucket已经满了，已经无法再扩张了，那么就无法再插入键值对
      return false;
    }

    // 如果这个bucket_page已经满了，则需要将这个page切分成两份
    auto image_idx = dir_page->GetSplitImageIndex(bucket_idx);
    // 增加这两个互为镜像的page的depth
    dir_page->IncrLocalDepth(bucket_idx);
    dir_page->IncrLocalDepth(image_idx);

    // create a new bucket page
    page_id_t new_page_id;
    bpm_->NewPageGuarded(&new_page_id);  // 自动UnpinPage
    auto new_page_guard = bpm_->FetchPageWrite(new_page_id);
    auto bucket_image_page = new_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_image_page->Init(bucket_max_size_);
    UpdateDirectoryMapping(dir_page, image_idx, new_page_id);

    // 根据key将bucket_page中的一部分键值对迁移到bucket_image_page中
    MigrateEntries(bucket_page, bucket_image_page, image_idx, dir_page->GetLocalDepthMask(image_idx));

    // 如果hash与新生成的page的idx对应
    if ((hash | dir_page->GetLocalDepthMask(image_idx)) == image_idx) {
      bucket_page = bucket_image_page;
    }
  }
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // create a new page as a directory page
  page_id_t new_page_id;
  bpm_->NewPageGuarded(&new_page_id);
  header->SetDirectoryPageId(directory_idx, new_page_id);

  auto dir_guard = bpm_->FetchPageWrite(new_page_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);
  auto bucket_idx = dir_page->HashToBucketIndex(hash);

  // the new directory has no bucket, so use InsertToNewBucket
  return InsertToNewBucket(dir_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_page_id;
  bpm_->NewPageGuarded(&new_page_id);
  // Don't forget to update meta-data
  UpdateDirectoryMapping(directory, bucket_idx, new_page_id);
  auto bucket_guard = bpm_->FetchPageWrite(new_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (size_t i = 0; i < old_bucket->Size(); i++) {
    auto key = old_bucket->KeyAt(i);
    auto hash = Hash(key);
    if ((hash & local_depth_mask) == new_bucket_idx) {
      auto value = old_bucket->ValueAt(i);
      old_bucket->Remove(key, cmp_);
      new_bucket->Insert(key, value, cmp_);
      i--;
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  LOG_DEBUG("Enter Remove with key = %u", Hash(key));
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);

  // 怎么知道hash对应的那个directory page存在
  auto hash = Hash(key);
  auto dir_page_id = header_page->HashToDirectoryPageId(hash);
  if (dir_page_id == 0 || dir_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }

  // hash对应的那个directory page已经存在了
  guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = guard.template AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);
  auto bucket_page_id = dir_page->HashToBucketPageId(hash);
  if (bucket_page_id == 0 || bucket_page_id == static_cast<page_id_t>(INVALID_PAGE_ID)) {
    return false;
  }

  auto guard_bucket = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = guard_bucket.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto ret = bucket_page->Remove(key, cmp_);
  if (!ret) {
    return ret;
  }

  // 删除成功，接下来考虑能不能合并一些bucket
  auto bucket_idx = dir_page->HashToBucketIndex(hash);
  while (dir_page->GetLocalDepth(bucket_idx) > 0) {
    auto split_idx = dir_page->GetSplitImageIndexNoOver(bucket_idx);
    auto split_page_id = dir_page->GetBucketPageId(split_idx);
    if(split_page_id == bucket_page_id) {
      // 如果这两个是同一个page，则不需要合并
      break;
    }
    auto split_guard = bpm_->FetchPageWrite(split_page_id);
    auto split_page = split_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket_page->IsEmpty() && !split_page->IsEmpty()) {
      // 如果两个bucket都不为空，那么就无法合并，直接退出函数
      break;
    }
    if (dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(split_idx)) {
      // 如果两个bucket的深度不相同，那也无法合并
      break;
    }

    dir_page->DecrLocalDepth(bucket_idx);
    dir_page->DecrLocalDepth(split_idx);
    auto merge_idx = std::min(bucket_idx, split_idx);
    auto source_idx = std::max(bucket_idx, split_idx);

    // merge page and delete the old page
    if (source_idx == bucket_idx) {
      MigrateEntries(bucket_page, split_page,
                     merge_idx, dir_page->GetLocalDepthMask(bucket_idx));
      guard_bucket = std::move(split_guard);
      bucket_page = guard_bucket.template AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
      bucket_page_id = split_page_id;
      bucket_idx = split_idx;
      UpdateDirectoryMapping(dir_page, source_idx, split_page_id);
      bpm_->DeletePage(bucket_page_id);
    }else{
      MigrateEntries(split_page, bucket_page,
                     merge_idx, dir_page->GetLocalDepthMask(bucket_idx));
      UpdateDirectoryMapping(dir_page, source_idx, bucket_page_id);
      bpm_->DeletePage(bucket_page_id);
    }
  }

  // 检查directory是否可以收缩
  if(!dir_page->CanShrink()) {
    return ret;
  }
  // 收缩directory
  dir_page->DecrGlobalDepth();

  return ret;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
