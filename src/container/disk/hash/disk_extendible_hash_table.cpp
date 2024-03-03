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
auto ToString(const RID x) -> std::string { return x.ToString(); }

auto ToString(const unsigned int x) -> std::string { return std::to_string(x); }

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
  header_page_id_ = INVALID_PAGE_ID;
  bpm_->NewPageGuarded(&header_page_id_);
  auto guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);

  for (uint32_t i = 0; i < header_page->MaxSize(); i++) {
    header_page->SetDirectoryPageId(i, INVALID_PAGE_ID);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.template As<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto dir_page_id = header_page->HashToDirectoryPageId(hash);
  guard.Drop();
  if (dir_page_id == 0 || dir_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }

  guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = guard.template As<ExtendibleHTableDirectoryPage>();
  auto bucket_page_id = dir_page->HashToBucketPageId(hash);
  if (bucket_page_id == 0 || bucket_page_id == static_cast<page_id_t>(INVALID_PAGE_ID)) {
    return false;
  }
  guard.Drop();

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
    LOG_DEBUG("Enter Insert with key = %u", Hash(key));
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
  auto directory = guard.template AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  auto bucket_page_id = directory->HashToBucketPageId(hash);
  if (bucket_page_id == 0 || bucket_page_id == static_cast<page_id_t>(INVALID_PAGE_ID)) {
    auto idx = directory->HashToBucketIndex(hash);
    return InsertToNewBucket(directory, idx, key, value);
  }

  auto guard_bucket = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = guard_bucket.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->IsExist(key, cmp_)) {
    return false;
  }
  while (bucket_page->IsFull()) {
    auto bucket_idx = directory->HashToBucketIndex(hash);
    if (directory->GetLocalDepth(bucket_idx) >= directory->GetMaxDepth()) {
      // 如果bucket已经满了，已经无法再扩张了，那么就无法再插入键值对
      return false;
    }

    // 如果这个bucket_page已经满了，则需要将这个page切分成两份
    auto split_image_idx = directory->GetSplitImageIndex(bucket_idx);

    // create a new bucket page
    page_id_t split_page_id;
    bpm_->NewPageGuarded(&split_page_id);  // 自动UnpinPage
    auto new_page_guard = bpm_->FetchPageWrite(split_page_id);
    auto bucket_image_page = new_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_image_page->Init(bucket_max_size_);

    // 更新directory中的LD和bucket_page_id
    auto local_depth = directory->GetLocalDepth(bucket_idx);
    if (local_depth == directory->GetGlobalDepth()) {
      // 调用UpdateDirectoryMapping之前保证GD大于image_idx的LD(也是bucket_idx的LD)
      directory->IncrGlobalDepth();
    }
    // 注意：此时directory会有2^(GD-LD)个entry指向bucket_page。所有指向bucket_page的entry的LD和bucket_page_id都需要更新
    std::vector<uint32_t> bucket_idxes;  // 存储所有需要修改的entry的idx
    auto ld_mask = directory->GetLocalDepthMask(split_image_idx);
    for (uint32_t i = 0; i < (1 << directory->GetGlobalDepth()); i++) {
      // 可以证明，肯有ld_mask & split_image_idx == ld_mask & bucket_idx
      if ((ld_mask & i) == (ld_mask & split_image_idx)) {
        bucket_idxes.push_back(i);
      }
    }
    // 先更新LD
    // 实际上，image_idx和bucket_idx的local_depth_mask应该是相同的
    bool do_update = false;
    for (const auto &i : bucket_idxes) {
      // 可以证明，肯有ld_mask & split_image_idx == ld_mask & bucket_idx
      directory->IncrLocalDepth(i);
      if (do_update) {
        directory->SetBucketPageId(i, split_page_id);
      }
      do_update = !do_update;
    }

    // 根据key将bucket_page中的一部分键值对迁移到bucket_image_page中
    MigrateEntries(bucket_page, bucket_image_page, split_image_idx, directory->GetLocalDepthMask(split_image_idx));

    // 如果键值对要插入的bucket是bucket_image_page
    ld_mask = directory->GetLocalDepthMask(split_image_idx);
    if ((hash & ld_mask) == (split_image_idx & ld_mask)) {
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
  directory->SetBucketPageId(bucket_idx, new_page_id);
  directory->SetLocalDepth(bucket_idx, 0);
  auto bucket_guard = bpm_->FetchPageWrite(new_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (size_t i = 0; i < old_bucket->Size(); i++) {
    auto key = old_bucket->KeyAt(i);
    auto hash = Hash(key);
    if ((hash & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
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
  //  LOG_DEBUG("Enter Remove with key = %x", Hash(key));
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
  ExtendibleHTableDirectoryPage::page_id = dir_page_id;
  auto bucket_page_id = dir_page->HashToBucketPageId(hash);
  if (bucket_page_id == 0 || bucket_page_id == static_cast<page_id_t>(INVALID_PAGE_ID)) {
    return false;
  }
  LOG_DEBUG("directory page id = %u", dir_page_id);
  auto guard_bucket = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = guard_bucket.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  auto ret = bucket_page->Remove(key, cmp_);
  if (!ret) {
    return ret;
  }

  // 合并的时候需要注意下面的易错点：
  // 1. "合并"这个动词的主语应该是两个bucket page。在directory中，可能会有多个entry指向同一个bucket_page。
  // 假设需要合并bucket1和bucket2，有entry1和entry2指向bucket1，entry3和entry4指向bucket2。那么假设在合并bucket1和bucket2之后的
  // bucket为bucket_merge，entry1、entry2、entry3和entry4都要指向bucket_merge。
  // 2.
  // 需要进行递归地合并。比如说，bucket1和bucket2合并之后得到bucket_merge。而bucket_merge为空，那么bucket_merge仍然需要与bucket3
  // 进行合并。
  // 3. 在合并时只有merge_idx，没有split_idx

  // 删除成功，接下来考虑能不能合并一些bucket
  auto bucket_idx = dir_page->HashToBucketIndex(hash);
  while (dir_page->GetLocalDepth(bucket_idx) > 0) {
    auto merge_idx = dir_page->GetMergeImageIndex(bucket_idx);
    auto merge_page_id = dir_page->GetBucketPageId(merge_idx);
    if (merge_page_id == bucket_page_id) {
      // 如果这两个是同一个page，则不需要合并
      break;
    }
    auto merge_guard = bpm_->FetchPageWrite(merge_page_id);
    auto merge_page = merge_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket_page->IsEmpty() && !merge_page->IsEmpty()) {
      // 如果两个bucket都不为空，那么就无法合并，直接退出函数
      break;
    }
    if (dir_page->GetLocalDepth(bucket_idx) != dir_page->GetLocalDepth(merge_idx)) {
      // 如果两个bucket的深度不相同，那也无法合并
      // 一般来说应该不会发生这种情况吧。。。
      break;
    }

    // 收集所有需要更新的entry的idx
    std::vector<uint32_t> bucket_idxes;
    auto ld_mask1 = dir_page->GetLocalDepthMask(bucket_idx);
    auto ld_mask2 = dir_page->GetLocalDepthMask(merge_idx);
    for (uint32_t i = 0; i < (1 << dir_page->GetGlobalDepth()); i++) {
      // 可以证明，肯有ld_mask & image_idx == ld_mask & bucket_idx
      if ((ld_mask1 & i) == (ld_mask1 & bucket_idx)) {
        bucket_idxes.push_back(i);
      }
      if ((ld_mask2 & i) == (ld_mask2 & merge_idx)) {
        bucket_idxes.push_back(i);
      }
    }
    // 更新LD和bucket_page_id
    for (const auto &i : bucket_idxes) {
      dir_page->SetBucketPageId(i, bucket_page_id);
      dir_page->DecrLocalDepth(i);
    }
    // 将merge_page中的数据钱已到bucket_page中
    MigrateEntries(merge_page, bucket_page, bucket_idx, dir_page->GetLocalDepthMask(bucket_idx));

    if (merge_idx < bucket_idx) {
      bucket_idx = merge_idx;
    }

    // 检查directory是否可以收缩
    if (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
  }

  return ret;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
