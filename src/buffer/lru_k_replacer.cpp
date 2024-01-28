//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 初始化frame_id。如果到最后frame_id还等于-1，说明没有找到可以Evict的帧
  *frame_id = -1;
  // 访问次数不足k次的帧
  std::vector<frame_id_t> inf_node;
  // 遍历每一个节点，寻找一个需要弹出的帧
  for (const auto &item : node_store_) {
    if (item.second.IsEvictable()) {
      // 如果是
      continue;
    }
    auto &node = item.second;
    size_t time;
    if (!node.LeastRecentK(&time)) {
      // 如果node的访问次数少于k个
      inf_node.push_back(item.first);
      continue;
    }
    if (*frame_id == -1) {
      *frame_id = item.first;
      continue;
    }
    size_t t1;
    node_store_.at(*frame_id).LeastRecentK(&t1);
    if (time < t1) {
      *frame_id = item.first;
    }
  }

  if (!inf_node.empty()) {
    frame_id_t tmp = -1;
    for (const auto &item : inf_node) {
      if (tmp == -1) {
        tmp = item;
        continue;
      }
      if (node_store_.at(tmp).MostRecent() < node_store_.at(item).MostRecent()) {
        tmp = item;
      }
    }
    *frame_id = tmp;

    curr_size_--;
    return true;
  }

  if (*frame_id == -1) {
    return false;
  }
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub