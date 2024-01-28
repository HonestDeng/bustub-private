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
  // 遍历每一个节点，寻找一个需要弹出的帧
  bool found = false;
  frame_id_t optimal_frame = -1;
  bool begin = true;
  for (const auto &item : node_store_) {
    if (!item.second.IsEvictable()) {
      // 这一页不能弹出
      continue;
    }
    // 只要能执行到这里，就说明不所有的frame都被pin了，还存在有frame可以弹出
    found = true;

    if (begin) {
      // 在第一次循环初始化optimal_frame
      optimal_frame = item.first;
      begin = false;
      continue;
    }

    auto &optimal_node = node_store_.at(optimal_frame);
    auto &cur_node = node_store_.at(item.first);
    if (cur_node.IsInf() && optimal_node.IsInf()) {
      // 如果两个节点都是无穷大，选择最近一次访问最早的淘汰
      if (cur_node.MostRecent() < optimal_node.MostRecent()) {
        // 如果cur_node的最近一次访问时间小于optimal_node的，那么说明cur_node的访问时间比optimal_node早
        optimal_frame = item.first;
      }
    } else if (!optimal_node.IsInf() && cur_node.IsInf()) {
      // 如果optimal_node不是无穷大而cur_node是无穷大，那么应该淘汰cur_node
      optimal_frame = item.first;
    } else if (optimal_node.IsInf() && !cur_node.IsInf()) {
      // do nothing
    } else {
      // 都不是无穷大
      if (cur_node.LeastRecentK() < optimal_node.LeastRecentK()) {
        optimal_frame = item.first;
      }
    }
  }

  if (!found) {
    return false;
  }
  *frame_id = optimal_frame;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub