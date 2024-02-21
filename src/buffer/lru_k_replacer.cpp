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
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/*
 * 如果有 k = 3, 并且访问记录为 1 2 3 4 1 2 3 1 2
 * 那么应该弹出3。因为3和4的访问次数都不够3次，但是3具有最早的一次访问。
 */
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
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
      // 如果两个节点都是无穷大，选择第一次访问最早的页淘汰
      if (cur_node.EarliestRecord() < optimal_node.EarliestRecord()) {
        // 如果cur_node的最近一次访问时间小于optimal_node的，那么说明cur_node的访问时间比optimal_node早
        optimal_frame = item.first;
      }
    } else if (!optimal_node.IsInf() && cur_node.IsInf()) {
      // 如果optimal_node不是无穷大而cur_node是无穷大，那么应该淘汰cur_node
      optimal_frame = item.first;
    } else if (!optimal_node.IsInf() && !cur_node.IsInf()) {
      // 都不是无穷大
      if (cur_node.LeastRecentK() < optimal_node.LeastRecentK()) {
        optimal_frame = item.first;
      }
    }
  }

  if (!found) {
    latch_.unlock();
    return false;
  }
  *frame_id = optimal_frame;
  node_store_.erase(optimal_frame);
  curr_size_--;
  latch_.unlock();
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<unsigned long>(frame_id) < replacer_size_, "frame_id invalid");

  if (node_store_.count(frame_id) != 0) {
    // 如果frame已经存在于node_store中了
    node_store_.at(frame_id).Record(current_timestamp_);
  } else {
    LRUKNode node(k_);
    node_store_.insert(std::make_pair(frame_id, node));
    node_store_.at(frame_id).Record(current_timestamp_);
  }

  current_timestamp_++;  // increase the timestamp
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<unsigned long>(frame_id) < replacer_size_, "frame_id invalid");
  const auto node = node_store_.at(frame_id);
  if (node.IsEvictable() && !set_evictable) {
    // 如果旧值是true吗，新值是false，那么curr_size--
    curr_size_--;
  } else if (!node.IsEvictable() && set_evictable) {
    curr_size_++;
  }
  node_store_.at(frame_id).SetEvictable(set_evictable);
  current_timestamp_++;
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  current_timestamp_++;
  if (node_store_.count(frame_id) == 0) {
    latch_.unlock();
    return;
  }
  BUSTUB_ASSERT(node_store_.at(frame_id).IsEvictable(), "remove an non-evictable frame");
  node_store_.erase(frame_id);
  curr_size_--;
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub