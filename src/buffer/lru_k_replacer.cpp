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
  current_timestamp_++;  // increase the timestamp

  size_t k_dist = 0;  // size_t 不能小于0
  frame_id_t target = -1;
  for (const auto &item : node_store_) {
    const auto &id = item.first;
    const auto &node = item.second;
    if (!node.is_evictable_) {
      continue;
    }
    size_t tmp = LONG_MAX;
    size_t node_k_dist = node.k_dist(current_timestamp_);
    if (k_dist < node_k_dist) {
      k_dist = node_k_dist;
      target = id;
    } else if (k_dist == node_k_dist && node_k_dist == tmp) {
      if (node.least_recent() < node_store_.at(target).least_recent()) {
        // 当前比较的两个frame都是不够k次访问的，并且node的最近一次访问要比targe早
        target = id;
      }
    }
  }
  if (target == -1) {
    // not found candidate frame
    return false;
  }
  *frame_id = target;
  curr_size_--;
//  replacer_size_--;
  node_store_.erase(target);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  current_timestamp_++;  // increase the timestamp

  BUSTUB_ASSERT(frame_id >= 0 && static_cast<unsigned long>(frame_id) < replacer_size_, "frame_id invalid");

  if (node_store_.count(frame_id) != 0) {
    // 如果frame已经存在于node_store中了
    node_store_.at(frame_id).record(current_timestamp_);
  } else {
    LRUKNode node(frame_id, k_);
    node_store_.insert(std::make_pair(frame_id, node));
    node_store_.at(frame_id).record(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  current_timestamp_++;
  const auto node = node_store_.at(frame_id);
  if(node.is_evictable_ && !set_evictable) {
    // 如果旧值是true吗，新值是false，那么curr_size--
    curr_size_--;
  }else if(!node.is_evictable_ && set_evictable) {
    curr_size_++;
  }
  node_store_.at(frame_id).is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  current_timestamp_++;
  if(node_store_.count(frame_id) == 0) {
    return;
  }
  BUSTUB_ASSERT(node_store_.at(frame_id).is_evictable_, "remove an non-evictable frame");
  node_store_.erase(frame_id);
//  replacer_size_--;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  return curr_size_;
}

}  // namespace bustub
