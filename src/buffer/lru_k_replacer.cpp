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
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
// 这里的lruk不是常规的维护两个链表的作法
// 会将每一帧保存为LRUKNode，里面放前历史访问时间链表，然后从这个时间链表中去取第k次访问距离
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> wlock(latch_);
  bool ifsuccess = false;  // 有可能没有可驱逐的帧（pin_count>0，有线程在用，自然可以）

  bool inf_exist = false;  // 最近访问不足k次的帧，优先驱逐
  size_t inf_dis = 0;      // 小于k访问次的最早访问与当前时间差
  frame_id_t inf_evict_id = 0;

  size_t fin_dis = 0;  // 大于等于k次的“k-distance”
  frame_id_t fin_evict_id = 0;

  for (auto &fnode : node_store_) {
    if (!fnode.second.GetEvitable()) {
      continue;
    }

    ifsuccess = true;
    size_t dis = fnode.second.GetKDistance(current_timestamp_);

    if (dis == UINT32_MAX) {
      if (!inf_exist) {
        inf_exist = true;
      }
      if (fnode.second.GetFirstDistance(current_timestamp_) > inf_dis) {
        inf_dis = fnode.second.GetFirstDistance(current_timestamp_);
        inf_evict_id = fnode.first;
      }
    } else {
      if (dis > fin_dis) {
        fin_dis = dis;
        fin_evict_id = fnode.first;
      }
    }
  }

  if (ifsuccess) {
    *frame_id = (inf_exist) ? inf_evict_id : fin_evict_id;
    node_store_[*frame_id].SetNodeEvictable(false);
    node_store_[*frame_id].ClearTimestamp();
    node_store_.erase(*frame_id);
    curr_size_--;
  }
  current_timestamp_++;

  return ifsuccess;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> wlock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw Exception("frame_id is larger than replacer_size_");
  }

  auto pos = node_store_.find(frame_id);
  if (pos == node_store_.end()) {
    if (node_store_.size() == replacer_size_) {
      return;
    }
    node_store_[frame_id] = LRUKNode(k_, frame_id, current_timestamp_);
  } else {
    node_store_[frame_id].AddTimestamp(current_timestamp_);
  }

  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> wlock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    throw Exception("frame is not in the disk");
  }

  if (node_store_[frame_id].GetEvitable() && !set_evictable) {
    node_store_[frame_id].SetNodeEvictable(set_evictable);
    curr_size_--;
  } else if (!node_store_[frame_id].GetEvitable() && set_evictable) {
    node_store_[frame_id].SetNodeEvictable(set_evictable);
    curr_size_++;
  } else {
    // without modifying anything
  }
  current_timestamp_++;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> wlock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_[frame_id].GetEvitable()) {
    throw Exception("no frame or a non-evictable frame");
  }
  node_store_[frame_id].SetNodeEvictable(false);
  node_store_[frame_id].ClearTimestamp();
  node_store_.erase(frame_id);
  curr_size_--;
  current_timestamp_++;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
