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

#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  this->max_depth_ = max_depth;
  this->global_depth_ = 0;
  for (uint32_t i = 0; i < MaxSize(); i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

// 根据hash值的低位，获取hash值在两个数组中对应的下标
auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

// 获得分裂后新的bucket_idx,这里的local_depth是增加前的depth
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  uint8_t local_depth = local_depths_[bucket_idx];
  return bucket_idx ^ (1 << local_depth);
}

// 返回global_depth_的掩码，用来计算下标值
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  uint32_t global_mask = (1 << global_depth_) - 1;
  return global_mask;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx > Size()) {
    throw Exception("bucket_idx is out of the range of directory");
  }
  uint32_t local_mask = (1 << local_depths_[bucket_idx]) - 1;
  return local_mask;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return max_depth_; }

// 增大到原来的两倍，而新空间存储着无效数据。
// 我们需要遍历两个数组的旧空间，使新旧空间存储的数据相同
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ == max_depth_) {
    return;
  }
  uint32_t presize = 1 << global_depth_;
  global_depth_++;
  uint32_t cursize = 1 << global_depth_;

  for (uint32_t i = presize; i < cursize; i++) {
    local_depths_[i] = local_depths_[i - presize];
    bucket_page_ids_[i] = bucket_page_ids_[i - presize];
  }
}

// 文档中未说明这么做，但是这样显然更严谨
void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    return;
  }
  uint32_t presize = 1 << global_depth_;
  global_depth_--;
  uint32_t cursize = 1 << global_depth_;

  for (uint32_t i = cursize; i < presize; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  if (global_depth_ == 0) {
    return false;
  }

  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }

  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  uint32_t cur_directory_size = 1 << global_depth_;
  return cur_directory_size;
}

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t {
  uint32_t max_directory_size = 1 << max_depth_;
  return max_directory_size;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

// 不需要判断local_depth与global_depth的大小关系，应该由上层确保LD <= GD
void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // if (local_depths_[bucket_idx] >= global_depth_) {
  //   return;
  // }
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (local_depths_[bucket_idx] <= 0) {
    return;
  }
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
