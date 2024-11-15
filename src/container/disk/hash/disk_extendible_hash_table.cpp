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

#include <cstdint>
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
// 在写这个类之前，需要回顾一下之前的内容。
// FetchPageRead/NewPageGuarded().UpgradeRead()：给page加读锁
// FetchPageWrite/NewPageGuarded().UpgradeWrite()：给page加写锁
// As/AsMut：将page里的data指针转为header/directory/bucket指针
// 在创建一个新的page时需要的page_id的初始化没有意义。因为在用NewPageGuarded()创建page的时候会调用AllocatePage分配page_id
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
  this->index_name_ = name;
  this->header_page_id_ = INVALID_PAGE_ID;
  BasicPageGuard header_guard = bpm->NewPageGuarded(&this->header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // 转成uint32_t作为哈希值
  uint32_t hash = Hash(key);

  // 取directory所在page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = header_guard.As<ExtendibleHTableHeaderPage>();
  auto directory_index = head_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = head_page->GetDirectoryPageId(directory_index);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();

  // 取bucket所在page
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_guard.Drop();

  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V lookup_value;
  if (bucket_page->Lookup(key, lookup_value, cmp_)) {
    result->emplace_back(lookup_value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> val_res;
  if (GetValue(key, &val_res, transaction)) {
    return false;
  }

  uint32_t hash = Hash(key);

  // 无目标directory则插入directory,bucket
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_index = head_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = head_page->GetDirectoryPageId(directory_index);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(head_page, directory_index, hash, key, value);
  }
  header_guard.Drop();

  // 无目标bucket则插入bucket
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_index, key, value);
  }

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->IsFull()) {
    return bucket_page->Insert(key, value, cmp_);
  }

  // 桶满，分裂
  uint32_t local_depth = directory_page->GetLocalDepth(bucket_index);
  uint32_t global_depth = directory_page->GetGlobalDepth();
  if (local_depth == global_depth) {
    if (global_depth == directory_page->GetMaxDepth()) {
      return false;
    }
    directory_page->IncrGlobalDepth();
  }
  // 加一新桶,并更新index-bucket_page_id
  auto new_bucket_index = directory_page->GetSplitImageIndex(bucket_index);
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  WritePageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);
  uint32_t local_depth_mask = (1 << (local_depth + 1)) - 1;
  UpdateDirectoryMapping(directory_page, new_bucket_index, new_bucket_page_id, local_depth + 1, local_depth_mask);

  // 重新分配原桶中的key,value（先全部取出，再重新分配）
  uint32_t bucket_size = bucket_page->Size();
  std::vector<std::pair<K, V>> origin_pairs;
  for (uint32_t i = 0; i < bucket_size; i++) {
    origin_pairs.emplace_back(bucket_page->EntryAt(0));
    bucket_page->RemoveAt(0);  // 删一个会往前进一个，所以一直是0
  }
  for (const auto &entry : origin_pairs) {
    auto pair_index = directory_page->HashToBucketIndex(Hash(entry.first));
    if (directory_page->GetBucketPageId(pair_index) == bucket_page_id) {
      bucket_page->Insert(entry.first, entry.second, cmp_);
    } else if (directory_page->GetBucketPageId(pair_index) == new_bucket_page_id) {
      new_bucket_page->Insert(entry.first, entry.second, cmp_);
    }
  }
  new_bucket_guard.Drop();
  bucket_guard.Drop();
  directory_guard.Drop();

  // 递归插入，防止分裂一次桶，即local_depth+1不足以将原桶中的数据分离
  return Insert(key, value, transaction);
}

// header插入新directory
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  WritePageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  auto bucket_index = directory_page->HashToBucketIndex(hash);
  bool insert_res = InsertToNewBucket(directory_page, bucket_index, key, value);
  return insert_res;
}

// directory插入新bucket
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  WritePageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  directory->SetLocalDepth(bucket_idx, 0);
  bool insert_res = bucket_page->Insert(key, value, cmp_);
  return insert_res;
}

// 更新directory中索引指向的bucket_page_id和local_depth
// 指向新桶的索引后new_local_depth位和GetSplitImageIndex得到的索引后new_local_depth位相同
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  // 注意此时的new_bucket_idx还是指向原桶的
  page_id_t pre_bucket_id = directory->GetBucketPageId(new_bucket_idx);

  for (uint32_t bucket_index = 0; bucket_index < directory->Size(); bucket_index++) {
    // 找指向原桶的索引
    if (directory->GetBucketPageId(bucket_index) == pre_bucket_id) {
      // 对比后new_local_depth位是否和分裂得到的new_bucket_idx后new_local_depth位相同
      if ((bucket_index & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
        directory->SetBucketPageId(bucket_index, new_bucket_page_id);
        directory->SetLocalDepth(bucket_index, new_local_depth);
      } else {
        directory->SetLocalDepth(bucket_index, new_local_depth);
      }
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  // 目标directory
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_index = head_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = head_page->GetDirectoryPageId(directory_index);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();

  // 目标bucket
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // directory_guard.Drop();

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool remove_res = bucket_page->Remove(key, cmp_);
  bucket_guard.Drop();

  Merge(directory_page, bucket_index);

  while (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }
  directory_guard.Drop();

  return remove_res;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::Merge(ExtendibleHTableDirectoryPage *directory_page, uint32_t bucket_index) {
  uint8_t local_depth = directory_page->GetLocalDepth(bucket_index);

  // 等于0的时候只剩一个bucket，不可能进行合并！！！！！（debug了一晚上）
  if (local_depth == 0) {
    return;
  }

  uint32_t origin_bucket_index = bucket_index;
  uint32_t split_bucket_index = bucket_index ^ (1 << (local_depth - 1));
  page_id_t origin_page_id = directory_page->GetBucketPageId(origin_bucket_index);
  page_id_t split_page_id = directory_page->GetBucketPageId(split_bucket_index);
  if (origin_page_id == INVALID_PAGE_ID || split_page_id == INVALID_PAGE_ID) {
    return;
  }
  uint8_t origin_local_depth = directory_page->GetLocalDepth(origin_bucket_index);
  uint8_t split_local_depth = directory_page->GetLocalDepth(split_bucket_index);

  if (origin_local_depth == split_local_depth) {
    WritePageGuard origin_bucket_guard = bpm_->FetchPageWrite(origin_page_id);
    auto origin_bucket_page = origin_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    WritePageGuard split_bucket_guard = bpm_->FetchPageWrite(split_page_id);
    auto split_bucket_page = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    if (origin_bucket_page->IsEmpty()) {
      origin_bucket_guard.Drop();
      bpm_->DeletePage(origin_page_id);

      for (uint32_t bucket_index = 0; bucket_index < directory_page->Size(); bucket_index++) {
        if (directory_page->GetBucketPageId(bucket_index) == origin_page_id) {
          directory_page->SetBucketPageId(bucket_index, split_page_id);
          directory_page->DecrLocalDepth(bucket_index);
        } else if (directory_page->GetBucketPageId(bucket_index) == split_page_id) {
          directory_page->DecrLocalDepth(bucket_index);
        }
      }

      split_bucket_guard.Drop();
      Merge(directory_page, split_bucket_index);
    }

    if (split_bucket_page->IsEmpty()) {
      split_bucket_guard.Drop();
      bpm_->DeletePage(split_page_id);

      for (uint32_t bucket_index = 0; bucket_index < directory_page->Size(); bucket_index++) {
        if (directory_page->GetBucketPageId(bucket_index) == origin_page_id) {
          directory_page->DecrLocalDepth(bucket_index);
        } else if (directory_page->GetBucketPageId(bucket_index) == split_page_id) {
          directory_page->SetBucketPageId(bucket_index, origin_page_id);
          directory_page->DecrLocalDepth(bucket_index);
        }
      }

      origin_bucket_guard.Drop();
      Merge(directory_page, origin_bucket_index);
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
