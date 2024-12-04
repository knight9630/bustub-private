//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// table_heap.h
//
// Identification: src/include/storage/table/table_heap.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "recovery/log_manager.h"
#include "storage/page/page_guard.h"
#include "storage/page/table_page.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

class TablePage;

/**
 * TableHeap represents a physical table on disk.
 * This is just a doubly-linked list of pages.
 * 个人感觉只是一个单向链表，从first_page_id到last_page_id
 * 虽然说在创建新page的page_id只是由上一个page_id加一得到，但似乎和双向链表并不相关
 */
class TableHeap {
  friend class TableIterator;

 public:
  ~TableHeap() = default;

  /**
   * Create a table heap without a transaction. (open table)
   * @param buffer_pool_manager the buffer pool manager
   * @param first_page_id the id of the first page
   */
  explicit TableHeap(BufferPoolManager *bpm);

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return std::nullopt.
   * @param meta tuple meta
   * @param tuple tuple to insert
   * @return rid of the inserted tuple
   *将元组插入表中。如果元组的大小超过页面限制，插入失败并返回空值。
   */
  auto InsertTuple(const TupleMeta &meta, const Tuple &tuple, LockManager *lock_mgr = nullptr,
                   Transaction *txn = nullptr, table_oid_t oid = 0) -> std::optional<RID>;

  /**
   * Update the meta of a tuple.
   * @param meta new tuple meta
   * @param rid the rid of the inserted tuple
   *根据给定的行标识符（RID），更新元组的元数据（如事务状态）。
   */
  void UpdateTupleMeta(const TupleMeta &meta, RID rid);

  /**
   * Read a tuple from the table.
   * @param rid rid of the tuple to read
   * @return the meta and tuple
   *读取指定 RID 的元组及其元数据。用于从表中获取完整的行数据。
   */
  auto GetTuple(RID rid) -> std::pair<TupleMeta, Tuple>;

  /**
   * Read a tuple meta from the table. Note: if you want to get tuple and meta together, use `GetTuple` instead
   * to ensure atomicity.
   * @param rid rid of the tuple to read
   * @return the meta
   *仅获取元组的元数据。如果需要元组本身，建议使用 GetTuple。
   */
  auto GetTupleMeta(RID rid) -> TupleMeta;

  /** @return 当创建此表的迭代器时，它会记录当前表堆（table heap）中最后一个元组的位置，并且迭代器会在此位置停止，以避免
   *Halloween 问题（同一元组被多次更新的问题）。在项目 3 中，你通常需要使用此函数。
   *鉴于你已经在项目 4 中实现了作为流水线中断点的更新执行器（update executor），可以使用 MakeEagerIterator
   *来测试更新执行器是否正确实现。如果所有内容都正确实现，则此函数和项目 4 中的 MakeEagerIterator 之间不应存在任何区别。
   */
  auto MakeIterator() -> TableIterator;

  /** @return the iterator of this table. The iterator will stop at the last tuple at the time of iterating.
   *和 MakeIterator 类似，但迭代器在运行时会动态更新结束位置。
   */
  auto MakeEagerIterator() -> TableIterator;

  /** @return the id of the first page of this table */
  inline auto GetFirstPageId() const -> page_id_t { return first_page_id_; }

  /**
   * Update a tuple in place. Should NOT be used in project 3. Implement your project 3 update executor as delete and
   * insert. You will need to use this function in project 4.
   * @param meta new tuple meta
   * @param tuple  new tuple
   * @param rid the rid of the tuple to be updated
   * @param check the check to run before actually update.
   *在原地更新指定 RID 的元组。更新前可通过可选的检查函数验证条件。
   */
  auto UpdateTupleInPlace(const TupleMeta &meta, const Tuple &tuple, RID rid,
                          std::function<bool(const TupleMeta &meta, const Tuple &table, RID rid)> &&check = nullptr)
      -> bool;

  /** For binder tests */
  static auto CreateEmptyHeap(bool create_table_heap = false) -> std::unique_ptr<TableHeap> {
    // The input parameter should be false in order to generate a empty heap
    assert(!create_table_heap);
    return std::unique_ptr<TableHeap>(new TableHeap(create_table_heap));
  }

  // The below functions are useful only when you want to implement abort in a way that removes an undo log from the
  // version chain. DO NOT USE THEM if you are unsure what they are supposed to do.
  //
  // And if you decide to use the below functions, DO NOT use the normal ones like `GetTuple`. Having two read locks
  // on the same thing in one thread might cause deadlocks.

  auto AcquireTablePageReadLock(RID rid) -> ReadPageGuard;

  auto AcquireTablePageWriteLock(RID rid) -> WritePageGuard;

  void UpdateTupleInPlaceWithLockAcquired(const TupleMeta &meta, const Tuple &tuple, RID rid, TablePage *page);

  auto GetTupleWithLockAcquired(RID rid, const TablePage *page) -> std::pair<TupleMeta, Tuple>;

  auto GetTupleMetaWithLockAcquired(RID rid, const TablePage *page) -> TupleMeta;

 private:
  /** Used for binder tests */
  explicit TableHeap(bool create_table_heap = false);

  BufferPoolManager *bpm_;
  page_id_t first_page_id_{INVALID_PAGE_ID};

  std::mutex latch_;
  page_id_t last_page_id_{INVALID_PAGE_ID}; /* protected by latch_ */
};

}  // namespace bustub
