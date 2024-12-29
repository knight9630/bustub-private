//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  /** 在事务提交时，将为其分配一个单调递增的提交时间戳。 */
  timestamp_t new_commit_ts = last_commit_ts_ + 1;

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  /** 更新所有写元组的时间戳 */
  for (const auto &table_rids : txn->write_set_) {
    for (const auto &rid : table_rids.second) {
      auto tuplemeta = catalog_->GetTable(table_rids.first)->table_->GetTupleMeta(rid);
      catalog_->GetTable(table_rids.first)->table_->UpdateTupleMeta({new_commit_ts, tuplemeta.is_deleted_}, rid);
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  /** 设置事务提交时间戳 */
  txn->commit_ts_ = new_commit_ts;
  last_commit_ts_ = new_commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

// 进行GC的时候，系统会保证没有并发执行的事务，所以不需要考虑加锁放锁
// table_heap中的tuple都是最新的版本，undo_log中的都是tuple的历史版本
// 如果tuple_heap中的tuple是可读的，那么将遍历它的undo_log
// undo_log如果也是可读的，则是可删除的
// 如果tuple_heap中的tuple是不可读的，遍历undo_logs，第一个可读的undo_log不删除
// 如果事务中的undo_logs全都可删除，且事务状态是已提交或回滚，则事务被清除。
void TransactionManager::GarbageCollection() {
  auto table_names = catalog_->GetTableNames();
  timestamp_t watermark = GetWatermark();

  for (const auto &name : table_names) {
    auto table_heap = catalog_->GetTable(name)->table_.get();
    auto tuple_iter = table_heap->MakeIterator();
    while (!tuple_iter.IsEnd()) {
      RID rid = tuple_iter.GetRID();
      bool if_tuple_r = false;
      bool if_first_log = true;
      // 最新可读
      if (table_heap->GetTupleMeta(rid).ts_ <= watermark) {
        if_tuple_r = true;
      }

      auto undo_link_opt = GetUndoLink(rid);
      if (undo_link_opt.has_value()) {
        UndoLink undo_link = undo_link_opt.value();

        while (undo_link.IsValid()) {
          auto undo_log_opt = GetUndoLogOptional(undo_link);

          if (undo_log_opt.has_value()) {
            if (undo_log_opt->ts_ <= watermark) {
              // table_heap中元组可读
              if (if_tuple_r) {
                UndoLog new_undolog = undo_log_opt.value();
                new_undolog.is_deleted_ = true;
                txn_map_[undo_link.prev_txn_]->ModifyUndoLog(undo_link.prev_log_idx_, new_undolog);
              } else {
                // table_heap中元组不可读
                if (if_first_log) {
                  if_first_log = false;
                } else {
                  UndoLog new_undolog = undo_log_opt.value();
                  new_undolog.is_deleted_ = true;
                  txn_map_[undo_link.prev_txn_]->ModifyUndoLog(undo_link.prev_log_idx_, new_undolog);
                }
              }

              undo_link = undo_log_opt->prev_version_;
            } else {
              undo_link = undo_log_opt->prev_version_;
              continue;
            }

          } else {
            break;
          }
        }
      }
      ++tuple_iter;
    }

    std::vector<txn_id_t> del_ids;
    for (auto &txn_ : txn_map_) {
      size_t undo_nums = txn_.second->GetUndoLogNum();
      bool all_invalid = true;
      for (size_t i = 0; i < undo_nums; i++) {
        if (!txn_.second->GetUndoLog(i).is_deleted_) {
          all_invalid = false;
        }
      }

      if (all_invalid && (txn_.second->GetTransactionState() == TransactionState::COMMITTED ||
                          txn_.second->GetTransactionState() == TransactionState::ABORTED)) {
        txn_.second->ClearUndoLog();
        del_ids.emplace_back(txn_.first);
      }
    }
    for (const auto &id : del_ids) {
      txn_map_.erase(id);
    }
    std::cout << "剩余事务id:" << std::endl;
    for (const auto &txn_ : txn_map_) {
      std::cout << txn_.first << std::endl;
    }
  }
}

}  // namespace bustub
