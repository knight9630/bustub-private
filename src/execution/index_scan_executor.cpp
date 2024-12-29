//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "catalog/catalog.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  // 和顺序扫描类似，清空重新插入
  all_index_rids_.clear();
  // 利用索引扫描获取rids
  auto index_schema = index_info->key_schema_;
  auto key = plan_->pred_key_;
  auto value = key->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &index_schema);
  hash_index->ScanKey(index_key, &all_index_rids_, exec_ctx_->GetTransaction());
  rid_pos_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // pro3
  // auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  // TupleMeta meta{};
  // do {
  //   if (rid_pos_ >= all_index_rids_.size()) {
  //     return false;
  //   }
  //   *rid = all_index_rids_[rid_pos_];
  //   meta = table_info->table_->GetTupleMeta(*rid);
  //   *tuple = table_info->table_->GetTuple(*rid).second;
  //   rid_pos_++;
  // } while (meta.is_deleted_);
  // return true;

  // 虽然主键索引的结果只会有一个，但为了和顺序查询保持一致，还是用一样的结构
  std::pair<TupleMeta, Tuple> meta_and_tuple{};
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  bool if_find = false;
  do {
    if (rid_pos_ >= all_index_rids_.size()) {
      return false;
    }
    if_find = false;
    meta_and_tuple = table_info->table_->GetTuple(all_index_rids_[rid_pos_]);
    TupleMeta seq_meta = meta_and_tuple.first;
    Tuple seq_tuple = meta_and_tuple.second;
    rid_pos_++;
    auto tnx = exec_ctx_->GetTransaction();
    auto tnx_mgr = exec_ctx_->GetTransactionManager();
    /**
     * 情况一：当前事务的id等于表堆中元组的时间戳，代表表堆中的元组正在被当前事务修改
     * 情况二：当前事务读取时间戳大于等于表堆中元组的时间戳
     * 情况三：当前事务读取时间戳小于表堆中元组的时间戳，去undo_log中去看有没有符合要求的版本
     */
    if (tnx->GetTransactionTempTs() == seq_meta.ts_ || tnx->GetReadTs() >= seq_meta.ts_) {
      *tuple = seq_tuple;
      *rid = seq_tuple.GetRid();
      if (!seq_meta.is_deleted_) {
        if_find = true;
      }
    } else {
      std::vector<UndoLog> undo_logs;
      // 由undo_log_link得到undo_log
      auto undo_log_link_opt = tnx_mgr->GetUndoLink(seq_tuple.GetRid());
      if (!undo_log_link_opt.has_value() || (undo_log_link_opt.has_value() && !undo_log_link_opt.value().IsValid())) {
        continue;
      }

      auto undo_log_opt = tnx_mgr->GetUndoLogOptional(undo_log_link_opt.value());
      if (!undo_log_opt.has_value()) {
        continue;
      }

      while (undo_log_opt.has_value()) {
        if (undo_log_opt->ts_ <= tnx->GetReadTs()) {
          undo_logs.emplace_back(undo_log_opt.value());
          break;
        }

        if (undo_log_opt->prev_version_.IsValid()) {
          undo_log_opt = tnx_mgr->GetUndoLogOptional(undo_log_opt->prev_version_);
        } else {
          break;
        }
      }
      // 获取之前符合要求的版本，undo_logs为空则说明没有目标元组
      if (undo_logs.empty()) {
        continue;
      }
      auto retuple = ReconstructTuple(&GetOutputSchema(), seq_tuple, seq_meta, undo_logs);
      if (!retuple.has_value()) {
        continue;
      }
      *tuple = retuple.value();
      *rid = seq_tuple.GetRid();
      if_find = true;
    }
  } while (!if_find || (plan_->filter_predicate_ != nullptr &&
                        !plan_->filter_predicate_->Evaluate(tuple, table_info->schema_).GetAs<bool>()));

  return true;
}

}  // namespace bustub
