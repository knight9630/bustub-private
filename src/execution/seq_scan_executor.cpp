//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include "execution/execution_common.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

// 初始将所有RID保存，并记录当前RID的位置
void SeqScanExecutor::Init() {
  auto exec_ctx = GetExecutorContext();
  auto table_info = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info->table_.get();
  auto rid_iter = table_heap_->MakeIterator();
  // 初始化不仅回到起点位置，更要将原本的删去重新插入
  allrids_.clear();
  while (!rid_iter.IsEnd()) {
    allrids_.push_back(rid_iter.GetRID());
    ++rid_iter;
  }
  rid_pos_ = 0;
}

// 从当前位置开始遍历，如果删除或者不满足过滤条件的tuple则检查下一个
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> meta_and_tuple{};
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  bool if_find = false;
  do {
    if (rid_pos_ >= allrids_.size()) {
      return false;
    }
    meta_and_tuple = table_heap_->GetTuple(allrids_[rid_pos_]);
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
      if (!undo_logs.empty()) {
        auto retuple = ReconstructTuple(&GetOutputSchema(), seq_tuple, seq_meta, undo_logs);
        if (!retuple.has_value()) {
          continue;
        } else {
          *tuple = retuple.value();
          *rid = seq_tuple.GetRid();
          if_find = true;
        }
      } else {
        continue;
      }
    }
  } while (!if_find || (plan_->filter_predicate_ != nullptr &&
                        !plan_->filter_predicate_->Evaluate(tuple, table_info->schema_).GetAs<bool>()));

  return true;
}

}  // namespace bustub
