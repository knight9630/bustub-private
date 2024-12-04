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

// 从当前位置开始遍历，找到未删除且满足过滤条件的tuple则返回
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> meta_and_tuple{};
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  do {
    if (rid_pos_ >= allrids_.size()) {
      return false;
    }
    meta_and_tuple = table_heap_->GetTuple(allrids_[rid_pos_]);
    *tuple = meta_and_tuple.second;
    *rid = allrids_[rid_pos_];
    rid_pos_++;
  } while (meta_and_tuple.first.is_deleted_ ||
           (plan_->filter_predicate_ != nullptr &&
            !plan_->filter_predicate_->Evaluate(tuple, table_info->schema_).GetAs<bool>()));

  return true;
}

}  // namespace bustub
