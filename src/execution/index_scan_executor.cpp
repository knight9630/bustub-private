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
  auto table_schema = index_info->key_schema_;
  auto key = plan_->pred_key_;
  auto value = key->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &table_schema);
  hash_index->ScanKey(index_key, &all_index_rids_, exec_ctx_->GetTransaction());
  rid_pos_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "IndexScanExecutor:" << plan_->ToString() << std::endl;
  if (plan_->filter_predicate_ != nullptr) {
    std::cout << plan_->filter_predicate_->ToString() << std::endl;
  }
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);

  TupleMeta meta{};
  do {
    if (rid_pos_ >= all_index_rids_.size()) {
      return false;
    }
    *rid = all_index_rids_[rid_pos_];
    meta = table_info->table_->GetTupleMeta(*rid);
    *tuple = table_info->table_->GetTuple(*rid).second;
    rid_pos_++;
  } while (meta.is_deleted_);

  return true;
}

}  // namespace bustub
