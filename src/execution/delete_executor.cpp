//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  has_deleted_ = false;
}

// 将update算子更新部分去除即可
auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "DeleteExecutor:" << plan_->ToString() << std::endl;
  std::cout << "plan_->filter_predicate_->ToString()" << std::endl;
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  Tuple child_tuple = {};
  RID child_rid = {};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    count++;
    // 将原tuple标记为已删除
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);

    // 更新索引,删除原索引，插入新索引
    for (auto index_info : table_indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      auto old_key = child_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);
      index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
