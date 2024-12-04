//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>
#include "storage/table/tuple.h"
#include "type/type_id.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  has_updated_ = false;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "UpdateExecutor:" << plan_->ToString() << std::endl;
  for (const auto &target_expression : plan_->target_expressions_) {
    if (target_expression != nullptr) {
      std::cout << target_expression->ToString() << std::endl;
    }
  }
  if (has_updated_) {
    return false;
  }
  has_updated_ = true;
  int count = 0;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple child_tuple = {};
  RID child_rid = {};

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    count++;
    // 将原tuple标记为已删除，插入新的tuple(Table_page中没有deletetuple，虽然我觉得从内存中删除可能会更好)
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, child_rid);
    std::vector<Value> update_values;
    update_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      update_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple update_tuple(update_values, &table_info_->schema_);
    std::optional<RID> oprid = table_info_->table_->InsertTuple(TupleMeta{0, false}, update_tuple);
    RID update_rid = oprid.value();

    // 更新索引,删除原索引，插入新索引
    for (auto &index_info : table_indexes) {
      auto index = index_info->index_.get();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      auto old_key = child_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), key_attrs);
      index->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
      auto update_key = update_tuple.KeyFromTuple(table_info_->schema_, *index->GetKeySchema(), key_attrs);
      index->InsertEntry(update_key, update_rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
