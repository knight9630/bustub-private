//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/rid.h"
#include "storage/table/tuple.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "InsertExecutor:" << plan_->ToString() << std::endl;
  std::cout << "plan_->filter_predicate_->ToString()" << std::endl;
  // 防止下一个循环又插入导致死循环
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(tuple, rid)) {
    RID new_rid = table_info->table_->InsertTuple(TupleMeta{0, false}, *tuple).value();
    for (auto &indexinfo : table_indexes) {
      auto key = tuple->KeyFromTuple(table_info->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      indexinfo->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
    }
    count++;
  }

  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &GetOutputSchema());
  return true;
}

}  // namespace bustub
