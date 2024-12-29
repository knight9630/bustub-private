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

#include <iostream>
#include <memory>
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  has_deleted_ = false;
}

// 将update算子更新部分去除即可
auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;
  int count = 0;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple child_tuple = {};
  RID child_rid = {};

  auto tnx = exec_ctx_->GetTransaction();
  auto tnx_mgr = exec_ctx_->GetTransactionManager();
  Schema child_schema = child_executor_->GetOutputSchema();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::cout << "delete_executor" << std::endl;
    DeleteFunction(exec_ctx_, child_schema, table_info_, tnx, tnx_mgr, child_tuple, child_rid);
    count++;
  }

  if (!has_deleted_) {
    has_deleted_ = true;
    return false;
  }
  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub