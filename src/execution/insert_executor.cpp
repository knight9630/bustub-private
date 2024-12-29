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

#include "execution/executors/insert_executor.h"
#include <memory>
#include <thread>
#include "common/rid.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_inserted_ = false;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // 获取主键索引（只会有一个）
  for (const auto &table_index_info : table_indexes) {
    if (table_index_info->is_primary_key_) {
      primary_key_index_ = table_index_info;
      break;
    }
  }
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 防止下一个循环又插入导致死循环
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  int count = 0;

  auto tnx = exec_ctx_->GetTransaction();
  auto tnx_mgr = exec_ctx_->GetTransactionManager();
  Schema child_schema = child_executor_->GetOutputSchema();
  Tuple child_tuple = {};
  RID child_rid = {};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::cout << std::this_thread::get_id() << std::endl;
    std::cout << "insert_executor" << std::endl;
    InsertFunction(exec_ctx_, child_schema, primary_key_index_, table_info_, tnx, tnx_mgr, child_tuple, child_rid);
    count++;
  }

  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &GetOutputSchema());
  return true;
}

}  // namespace bustub
