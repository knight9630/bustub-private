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
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 防止下一个循环又插入导致死循环
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto tnx = exec_ctx_->GetTransaction();
  auto tnx_mgr = exec_ctx_->GetTransactionManager();

  while (child_executor_->Next(tuple, rid)) {
    // 1.将新创建的元组的时间戳设置为当前事务的id
    RID new_rid = table_info->table_->InsertTuple(TupleMeta{tnx->GetTransactionTempTs(), false}, *tuple).value();
    // 2.在版本链中插入一个占位空值
    if (tnx_mgr->UpdateUndoLink(new_rid, std::nullopt)) {
      std::cout << "insert UndoLink插入成功" << std::endl;
    } else {
      std::cout << "insert UndoLink插入失败" << std::endl;
    }
    // 3.将创建的元组rid添加到当前事务的write set中
    tnx->AppendWriteSet(plan_->GetTableOid(), new_rid);
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
