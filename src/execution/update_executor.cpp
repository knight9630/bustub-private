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
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

  auto tnx = exec_ctx_->GetTransaction();
  auto tnx_mgr = exec_ctx_->GetTransactionManager();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // p3版本
    /**
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
    }*/
    TupleMeta child_tm = table_info_->table_->GetTupleMeta(child_rid);
    // 检查写写冲突
    if (CheckwwConflict(child_tm, tnx)) {
      tnx->SetTainted();
      throw ExecutionException("wwconflict happen when update ");
    }

    std::vector<Value> update_values;
    update_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      update_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple update_tuple(update_values, &table_info_->schema_);
    count++;

    // 被当前事务修改,更新undo_log
    if (CheckSelfModify(table_info_->table_->GetTupleMeta(child_rid), tnx)) {
      auto undo_log_link_opt = tnx_mgr->GetUndoLink(child_rid);
      if (undo_log_link_opt.has_value() && undo_log_link_opt->IsValid()) {
        UndoLog new_undolog =
            UpdateUndoLog(tnx->GetUndoLog(undo_log_link_opt->prev_log_idx_), child_tuple, update_tuple,
                          child_tm.is_deleted_, false, &child_executor_->GetOutputSchema());
        tnx->ModifyUndoLog(undo_log_link_opt->prev_log_idx_, new_undolog);
      }
    } else {
      // 被其它事务修改，添加undo_log
      UndoLog new_undolog =
          GenerateUndoLog(child_tuple, update_tuple, table_info_->table_->GetTupleMeta(child_rid).is_deleted_, false,
                          table_info_->table_->GetTupleMeta(child_rid).ts_, &child_executor_->GetOutputSchema());
      // 将之前最新的UndoLink作为新添加的UndoLog的前一个版本
      auto prev_link = tnx_mgr->GetUndoLink(child_rid);
      if (prev_link.has_value()) {
        new_undolog.prev_version_ = tnx_mgr->GetUndoLink(child_rid).value();
      }
      auto new_undolink = tnx->AppendUndoLog(new_undolog);
      // 更新最新undo版本
      auto new_ver_link = VersionUndoLink::FromOptionalUndoLink(new_undolink);
      BUSTUB_ASSERT(new_ver_link.has_value(), "update: new_ver_link is nullopt");
      if (tnx_mgr->UpdateVersionLink(child_rid, new_ver_link)) {
        std::cout << "update versionlink succeed" << std::endl;
      } else {
        std::cout << "update versionlink fail" << std::endl;
      }
    }
    tnx->AppendWriteSet(table_info_->oid_, child_rid);
    table_info_->table_->UpdateTupleInPlace({tnx->GetTransactionTempTs(), false}, update_tuple, child_rid, nullptr);
  }

  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
