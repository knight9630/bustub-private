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
#include <vector>
#include "catalog/schema.h"
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
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  count_ = 0;
  // 获取主键索引（只会有一个）
  for (const auto &table_index_info : table_indexes) {
    if (table_index_info->is_primary_key_) {
      primary_key_index_ = table_index_info;
      break;
    }
  }
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }

  Tuple child_tuple = {};
  RID child_rid = {};

  auto tnx = exec_ctx_->GetTransaction();
  auto tnx_mgr = exec_ctx_->GetTransactionManager();
  bool primary_change = false;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    has_updated_ = true;
    bool self_modify = CheckSelfModify(table_info_->table_->GetTupleMeta(child_rid), tnx);
    std::cout << "update_executor" << std::endl;
    std::cout << "更新前元组:" << child_tuple.GetValue(&child_executor_->GetOutputSchema(), 0).GetAs<int>() << " "
              << child_tuple.GetValue(&child_executor_->GetOutputSchema(), 1).GetAs<int>() << std::endl;

    std::vector<Value> update_values;
    update_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      update_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple update_tuple(update_values, &child_executor_->GetOutputSchema());
    update_tuple.SetRid(child_rid);
    std::cout << "更新后元组:" << update_tuple.GetValue(&child_executor_->GetOutputSchema(), 0).GetAs<int>() << " "
              << update_tuple.GetValue(&child_executor_->GetOutputSchema(), 1).GetAs<int>() << std::endl;
    if (primary_key_index_ != nullptr) {
      primary_change = CheckPrimaryModify(child_tuple, update_tuple, table_info_, primary_key_index_);
      if (primary_change) {
        std::cout << "主键修改update" << std::endl;
        break;
      }
    }

    // 被当前事务修改,更新undo_log
    if (self_modify) {
      std::cout << "Update 自我修改" << std::endl;
      auto undo_log_link_opt = tnx_mgr->GetUndoLink(child_rid);
      if (undo_log_link_opt.has_value() && undo_log_link_opt->IsValid()) {
        UndoLog old_undolog = tnx->GetUndoLog(undo_log_link_opt->prev_log_idx_);
        UndoLog new_undolog = UpdateUndoLog(old_undolog, child_tuple, update_tuple,
                                            table_info_->table_->GetTupleMeta(child_rid).is_deleted_, false,
                                            &child_executor_->GetOutputSchema());
        tnx->ModifyUndoLog(undo_log_link_opt->prev_log_idx_, new_undolog);
      }

      table_info_->table_->UpdateTupleInPlace({tnx->GetTransactionTempTs(), false}, update_tuple, child_rid, nullptr);
    } else {
      std::cout << "Update 其它修改" << std::endl;
      // 更改in_process为真
      bool change_in_process = InProcessLock(exec_ctx_, child_rid);
      if (!change_in_process) {
        tnx->SetTainted();
        throw ExecutionException("change in_process fail when update ");
      }
      // 检查写写冲突
      bool check_ww_conflict = CheckwwConflict(table_info_->table_->GetTupleMeta(child_rid), tnx, tnx_mgr);
      if (check_ww_conflict) {
        InProcessUnlock(exec_ctx_, child_rid);
        tnx->SetTainted();
        throw ExecutionException("wwconflict happen when update ");
      }

      // 被其它事务修改，添加undo_log
      UndoLog new_undolog =
          GenerateUndoLog(child_tuple, update_tuple, table_info_->table_->GetTupleMeta(child_rid).is_deleted_, false,
                          table_info_->table_->GetTupleMeta(child_rid).ts_, &child_executor_->GetOutputSchema());
      // 将之前最新的UndoLink作为新添加的UndoLog的前一个版本
      auto prev_link = tnx_mgr->GetUndoLink(child_rid);
      if (prev_link.has_value()) {
        new_undolog.prev_version_ = prev_link.value();
      }
      auto new_undolink = tnx->AppendUndoLog(new_undolog);
      // 更新最新undo版本
      auto new_ver_link = VersionUndoLink::FromOptionalUndoLink(new_undolink);
      BUSTUB_ASSERT(new_ver_link.has_value(), "update: new_ver_link is nullopt");
      new_ver_link->in_progress_ = true;
      if (tnx_mgr->UpdateVersionLink(child_rid, new_ver_link)) {
        std::cout << "update versionlink succeed" << std::endl;
      } else {
        std::cout << "update versionlink fail" << std::endl;
      }

      table_info_->table_->UpdateTupleInPlace({tnx->GetTransactionTempTs(), false}, update_tuple, child_rid, nullptr);
      InProcessUnlock(exec_ctx_, child_rid);
    }
    tnx->AppendWriteSet(table_info_->oid_, child_rid);
    count_++;
  }

  Schema child_schema = child_executor_->GetOutputSchema();
  std::vector<std::pair<Tuple, RID>> primary_tuple_rids;
  // 当主键被更新时，需要编写单独的逻辑来处理
  if (primary_change) {
    std::cout << "主键修改！！！！！！！！" << std::endl;
    child_executor_->Init();
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      primary_tuple_rids.emplace_back(child_tuple, child_rid);
      DeleteFunction(exec_ctx_, child_schema, table_info_, tnx, tnx_mgr, child_tuple, child_rid);
    }

    for (auto tuple_rid : primary_tuple_rids) {
      std::vector<Value> insert_values;
      insert_values.reserve(plan_->target_expressions_.size());
      for (const auto &expr : plan_->target_expressions_) {
        insert_values.push_back(expr->Evaluate(&tuple_rid.first, child_executor_->GetOutputSchema()));
      }
      Tuple insert_tuple(insert_values, &table_info_->schema_);
      insert_tuple.SetRid(tuple_rid.second);

      InsertFunction(exec_ctx_, child_schema, primary_key_index_, table_info_, tnx, tnx_mgr, insert_tuple,
                     tuple_rid.second);
    }
  }

  if (!has_updated_) {
    has_updated_ = true;
    return false;
  }
  std::vector<Value> res{{TypeId::INTEGER, count_}};
  *tuple = Tuple(res, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub