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
#include "concurrency/transaction.h"
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
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    /**
    // p3版本
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
    */
    std::cout << child_rid.GetPageId() << " " << child_rid.GetSlotNum() << std::endl;
    TupleMeta child_tm = table_info_->table_->GetTupleMeta(child_rid);
    // 检查写写冲突
    if (CheckwwConflict(child_tm, tnx)) {
      tnx->SetTainted();
      throw ExecutionException("wwconflict happen when delete ");
    }
    count++;

    // 被当前事务修改,更新undo_log
    if (CheckSelfModify(table_info_->table_->GetTupleMeta(child_rid), tnx)) {
      auto undo_log_link_opt = tnx_mgr->GetUndoLink(child_rid);
      if (undo_log_link_opt.has_value() && undo_log_link_opt->IsValid()) {
        UndoLog new_undolog = UpdateUndoLog(tnx->GetUndoLog(undo_log_link_opt->prev_log_idx_), child_tuple, {}, false,
                                            true, &child_executor_->GetOutputSchema());
        tnx->ModifyUndoLog(undo_log_link_opt->prev_log_idx_, new_undolog);
      }
    } else {
      // 被其它事务修改，添加undo_log
      UndoLog new_undolog =
          GenerateUndoLog(child_tuple, {}, false, true, table_info_->table_->GetTupleMeta(child_rid).ts_,
                          &child_executor_->GetOutputSchema());
      // 将之前最新的UndoLink作为新添加的UndoLog的前一个版本
      auto prev_link = tnx_mgr->GetUndoLink(child_rid);
      if (prev_link.has_value()) {
        new_undolog.prev_version_ = tnx_mgr->GetUndoLink(child_rid).value();
      }

      // 加入事务的UndoLogs
      auto new_undolink = tnx->AppendUndoLog(new_undolog);

      // 更新最新VersionUndoLink
      auto new_ver_link = VersionUndoLink::FromOptionalUndoLink(new_undolink);
      BUSTUB_ASSERT(new_ver_link.has_value(), "update: new_ver_link is nullopt");
      if (tnx_mgr->UpdateVersionLink(child_rid, new_ver_link)) {
        std::cout << "update versionlink succeed" << std::endl;
      } else {
        std::cout << "update versionlink fail" << std::endl;
      }
    }
    tnx->AppendWriteSet(table_info_->oid_, child_rid);
    table_info_->table_->UpdateTupleMeta(TupleMeta{tnx->GetTransactionTempTs(), true}, child_rid);
  }

  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &plan_->OutputSchema());

  return true;
}

}  // namespace bustub
