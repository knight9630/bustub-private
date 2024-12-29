#include "execution/execution_common.h"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/plans/delete_plan.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> revalues;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    revalues.emplace_back(base_tuple.GetValue(schema, i));
  }

  bool is_deleted = base_meta.is_deleted_;
  for (const auto &undo_log : undo_logs) {
    // 一个undo_log代表一个事务所做的一次修改
    if (undo_log.is_deleted_) {
      is_deleted = true;
      continue;
    }
    is_deleted = false;

    /**针对插入/修改一个已经删除的元组 */
    if (CheckDeleteInsert(undo_log)) {
      return std::nullopt;
    }

    std::vector<bool> if_modified = undo_log.modified_fields_;

    // 获得修改元组的schema
    std::vector<uint32_t> attrs;
    for (size_t i = 0; i < if_modified.size(); i++) {
      if (if_modified[i]) {
        attrs.emplace_back(i);
      }
    }
    auto modified_schema = schema->CopySchema(schema, attrs);

    uint32_t modified_col_idx = 0;
    for (size_t i = 0; i < if_modified.size(); i++) {
      if (if_modified[i]) {
        revalues[i] = undo_log.tuple_.GetValue(&modified_schema, modified_col_idx);
        modified_col_idx++;
      }
    }
  }

  if (is_deleted) {
    return std::nullopt;
  }

  return std::make_optional<Tuple>(revalues, schema);
}

// 1.事务 A 尝试更新 tuple 时，发现 tuple 的最新 timestamp 属于另一个 uncommitted 的事务B
// 2.事务 A 尝试更新 tuple 时，发现 tuple 的最新 timestamp 属于另一个 committed 的事务B，且 B commited timestamp > A
// read timestamp
auto CheckwwConflict(const TupleMeta &tm, const Transaction *tnx, const TransactionManager *tnx_mgr) -> bool {
  std::cout << tm.ts_ << std::endl;
  std::cout << tnx->GetReadTs() << std::endl;
  std::cout << tnx->GetTransactionTempTs() << std::endl;
  // return tm.ts_ > tnx->GetReadTs() && tm.ts_ != tnx->GetTransactionTempTs();
  for (const auto &id_tnx : tnx_mgr->txn_map_) {
    if (tm.ts_ == id_tnx.first && tm.ts_ != tnx->GetTransactionTempTs()) {
      return true;
    }

    if (tm.ts_ == id_tnx.second->GetCommitTs() && tm.ts_ > tnx->GetReadTs()) {
      return true;
    }
  }

  return false;
}

// 检查是否是本事务修改
auto CheckSelfModify(const TupleMeta &tm, const Transaction *tnx) -> bool {
  return tm.ts_ == tnx->GetTransactionTempTs();
}

// 检查主键是否修改
auto CheckPrimaryModify(Tuple &old_tuple, Tuple &new_tuple, const TableInfo *table_info,
                        const IndexInfo *index_info) -> bool {
  auto old_key_tuple =
      old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  auto new_key_tuple =
      new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
  return !IsTupleContentEqual(old_key_tuple, new_key_tuple);
}

// 设置in_process为true,表示有事务正对tuple进行修改
auto InProcessLock(ExecutorContext *exec_ctx, RID rid) -> bool {
  auto ver_undolink = VersionUndoLink::FromOptionalUndoLink(exec_ctx->GetTransactionManager()->GetUndoLink(rid));
  if (ver_undolink.has_value()) {
    InProcessCheck in_pro_ch(ver_undolink.value());
    ver_undolink->in_progress_ = true;
    // 在此处得到一个inprocess=true的version_link后，其他线程可能会修改该version_link
    // 所以会有check操作，保证修改的version_link是最新的（CAS问题）
    return exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, ver_undolink, in_pro_ch);
  }
  auto new_ver_link = std::make_optional(VersionUndoLink{{}, true});
  return exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, new_ver_link);
}

// 设置in_process为false
void InProcessUnlock(ExecutorContext *exec_ctx, RID rid) {
  auto vul = VersionUndoLink::FromOptionalUndoLink(exec_ctx->GetTransactionManager()->GetUndoLink(rid));
  if (vul.has_value()) {
    exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, vul, nullptr);
  }
}

auto CheckDeleteInsert(const UndoLog &undolog) -> bool {
  for (size_t i = 0; i < undolog.modified_fields_.size(); i++) {
    if (undolog.modified_fields_[i] == false) {
      return false;
    }
  }
  if (undolog.tuple_.GetLength() != 0) {
    return false;
  }

  return true;
}

auto GenerateUndoLog(const Tuple &old_tuple, const Tuple &new_tuple, bool old_is_deleted, bool new_is_deleted,
                     timestamp_t ts, const Schema *schema) -> UndoLog {
  // 删除/更新 一个被删除的元组，相当于加一个什么信息都没有的undolog
  if (old_is_deleted) {
    return {false, std::vector<bool>(schema->GetColumnCount(), true), {}, ts};
  }

  // 用于delete算子，表示新元组被删除，
  if (new_is_deleted) {
    std::vector<bool> modified_fields;
    modified_fields.reserve(schema->GetColumnCount());
    for (uint32_t column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
      modified_fields.push_back(true);
    }
    return {false, modified_fields, old_tuple, ts};
  }

  // 都未删除，比较新旧元组不同的位置
  std::vector<bool> modified_fields;
  std::vector<Value> modified_old_values;
  std::vector<uint32_t> modified_attrs;
  for (uint32_t column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
    if (!old_tuple.GetValue(schema, column_idx).CompareExactlyEquals(new_tuple.GetValue(schema, column_idx))) {
      modified_fields.push_back(true);
      modified_old_values.push_back(old_tuple.GetValue(schema, column_idx));
      modified_attrs.push_back(column_idx);
    } else {
      modified_fields.push_back(false);
    }
  }
  Schema modified_schema = Schema::CopySchema(schema, modified_attrs);
  return {false, modified_fields, Tuple{modified_old_values, &modified_schema}, ts};
}

auto GetSchemaofModifiedFields(const Schema *schema, const std::vector<bool> &modified_fields) -> Schema {
  std::vector<uint32_t> attrs;
  for (uint32_t column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
    if (modified_fields[column_idx]) {
      attrs.push_back(column_idx);
    }
  }
  Schema modified_schema = Schema::CopySchema(schema, attrs);
  return modified_schema;
}

auto UpdateUndoLog(const UndoLog &old_log, const Tuple &old_tuple, const Tuple &new_tuple, bool old_is_deleted,
                   bool new_is_deleted, const Schema *schema) -> UndoLog {
  // 被当前事务删除后又进行 delete/update 将返回原undolog，从而undolog不变
  // 旧元组删除
  if (old_is_deleted) {
    return old_log;
  }
  // delete->insert->update
  // 插入时会产生一个空的undolog，再update保持不变
  if (CheckDeleteInsert(old_log)) {
    return old_log;
  }
  Schema old_modified_schema = GetSchemaofModifiedFields(schema, old_log.modified_fields_);

  // 新操作为删除操作，undolog要记录原来的tuple的所有值,将原undo_log中的已修改区域和其他区域都标记为已修改
  if (new_is_deleted) {
    std::vector<bool> modified_fields;
    std::vector<Value> modified_values;
    uint32_t modified_idx = 0;
    for (uint32_t column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
      modified_fields.push_back(true);
      if (old_log.modified_fields_[column_idx]) {
        modified_values.push_back(old_log.tuple_.GetValue(&old_modified_schema, modified_idx));
        modified_idx++;
      } else {
        modified_values.push_back(old_tuple.GetValue(schema, column_idx));
      }
    }

    return {false, modified_fields, Tuple{modified_values, schema}, old_log.ts_, old_log.prev_version_};
  }

  // 新操作不为删除，更新undo_log，加入的值有旧元组得到
  std::vector<bool> new_modified_fields;
  std::vector<Value> new_modified_values;
  std::vector<uint32_t> new_modified_attrs;
  uint32_t modified_idx = 0;
  for (uint32_t column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
    /**
     * 这里优先判断old_log
     * 是因为当一个事务对同一元组修改两次的时候，
     * 在第二次修改时，当同一个位置再次修改时
     * 应该保留最开始的结果，而不是第一次修改后的结果！
     */
    if (old_log.modified_fields_[column_idx]) {
      new_modified_fields.push_back(true);
      new_modified_values.push_back(old_log.tuple_.GetValue(&old_modified_schema, modified_idx));
      modified_idx++;
      new_modified_attrs.push_back(column_idx);
    } else if (!new_tuple.GetValue(schema, column_idx).CompareExactlyEquals(old_tuple.GetValue(schema, column_idx))) {
      new_modified_fields.push_back(true);
      new_modified_values.push_back(old_tuple.GetValue(schema, column_idx));
      new_modified_attrs.push_back(column_idx);
    } else {
      new_modified_fields.push_back(false);
    }
  }
  Schema new_modified_schema = Schema::CopySchema(schema, new_modified_attrs);
  return {false, new_modified_fields, Tuple{new_modified_values, &new_modified_schema}, old_log.ts_,
          old_log.prev_version_};
}

void InsertFunction(ExecutorContext *exec_ctx_, Schema child_schema, const IndexInfo *primary_key_index_,
                    const TableInfo *table_info_, Transaction *tnx, TransactionManager *tnx_mgr, Tuple child_tuple,
                    RID child_rid) {
  // 如果主键索引存在，相同主键的tuple存在，并且tuple未删除，终止；反之，更新TupleMeta
  bool new_insert = true;
  std::vector<RID> exist_rids;
  RID existed_rid;
  if (primary_key_index_ != nullptr) {
    Tuple key = child_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                         primary_key_index_->index_->GetKeyAttrs());
    primary_key_index_->index_->ScanKey(key, &exist_rids, tnx);
    if (!exist_rids.empty()) {
      if (!table_info_->table_->GetTupleMeta(exist_rids[0]).is_deleted_) {
        tnx->SetTainted();
        throw ExecutionException("inserted tuple has existed and not deleted!");
      } else {
        existed_rid = exist_rids[0];
        new_insert = false;
      }
    }
  }

  // 元组不存在，和4.3一样，插入并添加索引（只考虑主键索引）
  if (new_insert) {
    // 1.将新创建的元组的时间戳设置为当前事务的id
    RID new_rid = table_info_->table_->InsertTuple(TupleMeta{tnx->GetTransactionTempTs(), false}, child_tuple).value();
    // 2.在版本链中插入一个占位空值
    if (tnx_mgr->UpdateUndoLink(new_rid, std::nullopt, nullptr)) {
      std::cout << "insert UndoLink插入成功" << std::endl;
    } else {
      std::cout << "insert UndoLink插入失败" << std::endl;
    }
    // 3.添加索引
    if (primary_key_index_ != nullptr) {
      auto key = child_tuple.KeyFromTuple(table_info_->schema_, primary_key_index_->key_schema_,
                                          primary_key_index_->index_->GetKeyAttrs());
      bool insert_index = primary_key_index_->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
      if (!insert_index) {
        tnx->SetTainted();
        throw ExecutionException("Error appears when insert index during tuple insert");
      }
    }
    // 4.将创建的元组rid添加到当前事务的write set中
    tnx->AppendWriteSet(table_info_->oid_, new_rid);
  } else {
    //
    // 原索引还在，相同索引的元组存在（标记删除）,将新插入的元组放在老元组的位置
    TupleMeta child_tm = table_info_->table_->GetTupleMeta(existed_rid);
    // 被当前事务修改,更新undo_log（这里其实不变）
    if (CheckSelfModify(child_tm, tnx)) {
      auto undo_log_link_opt = tnx_mgr->GetUndoLink(existed_rid);
      if (undo_log_link_opt.has_value() && undo_log_link_opt->IsValid()) {
        UndoLog new_undolog = UpdateUndoLog(tnx->GetUndoLog(undo_log_link_opt->prev_log_idx_), child_tuple, {}, true,
                                            false, &child_schema);
        tnx->ModifyUndoLog(undo_log_link_opt->prev_log_idx_, new_undolog);
      }
      tnx->AppendWriteSet(table_info_->oid_, existed_rid);
      table_info_->table_->UpdateTupleInPlace({tnx->GetTransactionTempTs(), false}, child_tuple, existed_rid, nullptr);
    } else {
      // 检查写写冲突
      if (CheckwwConflict(child_tm, tnx, tnx_mgr)) {
        InProcessUnlock(exec_ctx_, existed_rid);
        tnx->SetTainted();
        throw ExecutionException("wwconflict happen when insert ");
      }

      // 更改in_process为真
      bool change_in_process = InProcessLock(exec_ctx_, existed_rid);
      if (!change_in_process) {
        tnx->SetTainted();
        throw ExecutionException("change in_process fail when insert ");
      }

      // 被其它事务修改，添加undo_log（其实是个空的）
      UndoLog new_undolog = GenerateUndoLog(child_tuple, {}, true, false, child_tm.ts_, &child_schema);
      // 将之前最新的UndoLink作为新添加的UndoLog的前一个版本
      auto prev_link = tnx_mgr->GetUndoLink(existed_rid);
      if (prev_link.has_value()) {
        new_undolog.prev_version_ = prev_link.value();
      }
      auto new_undolink = tnx->AppendUndoLog(new_undolog);
      // 更新最新undo版本
      auto new_ver_link = VersionUndoLink::FromOptionalUndoLink(new_undolink);
      BUSTUB_ASSERT(new_ver_link.has_value(), "update: new_ver_link is nullopt");
      new_ver_link->in_progress_ = true;
      if (tnx_mgr->UpdateVersionLink(existed_rid, new_ver_link)) {
        std::cout << "Insert update versionlink succeed" << std::endl;
      } else {
        std::cout << "Insert update versionlink fail" << std::endl;
      }
      tnx->AppendWriteSet(table_info_->oid_, existed_rid);
      table_info_->table_->UpdateTupleInPlace({tnx->GetTransactionTempTs(), false}, child_tuple, existed_rid, nullptr);
      InProcessUnlock(exec_ctx_, existed_rid);
    }
  }
}

void DeleteFunction(ExecutorContext *exec_ctx_, Schema child_schema, const TableInfo *table_info_, Transaction *tnx,
                    TransactionManager *tnx_mgr, const Tuple &child_tuple, const RID &child_rid) {
  TupleMeta child_tm = table_info_->table_->GetTupleMeta(child_rid);

  // 被当前事务修改,更新undo_log
  if (CheckSelfModify(child_tm, tnx)) {
    auto undo_log_link_opt = tnx_mgr->GetUndoLink(child_rid);
    if (undo_log_link_opt.has_value() && undo_log_link_opt->IsValid()) {
      UndoLog new_undolog = UpdateUndoLog(tnx->GetUndoLog(undo_log_link_opt->prev_log_idx_), child_tuple, {},
                                          child_tm.is_deleted_, true, &child_schema);
      tnx->ModifyUndoLog(undo_log_link_opt->prev_log_idx_, new_undolog);
    }
    tnx->AppendWriteSet(table_info_->oid_, child_rid);
    table_info_->table_->UpdateTupleMeta(TupleMeta{tnx->GetTransactionTempTs(), true}, child_rid);
  } else {
    //

    // 更改in_process为真
    bool change_in_process = InProcessLock(exec_ctx_, child_rid);
    if (!change_in_process) {
      tnx->SetTainted();
      throw ExecutionException("change in_process fail when delete ");
    }

    if (CheckwwConflict(child_tm, tnx, tnx_mgr)) {
      InProcessUnlock(exec_ctx_, child_rid);
      tnx->SetTainted();
      throw ExecutionException("wwconflict happen when delete ");
    }

    // 被其它事务修改，添加undo_log
    UndoLog new_undolog = GenerateUndoLog(child_tuple, {}, child_tm.is_deleted_, true, child_tm.ts_, &child_schema);

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
    new_ver_link->in_progress_ = true;
    if (tnx_mgr->UpdateVersionLink(child_rid, new_ver_link)) {
      std::cout << "update versionlink succeed when delete" << std::endl;
    } else {
      std::cout << "update versionlink fail when delete" << std::endl;
    }
    tnx->AppendWriteSet(table_info_->oid_, child_rid);
    table_info_->table_->UpdateTupleMeta(TupleMeta{tnx->GetTransactionTempTs(), true}, child_rid);
    InProcessUnlock(exec_ctx_, child_rid);
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(stderr, "TnxMgrDbg ");

  auto rid_iter = table_heap->MakeIterator();
  while (!rid_iter.IsEnd()) {
    RID rid = rid_iter.GetRID();
    auto iter = txn_mgr->version_info_.find(rid.GetPageId());
    if (iter != txn_mgr->version_info_.end()) {
      auto pg_ver_info = iter->second;
      auto iter2 = pg_ver_info->prev_version_.find(rid.GetSlotNum());
      if (iter2 != pg_ver_info->prev_version_.end()) {
        auto version_undolink = pg_ver_info->prev_version_[rid.GetSlotNum()];
        UndoLink undolink = version_undolink.prev_;
        while (undolink.IsValid()) {
          std::cout << "事务id:" << undolink.prev_txn_ << " " << "undologs中位置:" << undolink.prev_log_idx_
                    << std::endl;

          auto undolog_opt = txn_mgr->GetUndoLogOptional(undolink);
          if (undolog_opt.has_value()) {
            UndoLog undolog = undolog_opt.value();
            if (CheckDeleteInsert(undolog)) {
              std::cout << "删除又插入的空log" << std::endl;
              return;
            }
            std::vector<uint32_t> attrs;
            uint32_t i = 0;
            std::cout << "修改区域" << std::endl;
            for (const auto &mf : undolog.modified_fields_) {
              std::cout << mf << " ";
              if (mf) {
                attrs.push_back(i);
              }
              i++;
            }
            auto modified_schema = Schema::CopySchema(&table_info->schema_, attrs);
            std::cout << std::endl;
            std::cout << "旧元组的被修改旧值:" << std::endl;
            for (uint32_t col = 0; col < modified_schema.GetColumnCount(); col++) {
              std::cout << undolog.tuple_.GetValue(&modified_schema, col).GetAs<int>() << " ";
            }
            std::cout << std::endl;
            std::cout << "旧元组时间戳:" << undolog.ts_ << std::endl;
            std::cout << std::endl;

            undolink = undolog.prev_version_;
          } else {
            break;
          }
        }
      }
    }
    ++rid_iter;
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
