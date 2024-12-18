#include "execution/execution_common.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
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
    // 一个undo_log代表一片修改区域，如果undo_logs为空，则只和原tuple相关
    if (undo_log.is_deleted_) {
      is_deleted = true;
      continue;
    }
    is_deleted = false;

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

auto CheckwwConflict(const TupleMeta &tm, const Transaction *tnx) -> bool {
  std::cout << tm.ts_ << std::endl;
  std::cout << tnx->GetReadTs() << std::endl;
  std::cout << tnx->GetTransactionTempTs() << std::endl;
  return tm.ts_ > tnx->GetReadTs() && tm.ts_ != tnx->GetTransactionTempTs();
}

auto CheckSelfModify(const TupleMeta &tm, const Transaction *tnx) -> bool {
  return tm.ts_ == tnx->GetTransactionTempTs();
}

auto GenerateUndoLog(const Tuple &old_tuple, const Tuple &new_tuple, bool old_is_deleted, bool new_is_deleted,
                     timestamp_t ts, const Schema *schema) -> UndoLog {
  if (old_is_deleted) {
    return {true, {}, old_tuple, ts};
  }

  if (new_is_deleted) {
    std::vector<bool> modified_fields;
    modified_fields.reserve(schema->GetColumnCount());
    for (uint32_t column_idx = 0; column_idx < schema->GetColumnCount(); column_idx++) {
      modified_fields[column_idx] = true;
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
  if (old_log.is_deleted_ || old_is_deleted) {
    return old_log;
  }

  Schema old_modified_schema = GetSchemaofModifiedFields(schema, old_log.modified_fields_);
  // 新操作为删除操作，undolog要记录原来的tuple的所有值,将原undo_log中的已修改区域和其他区域都标记为已修改（待验证！！！）
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

  // 新旧都不为删除，更新undo_log，加入的值有旧元组得到
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
