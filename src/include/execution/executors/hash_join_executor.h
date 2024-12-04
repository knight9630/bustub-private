//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

struct JoinKey {
  std::vector<Value> joinkeys_;
  auto operator==(const JoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.joinkeys_.size(); i++) {
      if (joinkeys_[i].CompareEquals(other.joinkeys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/**哈希函数特化，放在调用之前 */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.joinkeys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

class SimpleJoinHashTable {
 public:
  /** 将tuple插入对应键所对应的tuple组中 */
  void JoinInsert(const JoinKey &join_key, const Tuple join_tuple) {
    if (ht_.count(join_key) == 0) {
      ht_.insert({join_key, {join_tuple}});
    } else {
      ht_[join_key].push_back({join_tuple});
    }
  }

  /** 获取键值对应的tuples */
  auto GetTuples(const JoinKey &join_key) -> std::optional<std::vector<Tuple>> {
    if (ht_.find(join_key) == ht_.end()) {
      return std::nullopt;
    }
    return ht_.at(join_key);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  std::unordered_map<JoinKey, std::vector<Tuple>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** 获取左表tuple的键值 */
  auto GetLeftJoinKey(Tuple *left_tuple) const -> JoinKey {
    std::vector<Value> left_key;
    for (const auto &left_expr : plan_->LeftJoinKeyExpressions()) {
      left_key.push_back(left_expr->Evaluate(left_tuple, left_child_->GetOutputSchema()));
    }
    return {left_key};
  }

  /** 获取右表tuple的键值 */
  auto GetRightJoinKey(Tuple *right_tuple) const -> JoinKey {
    std::vector<Value> right_key;
    for (const auto &right_expr : plan_->RightJoinKeyExpressions()) {
      right_key.push_back(right_expr->Evaluate(right_tuple, right_child_->GetOutputSchema()));
    }
    return {right_key};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  std::unique_ptr<SimpleJoinHashTable> jht_ = std::make_unique<SimpleJoinHashTable>();

  Tuple left_tuple_{};
  RID left_rid_{};

  /**与左表Tupled的键相匹配的所有右表tuple */
  std::optional<std::vector<Tuple>> match_right_tuples_;
  std::vector<Tuple>::iterator right_iter_;

  bool left_next_;
  /** 是否有右表tuple与左表tuple匹配 */
  bool if_match_;
};

}  // namespace bustub
