//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstdint>
#include <memory>
#include <optional>
#include "storage/table/tuple.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  left_next_ = left_child_->Next(&left_tuple_, &left_rid_);

  /**
   * 用右表构建哈希表
   * 如果用左表建哈希表，那么对于左连接，找右表没有对应tuple的左tuple会比较麻烦
   */
  Tuple right_tuple{};
  RID right_rid{};
  jht_->Clear();
  while (right_child_->Next(&right_tuple, &right_rid)) {
    jht_->JoinInsert(GetRightJoinKey(&right_tuple), right_tuple);
  }

  auto left_key = GetLeftJoinKey(&left_tuple_);
  match_right_tuples_ = jht_->GetTuples(left_key);
  if (match_right_tuples_ == std::nullopt) {
    if_match_ = false;
  } else {
    right_iter_ = match_right_tuples_->begin();
    if_match_ = true;
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_next_) {
    return false;
  }
  while (true) {
    std::vector<Value> output_values;
    output_values.reserve(GetOutputSchema().GetColumnCount());
    // 右表无匹配的元组或者右表查找结束则找下一个左表元组
    if (match_right_tuples_ != std::nullopt && right_iter_ != match_right_tuples_->end()) {
      Tuple match_right_tuple = *right_iter_;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.push_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t j = 0; j < right_child_->GetOutputSchema().GetColumnCount(); j++) {
        output_values.push_back(match_right_tuple.GetValue(&right_child_->GetOutputSchema(), j));
      }

      *tuple = Tuple(output_values, &GetOutputSchema());
      *rid = tuple->GetRid();
      ++right_iter_;
      return true;
    }

    // 左外连接无匹配元组补空值
    if (plan_->GetJoinType() == JoinType::LEFT && !if_match_) {
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        output_values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t j = 0; j < right_child_->GetOutputSchema().GetColumnCount(); j++) {
        output_values.emplace_back(
            ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple(output_values, &GetOutputSchema());
      *rid = tuple->GetRid();

      // 防止下一次Next又进入导致死循环
      if_match_ = true;
      return true;
    }

    // 检查下一个左表中的tuple
    left_next_ = left_child_->Next(&left_tuple_, &left_rid_);
    if (!left_next_) {
      return false;
    }
    auto left_key = GetLeftJoinKey(&left_tuple_);
    match_right_tuples_ = jht_->GetTuples(left_key);
    if (match_right_tuples_ == std::nullopt) {
      if_match_ = false;
    } else {
      right_iter_ = match_right_tuples_->begin();
      if_match_ = true;
    }
  }
}

}  // namespace bustub
