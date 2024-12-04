//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  // 很显然这两个算子是遍历算子
  left_executor_->Init();
  right_executor_->Init();
  // 是否有左元组是否与右元组匹配
  left_next_ = left_executor_->Next(&left_tuple_, &left_rid_);

  if_match_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID right_rid{};

  while (left_next_) {
    std::vector<Value> join_values;
    join_values.reserve(GetOutputSchema().GetColumnCount());

    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto one_join = plan_->predicate_->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                      right_executor_->GetOutputSchema());
      // 右表有可连接的元组
      if (!one_join.IsNull() && one_join.GetAs<bool>()) {
        if (!if_match_) {
          if_match_ = true;
        }
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          join_values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
          join_values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), j));
        }
        *tuple = Tuple(join_values, &GetOutputSchema());
        *rid = tuple->GetRid();
        return true;
      }
    }

    // 对于左连接，右表没有可连接的元组，插入空值
    if (plan_->GetJoinType() == JoinType::LEFT && !if_match_) {
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        join_values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t j = 0; j < right_executor_->GetOutputSchema().GetColumnCount(); j++) {
        join_values.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(j).GetType()));
      }
      *tuple = Tuple(join_values, &GetOutputSchema());
      *rid = tuple->GetRid();
      right_executor_->Init();
      left_next_ = left_executor_->Next(&left_tuple_, &left_rid_);
      return true;
    }

    right_executor_->Init();
    left_next_ = left_executor_->Next(&left_tuple_, &left_rid_);
    if_match_ = false;
  }

  return false;
}

}  // namespace bustub
