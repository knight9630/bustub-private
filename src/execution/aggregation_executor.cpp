//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

// 根据groupby将tuple中的Value放进哈希表中
void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();
  Tuple tuple = {};
  RID rid = {};
  while (child_executor_->Next(&tuple, &rid)) {
    auto aggregate_key = MakeAggregateKey(&tuple);
    auto aggregate_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }

  // 和顺序遍历类似，表示当前应该处理的tuple位置
  aht_iterator_ = static_cast<SimpleAggregationHashTable::Iterator>(aht_.Begin());
  has_aggregated_ = false;
}

// 遍历哈希表组成tuple再传给父算子
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "AggregationExecutor:" << plan_->ToString() << std::endl;
  // 表不空
  if (aht_.Begin() != aht_.End()) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }

    auto hash_key = aht_iterator_.Key();
    auto hash_value = aht_iterator_.Val();

    std::vector<Value> tuple_values;
    tuple_values.reserve((hash_key.group_bys_.size() + hash_value.aggregates_.size()));
    for (const auto &key : hash_key.group_bys_) {
      tuple_values.emplace_back(key);
    }
    for (const auto &value : hash_value.aggregates_) {
      tuple_values.emplace_back(value);
    }
    *tuple = Tuple(tuple_values, &GetOutputSchema());
    *rid = tuple->GetRid();
    ++aht_iterator_;
  }
  // 当表是空的时，哈希表aht_也为空，所以要初始化（尤其CountStarAggregate要初始为0）
  else {
    // 防止下个循环又输出，导致死循环
    if (has_aggregated_) {
      return false;
    }
    has_aggregated_ = true;

    // select v5, min(v1), sum(v2), count(*) from t1 group by v5(t1为空表)
    // 这种情况也不必返回
    if (plan_->GetAggregateTypes().size() > 1) {
      bool starcount = false;
      bool nostarcount = false;
      for (auto agg_type : plan_->GetAggregateTypes()) {
        if (agg_type != AggregationType::CountStarAggregate) {
          nostarcount = true;
        }
        if (agg_type == AggregationType::CountStarAggregate) {
          starcount = true;
        }
      }
      if (starcount && nostarcount) {
        return false;
      }
    }

    *tuple = {aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
    *rid = tuple->GetRid();
  }

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
