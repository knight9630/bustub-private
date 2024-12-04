//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

class TupleCompare {
 public:
  explicit TupleCompare(Schema schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : schema_(std::move(schema)), order_bys_(std::move(order_bys)) {}

  auto operator()(const Tuple &t1, const Tuple &t2) -> bool {
    // 越靠前的order by 优先级越高。只有前面的相等才会看后面
    for (const auto &order_by : order_bys_) {
      auto order_by_type = order_by.first;
      auto expr = order_by.second;
      Value v1 = expr->Evaluate(&t1, schema_);
      Value v2 = expr->Evaluate(&t2, schema_);
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }

      if (order_by_type == OrderByType::DESC) {
        return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
      }
      if (order_by_type == OrderByType::ASC || order_by_type == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      return false;
    }
    return false;
  }

 private:
  Schema schema_;
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
};
/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;

  /**得到排完序的所有tuple */
  std::vector<Tuple> sorted_tuples_;
  std::vector<Tuple>::iterator sorted_iter_;
};
}  // namespace bustub
