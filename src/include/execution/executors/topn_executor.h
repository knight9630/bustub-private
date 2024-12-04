//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stack>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class TopnTupleCompare {
 public:
  explicit TopnTupleCompare(Schema schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
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
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /**堆取出的tuple与实际要的顺序相反，所以用栈存储
   * 另外，还可以应对子算子得到的tuple数小于N的情况
   */
  std::stack<Tuple> ntuples_;
  size_t heap_size_ = 0;
};
}  // namespace bustub
