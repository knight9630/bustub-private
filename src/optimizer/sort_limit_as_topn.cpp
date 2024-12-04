#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 递归处理子节点
  std::vector<AbstractPlanNodeRef> optimized_child_plans;
  for (const auto &child_plan : plan->GetChildren()) {
    optimized_child_plans.push_back(OptimizeSortLimitAsTopN(child_plan));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_child_plans));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto order_child = limit_plan.children_[0];
    if (order_child->GetType() == PlanType::Sort) {
      const auto &order_child_plan = dynamic_cast<const SortPlanNode &>(*order_child);
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, order_child, order_child_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
