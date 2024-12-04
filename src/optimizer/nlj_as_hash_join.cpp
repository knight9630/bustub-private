#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/**只有逻辑类型是and并且两边都是相等判定才可以
 * 例如可优化select * from table_A join table_B on table_B.v1=table_A.v1 and table_A.v2=table_B.v2
 * select * from table_A join table_B on table_B.v1=table_A.v1 or table_A.v2=table_B.v2
 * select * from table_A join table_B on table_B.v1>table_A.v1 and table_A.v2=table_B.v2
 * 都不可优化
 */
void GetHashJoinExpressions(const AbstractExpressionRef &predicate,
                            std::vector<AbstractExpressionRef> *left_key_expressions,
                            std::vector<AbstractExpressionRef> *right_key_expressions) {
  auto *logic_expr = dynamic_cast<LogicExpression *>(predicate.get());
  if (logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    GetHashJoinExpressions(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions);
    GetHashJoinExpressions(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions);
  }

  auto *compare_expr = dynamic_cast<ComparisonExpression *>(predicate.get());
  if (compare_expr != nullptr && compare_expr->comp_type_ == ComparisonType::Equal) {
    auto left_column_expr = dynamic_cast<ColumnValueExpression &>(*compare_expr->GetChildAt(0));

    /**会出现select * from table_A join table_B on table_B.v1=table_A.v1
     * ComparisonExpression的左右两边的Expression顺序与表顺序相反的情况
     */
    if (left_column_expr.GetTupleIdx() == 0) {  // 左表
      left_key_expressions->emplace_back(compare_expr->GetChildAt(0));
      right_key_expressions->emplace_back(compare_expr->GetChildAt(1));
    } else {  // 右表
      left_key_expressions->emplace_back(compare_expr->GetChildAt(1));
      right_key_expressions->emplace_back(compare_expr->GetChildAt(0));
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  // 递归处理子节点
  std::vector<AbstractPlanNodeRef> optimized_child_plans;
  for (const auto &child_plan : plan->GetChildren()) {
    optimized_child_plans.push_back(OptimizeNLJAsHashJoin(child_plan));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_child_plans));

  // 将嵌套循环连接优化为哈希连接
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto predicate = join_plan.predicate_;
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    GetHashJoinExpressions(predicate, &left_key_expressions, &right_key_expressions);

    return std::make_shared<HashJoinPlanNode>(join_plan.output_schema_, join_plan.GetLeftPlan(),
                                              join_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                              join_plan.GetJoinType());
  }

  return optimized_plan;
}

}  // namespace bustub
