#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  /**
   * @brief merge projections that do identical project.
   * Identical projection might be produced when there's `SELECT *`, aggregation, or when we need to rename the columns
   * in the planner. We merge these projections so as to make execution faster.
   *如果查询中存在重复投影操作（例如SELECT *、聚合操作、重命名列时），合并这些投影以减少不必要的计算。
   */
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter condition into nested loop join.
   * In planner, we plan cross join + filter with cross product (done with nested loop join) and a filter plan node. We
   * can merge the filter condition into nested loop join to achieve better efficiency.
   *在查询中，如果存在交叉连接（Cross Join）加过滤器的场景，将过滤条件直接合并到嵌套循环连接（Nested Loop
   *Join）中，提高效率。
   */
  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into hash join.
   * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
   * with multiple eq conditions.
   *如果嵌套循环连接中有等值连接条件，尝试将其转换为哈希连接（Hash Join）。
   */
  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into index join.
   *如果嵌套循环连接的一侧有索引，可以将其优化为索引连接（Index Join）。
   */
  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always true filter
   */
  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter into filter_predicate of seq scan plan node
   *将过滤条件直接嵌入到顺序扫描节点的filter_predicate中。
   */
  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief rewrite expression to be used in nested loop joins. e.g., if we have `SELECT * FROM a, b WHERE a.x = b.y`,
   * we will have `#0.x = #0.y` in the filter plan node. We will need to figure out where does `0.x` and `0.y` belong
   * in NLJ (left table or right table?), and rewrite it as `#0.x = #1.y`.
   *
   * @param expr the filter expression
   * @param left_column_cnt number of columns in the left size of the NLJ
   * @param right_column_cnt number of columns in the left size of the NLJ
   *解析嵌套循环连接的表达式（如a.x = b.y），根据左右表的列数量，重写表达式以匹配连接操作的左右表。
   */
  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  /** @brief check if the predicate is true::boolean */
  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  /**
   * @brief optimize order by as index scan if there's an index on a table
   *在排序操作后如果紧接着限制结果数量，可以直接使用Top N算法优化。
   */
  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize seq scan as index scan if there's an index on a table
   * @note Fall 2023 only: using hash index and only support point lookup
   *如果目标表有索引，可以将顺序扫描优化为索引扫描。
   */
  auto OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief check if the index can be matched
   *检查某个表的索引是否匹配查询条件。
   */
  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  /**
   * @brief optimize sort + limit as top N
   */
  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief get the estimated cardinality for a table based on the table name. Useful when join reordering. BusTub
   * doesn't support statistics for now, so it's the only way for you to get the table size :(
   *
   * @param table_name
   * @return std::optional<size_t>
   * 根据表名估算表的基数（即表的行数）。
   */
  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */
  const Catalog &catalog_;

  //  决定是否强制使用某些初始优化规则。
  const bool force_starter_rule_;
};

}  // namespace bustub
