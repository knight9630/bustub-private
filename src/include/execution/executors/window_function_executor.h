//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */

class WindowHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  explicit WindowHashTable(const WindowFunctionType &win_func_type) : win_func_type_(win_func_type) {}

  /** @return The initial aggregate value for this aggregation executor
   * 根据聚合类型，生成每种聚合操作的初始值：
   * 对于CountStarAggregate，初始值为0。
   * 对于SUM、MIN、MAX等，初始值为NULL
   */
  auto GenerateInitialAggregateValue() -> Value {
    Value initial_val;
    switch (win_func_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        initial_val = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::Rank:
        // Others starts at null.
        initial_val = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
    }
    return initial_val;
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   * 接收一个输入值，将其合并到当前的聚合结果中。
   */
  auto CombineWinValues(Value &result_val, const Value &input_val) -> Value {
    switch (win_func_type_) {
      // 空或者不空都加
      case WindowFunctionType::CountStarAggregate:
        if (result_val.IsNull()) {
          result_val = ValueFactory::GetIntegerValue(0);
        }
        result_val = result_val.Add(Value(TypeId::INTEGER, 1));
        break;
        // 不空才加
      case WindowFunctionType::CountAggregate:
        if (!input_val.IsNull()) {
          if (result_val.IsNull()) {
            result_val = ValueFactory::GetIntegerValue(0);
          }
          result_val = result_val.Add(Value(TypeId::INTEGER, 1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (!input_val.IsNull()) {
          if (result_val.IsNull()) {
            result_val = input_val;
          } else {
            result_val = result_val.Add(input_val);
          }
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (!input_val.IsNull()) {
          if (result_val.IsNull()) {
            result_val = input_val;
          } else {
            if (result_val.CompareGreaterThan(input_val) == CmpBool::CmpTrue) {
              result_val = input_val;
            }
          }
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (!input_val.IsNull()) {
          if (result_val.IsNull()) {
            result_val = input_val;
          } else {
            if (result_val.CompareLessThan(input_val) == CmpBool::CmpTrue) {
              result_val = input_val;
            }
          }
        }
        break;
      case WindowFunctionType::Rank:
        result_val = input_val;
        break;
    }

    return result_val;
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * 如果键agg_key不存在，则初始化为默认的聚合值。
   * 调用CombineAggregateValues将传入的值agg_val合并到哈希表中对应的聚合值。
   */
  auto InsertCombine(const AggregateKey &win_key, const Value &val) -> Value {
    if (win_func_type_ == WindowFunctionType::Rank) {
      Value res_val;
      if (ht_.find(win_key) == ht_.end()) {
        ht_.insert({win_key, val});
        rank_last_num_.insert({win_key, ValueFactory::GetIntegerValue(1)});
        rank_num_.insert({win_key, ValueFactory::GetIntegerValue(1)});
        res_val = rank_last_num_[win_key];
      } else {
        // order by值和上一个不等才会更改
        res_val = rank_last_num_[win_key];
        if (val.CompareEquals(ht_[win_key]) != CmpBool::CmpTrue) {
          res_val = rank_num_[win_key];
          ht_[win_key] = val;
        }
      }
      rank_num_[win_key] = rank_num_[win_key].Add(ValueFactory::GetIntegerValue(1));
      std::cout << rank_num_[win_key].GetAs<int32_t>() << std::endl;

      return CombineWinValues(rank_last_num_[win_key], res_val);
    }

    if (ht_.count(win_key) == 0) {
      ht_.insert({win_key, GenerateInitialAggregateValue()});
    }
    return CombineWinValues(ht_[win_key], val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  auto Size() -> uint32_t {
    std::cout << ht_.size() << std::endl;
    return ht_.size();
  }

  auto GetValue(const AggregateKey &win_key) -> Value { return ht_[win_key]; }

 private:
  /** The hash table is just a map from aggregate keys to value */
  /** 对于rank函数，ht存的是上一个order by值，而不是其它函数的聚合结果! */
  std::unordered_map<AggregateKey, Value> ht_{};

  std::unordered_map<AggregateKey, Value> rank_last_num_;
  std::unordered_map<AggregateKey, Value> rank_num_;
  WindowFunctionType win_func_type_;
};

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** @return The tuple as an AggregateKey */
  auto MakeWinKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &partition_by_) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by_.size());
    for (const auto &expr : partition_by_) {
      // 这里应该是column_value_expression（取group by对应的列值作为键）
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeWinValue(const Tuple *tuple, const AbstractExpressionRef &expr) -> Value {
    Value val;
    // 这里应该是column_value_expression
    val = expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    return val;
  }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<std::unique_ptr<WindowHashTable>> all_wht_;
  /** 最终输出的tuples中的value */
  std::vector<std::vector<Value>> tuples_values_;
  std::vector<std::vector<Value>>::iterator tuples_iter_;
};
}  // namespace bustub
