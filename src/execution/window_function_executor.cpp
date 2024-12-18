#include "execution/executors/window_function_executor.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include "execution/executors/sort_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  std::vector<Tuple> sorted_tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples.emplace_back(tuple);
  }

  // 文档中说明各个窗口函数中只会有一个相同的order by,所以先直接将所有的tuple排序
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> one_order;
  for (const auto &window_function : plan_->window_functions_) {
    if (!window_function.second.order_by_.empty()) {
      one_order = window_function.second.order_by_;
      std::sort(sorted_tuples.begin(), sorted_tuples.end(), TupleCompare(GetOutputSchema(), one_order));
      break;
    }
  }

  auto window_functions = plan_->window_functions_;
  // 最终输出的column数
  size_t column_size = plan_->columns_.size();
  std::vector<bool> has_win_func(column_size);
  std::vector<bool> has_order_by(column_size, false);
  for (uint32_t column_index = 0; column_index < column_size; column_index++) {
    // 初始化窗口函数，没有则插入空
    if (window_functions.find(column_index) == window_functions.end()) {
      has_win_func[column_index] = false;
      all_wht_.emplace_back(nullptr);
    } else {
      has_win_func[column_index] = true;
      has_order_by[column_index] = !window_functions.find(column_index)->second.order_by_.empty();
      auto wht = std::make_unique<WindowHashTable>(window_functions.find(column_index)->second.type_);
      all_wht_.push_back(std::move(wht));
    }
  }
  tuples_values_.clear();
  // 保存键值，供无order by更新使用
  std::vector<std::vector<AggregateKey>> tuples_keys;

  for (const auto &tuple : sorted_tuples) {
    std::vector<Value> one_tuple_vals;
    std::vector<AggregateKey> one_tuple_keys;
    for (uint32_t column_index = 0; column_index < column_size; column_index++) {
      // 有窗口函数放入哈希表，无窗口函数直接取值
      if (has_win_func[column_index]) {
        auto window_function = window_functions[column_index];
        auto win_key = MakeWinKey(&tuple, window_function.partition_by_);

        // rank函数单独讨论
        if (window_function.type_ == WindowFunctionType::Rank) {
          auto order_val = one_order[0].second->Evaluate(&tuple, GetOutputSchema());
          one_tuple_keys.emplace_back(win_key);
          one_tuple_vals.emplace_back(all_wht_[column_index]->InsertCombine(win_key, order_val));
          continue;
        }

        auto win_val = MakeWinValue(&tuple, window_function.function_);
        one_tuple_keys.emplace_back(win_key);
        one_tuple_vals.emplace_back(all_wht_[column_index]->InsertCombine(win_key, win_val));
      } else {
        one_tuple_keys.emplace_back();
        one_tuple_vals.emplace_back(plan_->columns_[column_index]->Evaluate(&tuple, GetOutputSchema()));
      }
    }
    tuples_values_.emplace_back(one_tuple_vals);
    tuples_keys.emplace_back(one_tuple_keys);
  }

  // 对于没有order by的窗口函数，将所有column值更新为最后一个聚合值，而不是随着tuple所在位置改变
  for (size_t tuple_index = 0; tuple_index < tuples_values_.size(); tuple_index++) {
    for (uint32_t column_index = 0; column_index < column_size; column_index++) {
      if (has_win_func[column_index] && !has_order_by[column_index]) {
        tuples_values_[tuple_index][column_index] =
            all_wht_[column_index]->GetValue(tuples_keys[tuple_index][column_index]);
      }
    }
  }

  tuples_iter_ = tuples_values_.begin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (tuples_iter_ != tuples_values_.end()) {
    *tuple = Tuple(*tuples_iter_, &GetOutputSchema());
    *rid = tuple->GetRid();
    tuples_iter_++;
    return true;
  }
  return false;
}
}  // namespace bustub
