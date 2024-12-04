//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include <cstddef>

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**限制得到的tuple数目 */
void LimitExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  auto limit_num = plan_->GetLimit();
  size_t cur_num = 0;

  limited_tuples_.clear();
  while (cur_num < limit_num && child_executor_->Next(&tuple, &rid)) {
    limited_tuples_.emplace_back(tuple);
    cur_num++;
  }
  limited_iter_ = limited_tuples_.begin();
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (limited_iter_ != limited_tuples_.end()) {
    *tuple = *limited_iter_;
    *rid = tuple->GetRid();
    limited_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
