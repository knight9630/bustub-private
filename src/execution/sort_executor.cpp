#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  sorted_tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.emplace_back(tuple);
  }

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), TupleCompare(GetOutputSchema(), plan_->GetOrderBy()));
  sorted_iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (sorted_iter_ != sorted_tuples_.end()) {
    *tuple = *sorted_iter_;
    *rid = tuple->GetRid();
    sorted_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
