#include "execution/executors/topn_executor.h"
#include <cstddef>
#include <queue>
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  std::priority_queue<Tuple, std::vector<Tuple>, TopnTupleCompare> topn_heap(
      TopnTupleCompare(GetOutputSchema(), plan_->GetOrderBy()));
  Tuple tuple{};
  RID rid{};
  /**当超过限定量时，递增则去除最大的，递减则去除最小的 */
  while (child_executor_->Next(&tuple, &rid)) {
    topn_heap.push(tuple);
    if (heap_size_ < plan_->GetN()) {
      heap_size_++;
    } else {
      topn_heap.pop();
    }
  }

  // 初始化
  while (!ntuples_.empty()) {
    ntuples_.pop();
  }

  while (!topn_heap.empty()) {
    ntuples_.push(topn_heap.top());
    topn_heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!ntuples_.empty()) {
    *tuple = ntuples_.top();
    *rid = tuple->GetRid();
    ntuples_.pop();
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_size_; };

}  // namespace bustub
