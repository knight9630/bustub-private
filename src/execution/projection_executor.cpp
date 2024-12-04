#include "execution/executors/projection_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

ProjectionExecutor::ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                                       std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void ProjectionExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto ProjectionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "ProjectionExecutor:" << plan_->ToString() << std::endl;
  for (size_t i = 0; i < plan_->expressions_.size(); i++) {
    std::cout << plan_->expressions_[i]->ToString() << std::endl;
  }

  Tuple child_tuple{};

  // Get the next tuple
  const auto status = child_executor_->Next(&child_tuple, rid);

  if (!status) {
    return false;
  }

  // Compute expressions
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  for (const auto &expr : plan_->GetExpressions()) {
    // column_value_expression
    values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
  }

  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}
}  // namespace bustub
