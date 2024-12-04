#include <cstdint>
#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    // 谓词不为空且可以转化为比较谓词
    if (seqscan_plan.filter_predicate_ != nullptr) {
      const auto *comp_expr = dynamic_cast<ComparisonExpression *>(seqscan_plan.filter_predicate_.get());
      if (comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
        const auto *table_info = catalog_.GetTable(seqscan_plan.GetTableOid());
        // 获取所有索引
        const auto table_indexes = catalog_.GetTableIndexes(table_info->name_);
        // 第一个子谓词必须是列值谓词
        const auto *column_value_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->children_[0].get());
        if (column_value_expr != nullptr) {
          // 将索引列号和列值谓词的列号对比，一样则可以转为index_scan_node
          for (const auto *index : table_indexes) {
            const std::vector<uint32_t> &index_column_ids = index->index_->GetKeyAttrs();
            std::vector<uint32_t> value_column_ids = std::vector<uint32_t>{column_value_expr->GetColIdx()};

            if (index_column_ids == value_column_ids) {
              auto pred_key = dynamic_cast<ConstantValueExpression *>(comp_expr->GetChildAt(1).get());
              return std::make_shared<IndexScanPlanNode>(seqscan_plan.output_schema_, seqscan_plan.table_oid_,
                                                         index->index_oid_, seqscan_plan.filter_predicate_, pred_key);
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
