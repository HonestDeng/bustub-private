#include <memory>
#include <vector>
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_node = dynamic_cast<const SeqScanPlanNode &>(*plan);
    BUSTUB_ASSERT(plan->children_.empty(), "must have exactly zero children");
    auto predicate = std::dynamic_pointer_cast<const ComparisonExpression>(seq_scan_node.filter_predicate_);
    if (predicate == nullptr) {
      // 如果predicate不是比较类型
      return plan;
    }
    if(predicate->comp_type_ != ComparisonType::Equal) {
      // 如果predicate不是等值比较
      return plan;
    }
    if (!std::dynamic_pointer_cast<const ColumnValueExpression>(predicate->GetChildAt(0)) &&
        !std::dynamic_pointer_cast<const ColumnValueExpression>(predicate->GetChildAt(1))) {
      // 等号两边都不是列名
      return plan;
    }
    // 左右两边必定有一边是列名
    // 默认左边是列名，默认左边是符合优化条件的，即右边是常量
    auto column = std::dynamic_pointer_cast<ColumnValueExpression>(predicate->GetChildAt(0));
    auto constant = std::dynamic_pointer_cast<ConstantValueExpression>(predicate->GetChildAt(1));
    if (column == nullptr) {
      // 右边是列名
      column = std::dynamic_pointer_cast<ColumnValueExpression>(predicate->GetChildAt(1));
      constant = std::dynamic_pointer_cast<ConstantValueExpression>(predicate->GetChildAt(0));
    }
    if (constant == nullptr) {  // 另一边不是常量
      return plan;
    }

    auto index_oid = MatchIndex(seq_scan_node.table_name_, column->GetColIdx());
    if (index_oid == std::nullopt) {  // 索引不存在
      return plan;
    }
    auto optimized_plan = std::make_shared<IndexScanPlanNode>(seq_scan_node.output_schema_, seq_scan_node.table_oid_,
                                                              std::get<0>(index_oid.value()),
                                                              seq_scan_node.filter_predicate_, constant.get());
    return optimized_plan;
  }

  return plan;
}

}  // namespace bustub
