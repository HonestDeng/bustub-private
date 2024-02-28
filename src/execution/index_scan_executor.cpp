//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {}

void IndexScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Value> values;
  values.push_back(plan_->pred_key_->val_);
  Column col("key", INTEGER);
  std::vector<Column> cols;
  cols.push_back(col);
  Schema schema(cols);
  Tuple key(values, &schema);
  std::vector<RID> rids; // 根据Specification的说明，好像rids的最多只包含一个rid
  htable_->ScanKey(key, &rids, nullptr);

  if(rids.empty()) {
    return false;
  }

  auto pair = table_info_->table_->GetTuple(rids.front());
  if(pair.first.is_deleted_) {
    return false;
  }

  auto predicate = plan_->filter_predicate_;
  if (predicate != nullptr) {
    auto is_valid = predicate->Evaluate(&(pair.second), GetOutputSchema());
    if (is_valid.IsNull() || !is_valid.GetAs<bool>()) {
      // 如果is_valid为空或者为假，那么说明这个tuple不符合要求
      return false;
    }
  }

  *tuple = pair.second;
  *rid = rids.front();
  return true;
}

}  // namespace bustub
