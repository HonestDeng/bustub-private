//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  auto cata_log = exec_ctx_->GetCatalog();
  auto table_info = cata_log->GetTable(plan_->table_name_);
  iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());  // iter_最后不会指向一个野指针吗
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // tuple是一个指针，实际上已经指向一个有效的地址了

  auto predicate = plan_->filter_predicate_;
  while (!iter_->IsEnd()) {
    auto pair = iter_->GetTuple();
    if (pair.first.is_deleted_) {
      iter_->operator++();
      continue;
    }
    if (predicate != nullptr) {
      auto is_valid = predicate->Evaluate(&(pair.second), GetOutputSchema());
      if (is_valid.IsNull() || !is_valid.GetAs<bool>()) {
        // 如果is_valid为空或者为假，那么说明这个tuple不符合要求
        iter_->operator++();
        continue;
      }
    }
    *tuple = pair.second;
    *rid = iter_->GetRID();  // rid在调用本函数之前经过初始化的了嘛？
    iter_->operator++();
    return true;
  }
  return false;
}

}  // namespace bustub
