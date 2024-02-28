//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include <memory>
#include "type/integer_type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  auto cata_log = exec_ctx_->GetCatalog();
  auto table_info = cata_log->GetTable(plan_->table_oid_);
  auto indexes_info = cata_log->GetTableIndexes(table_info->name_);
  int cnt = 0;
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, rid);

    if (!status) {
      // 返回实际插入的tuple的条数
      Value cnt1(INTEGER, cnt);
      std::vector<Value> values;
      values.push_back(cnt1);
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    cnt++;
    auto r = table_info->table_->InsertTuple({0, false}, child_tuple);
    BUSTUB_ENSURE(r != std::nullopt, "Tuple size larger than page size");
    for (auto &index_info : indexes_info) {
      // 插入索引
      index_info->index_->InsertEntry(child_tuple, r.value(), nullptr);
    }
  }
}

}  // namespace bustub
