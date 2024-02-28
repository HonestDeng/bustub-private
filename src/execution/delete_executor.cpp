//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto indexes_info = catalog->GetTableIndexes(table_info->name_);
  int cnt = 0;

  while (true) {
    const auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      // 返回实际插入的tuple的条数
      Value cnt1(INTEGER, cnt);
      std::vector<Value> values;
      values.push_back(cnt1);
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }

    // 删除tuple和索引
    auto meta = table_info->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info->table_->UpdateTupleMeta(meta, *rid);
    for (auto &index_info : indexes_info) {
      index_info->index_->DeleteEntry(child_tuple, *rid, nullptr);
    }
  }
}

}  // namespace bustub
