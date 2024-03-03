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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    // InsertExecutor的Next函数只能被执行一次
    // 第一次执行返回true，以后的执行返回false
    return false;
  }
  executed_ = true;

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
      std::vector<Value> values;
      std::vector<Column> col_type;
      for (auto key_idx : index_info->index_->GetKeyAttrs()) {
        auto k = child_tuple.GetValue(&child_executor_->GetOutputSchema(), key_idx);
        values.push_back(k);
        col_type.emplace_back("key", k.GetTypeId());
      }
      Schema schema(col_type);
      Tuple key(values, &schema);
      index_info->index_->InsertEntry(key, r.value(), nullptr);
    }
  }
}

}  // namespace bustub
