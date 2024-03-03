//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (executed) {
    return false;
  }
  executed = true;
  Tuple child_tuple{};
  int cnt = 0;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      Value cnt1(INTEGER, cnt);
      std::vector<Value> values;
      values.push_back(cnt1);
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    // 不需要使用predicate判断tuple是否需要更新，因为SeqScan和Filter会帮我们过滤

    // 先删除原本的tuple，包括索引
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);  // 标记这个tuple已经被删除了
    for (auto &index_info : indexes_info_) {           // 删除所有的索引
      std::vector<Value> values;
      std::vector<Column> col_type;
      for (auto key_idx : index_info->index_->GetKeyAttrs()) {
        auto k = child_tuple.GetValue(&child_executor_->GetOutputSchema(), key_idx);
        values.push_back(k);
        col_type.emplace_back("key", k.GetTypeId());
      }
      Schema schema(col_type);
      Tuple key(values, &schema);
      index_info->index_->DeleteEntry(key, *rid, nullptr);
    }

    // 制造一个新的Value
    cnt++;
    std::vector<Value> values;
    for (const auto &expr : plan_->target_expressions_) {  // target_expressions包括每一列的值吗?
      auto value = expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
      values.push_back(value);
    }
    Tuple new_tuple(values, &(child_executor_->GetOutputSchema()));
    // 将新的Value插入到表中
    meta.is_deleted_ = false;
    std::cout << new_tuple.ToString(&child_executor_->GetOutputSchema());
    std::cout.flush();
    auto new_rid = table_info_->table_->InsertTuple(meta, new_tuple);
    // 更新索引
    for (auto &index_info : indexes_info_) {  // 更新所有的索引
      // 插入索引
      std::vector<Value> key_values;
      std::vector<Column> col_type;
      for (auto key_idx : index_info->index_->GetKeyAttrs()) {
        auto k = new_tuple.GetValue(&child_executor_->GetOutputSchema(), key_idx);
        key_values.push_back(k);
        col_type.emplace_back("key", k.GetTypeId());
      }
      Schema schema(col_type);
      Tuple key(key_values, &schema);
      if(!index_info->index_->InsertEntry(key, new_rid.value(), nullptr)){
        std::cout << "插入失败";
      }
    }
  }
}

}  // namespace bustub
