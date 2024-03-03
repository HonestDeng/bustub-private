//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();

  // construct htable with child tuple
  aht_ = std::make_unique<SimpleAggregationHashTable>(
      SimpleAggregationHashTable(plan_->GetGroupBys(), plan_->GetAggregateTypes()));

  Tuple child_tuple;
  RID rid;
  size_t tuple_cnt = 0;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &rid);
    if (!status) {
      break;
    }

    auto key = MakeAggregateKey(&child_tuple);
    auto value = MakeAggregateValue(&child_tuple);
    aht_->InsertCombine(key, value);
    tuple_cnt++;
  }
  if(tuple_cnt == 0) {
    // 如果这是个空表
    if(plan_->GetGroupBys().empty()) {
      // 并且没有group by
      struct AggregateKey key;
      for(size_t i = 0; i < plan_->GetGroupBys().size(); i++) {
        key.group_bys_.emplace_back(ValueFactory::GetNullValueByType(INTEGER));
      }
      aht_->ht_.insert({key, aht_->GenerateInitialAggregateValue()});
    }
  }
  iter_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (*iter_ != aht_->End()) {
    auto key = iter_->Key();
    auto val = iter_->Val();

    // construct output tuple
    std::vector<Value> values;
    values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
    values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());
    *tuple = Tuple(values, &GetOutputSchema());
    iter_->operator++();
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
