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
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() {
  child_executor_->Init();

  table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple dummy_tuple;
  RID emit_rid;
  if (!child_executor_->Next(&dummy_tuple, &emit_rid)) {
    return false;
  }

  Tuple to_update_tuple;
  auto fetched = table_info_->table_->GetTuple(emit_rid, &to_update_tuple, exec_ctx_->GetTransaction());
  if (!fetched) {
    return false;
  }

  Tuple updated_tuple = GenerateUpdatedTuple(to_update_tuple);

  // lock on to-update rid
  if (exec_ctx_->GetTransaction()->IsSharedLocked(emit_rid)) {
    // upgrade S lock to X lock
    if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), emit_rid)) {
      return false;
    }
  } else if (!exec_ctx_->GetTransaction()->IsExclusiveLocked(emit_rid) &&
             // accquire X lock if not held
             !exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), emit_rid)) {
    return false;
  }

  bool updated = table_info_->table_->UpdateTuple(updated_tuple, emit_rid, exec_ctx_->GetTransaction());

  if (updated) {
    std::for_each(
        table_indexes.begin(), table_indexes.end(),
        [&to_update_tuple, &updated_tuple, &emit_rid, &table_info = table_info_, &ctx = exec_ctx_](IndexInfo *index) {
          index->index_->DeleteEntry(
              to_update_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
              emit_rid, ctx->GetTransaction());
          index->index_->InsertEntry(
              updated_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()),
              emit_rid, ctx->GetTransaction());
          ctx->GetTransaction()->GetIndexWriteSet()->emplace_back(emit_rid, table_info->oid_, WType::UPDATE,
                                                                  updated_tuple, to_update_tuple, index->index_oid_,
                                                                  ctx->GetCatalog());
        });
  }

  return updated;
}
auto UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) -> Tuple {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
