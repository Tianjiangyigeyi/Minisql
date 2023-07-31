//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
*  Student Implement
*/
void UpdateExecutor::Init() {
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if(child_executor_->Next(row,rid))
  {
    string table_name=plan_->GetTableName();
    TableInfo* table_info;
    exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
    table_info->GetTableHeap()->ApplyDelete(*rid,nullptr);
    
    return true;
  }
  else
  {
    return false;
  }
 
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
    string table_name=plan_->GetTableName();
    TableInfo* table_info;
    exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
    Row row = src_row;
    table_info->GetTableHeap()->InsertTuple(row,nullptr);
  plan_->GetUpdateAttr();
  return Row();
}