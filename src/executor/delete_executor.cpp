//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
*  Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row *newRow;
  RowId * newId;
  if(child_executor_->Next(newRow,newId))
  { 
    string table_name=plan_->GetTableName();
    TableInfo* table_info;
    exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
    table_info->GetTableHeap()->ApplyDelete(*newId,nullptr);
    return true;
  }
  else
  {
    return false;
  }
  
}