//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
*  Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  LOG(INFO) << "selecet init";
    string table_name=plan_->GetTableName();
    auto predicate=plan_->GetPredicate();
    //找到table_info->table_heap，用tableIterrator查找，判断谓词条件，获取行信息，存入row，存入rid
  
    TableInfo *table_info;
    GetExecutorContext()->GetCatalog()->GetTable(table_name,table_info);
    TableHeap *table_heap = table_info->GetTableHeap();
    Row *row;
    RowId *rid;
    TableIterator it;
    LOG(INFO) << "25";
    for(it=table_heap->Begin(nullptr);it!=table_heap->End();)
    {
      LOG(INFO) << "inside seq";
      row=it.operator->();
      LOG(INFO) << "row " << row->GetField(1)->GetData();
      *rid =  row->GetRowId();
      if(predicate!=nullptr)
      {
        if(predicate->Evaluate(row).CompareEquals(Field(kTypeInt,1))==kTrue)
        {
          rows.push_back(row);
        }
      } else {
        LOG(INFO) <<"pushbak";
        rows.push_back(row);
      }
      LOG(INFO) << "before";
      // break;  // TODO: delete this!
      it++;
      LOG(INFO) << "after";
    }
    LOG(INFO) << "26";
    LOG(INFO) << rows.size() << " = size";
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  // string table_name=plan_->GetTableName();
  // auto predicate=plan_->GetPredicate();
  // //找到table_info->table_heap，用tableIterrator查找，判断谓词条件，获取行信息，存入row，存入rid
 
  // TableInfo *table_info;
  // GetExecutorContext()->GetCatalog()->GetTable(table_name,table_info);
  // TableHeap *table_heap = table_info->GetTableHeap();
  // TableIterator it;
  // for(it=table_heap->Begin(nullptr);it!=table_heap->End();it++)
  // {
  //   row=it.operator->();
  //   *rid =  row->GetRowId();
  //   if(predicate!=nullptr)
  //   {
  //     if(predicate->Evaluate(row).CompareEquals(Field(kTypeInt,1))==kTrue)
  //     {
  //       return true;
  //     }
  //   }
  // }
  // return false;

  if(cur_row<rows.size())
  {
    *row=*(rows[cur_row]);
    cur_row++;
    return true;
  }
  else
  {
    return false;
  }
}
