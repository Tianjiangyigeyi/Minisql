//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"
#include "executor/plans/values_plan.h"
#include "planner/expressions/constant_value_expression.h"
#include"executor/executors/values_executor.h"
//
InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  //   child_executor_->Init();
  // /*1. 用plan得到insert的value，（以field指针的形式存在vector中，但可能这个vector中不只是field指针，可以再确认下）
  //   2. 用values的field构造row，并insertTuple*/
  //   std::shared_ptr<const AbstractPlanNode> child_plan = plan_->GetChildPlan();
  //   const Schema* output_schema = child_plan->OutputSchema();  // 替换为实际的 Schema 对象
  //   std::vector<std::vector<AbstractExpressionRef>> values_ ;  // 替换为实际的 values 数据TODO:values值无法获取
  //   std::shared_ptr<ValuesPlanNode> values_plan = std::make_shared<ValuesPlanNode>(const_cast<Schema*>(output_schema),values_);
  //   const std::vector<std::vector<AbstractExpressionRef>> &values=values_plan->GetValues();
  //   vector<Field> fields;
  //   for(auto itVal=values.begin();itVal!=values.end();itVal++)
  //   {
  //     for(auto it= (*itVal).begin();it!=(*itVal).end();it++)
  //     {

  //       fields.push_back(std::make_shared<ConstantValueExpression>((*it)->Evaluate(nullptr))->val_);
  //     }
  //     Row newRow(fields);
  //     rows.push_back(newRow);
  //   }
  child_executor_->Init();
  Row *row = new Row;
  RowId *rid = new RowId;
  child_executor_->Next(row,rid);
  // LOG(INFO) << "38";
  string table_name = plan_->GetTableName();
  // LOG(INFO) << "39";
  TableInfo* table_info;
  LOG(INFO) << "40";
  exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
  LOG(INFO) << "342";
  // LOG(INFO) << row[0].GetField(1)->GetData();
  if(!table_info->GetTableHeap()->InsertTuple(*row ,nullptr))
  {
    cout<<"Insert failure."<<endl;
  }
LOG(INFO) << "drtjdryj";
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  // if(cur_row<=rows.size())
  // {
  //   string table_name = plan_->GetTableName();
  //   TableInfo* table_info;
  //   exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
  //   if(!table_info->GetTableHeap()->InsertTuple(rows[cur_row],nullptr))
  //   {
  //     cout<<"Insert failure."<<endl;
  //   }
  //   cur_row++;
  //   return true;
  // }
  // else
  // {
  //   return false;
  // }

  return false;
}