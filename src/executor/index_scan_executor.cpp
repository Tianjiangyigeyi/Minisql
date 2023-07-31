#include "executor/executors/index_scan_executor.h"
#include<set>
#include<algorithm>
/**
*  TODO:Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  string table_name=plan_->GetTableName();
  auto predicate=plan_->GetPredicate();
  vector<IndexInfo*>plan_indexes=plan_->indexes_;
  TableInfo *table_info;
  GetExecutorContext()->GetCatalog()->GetTable(table_name,table_info);
  TableHeap *table_heap = table_info->GetTableHeap();

  vector<IndexInfo*> indexes;
  GetExecutorContext()->GetCatalog()->GetTableIndexes(table_name,indexes);
  BufferPoolManager *bpm=GetExecutorContext()->GetBufferPoolManager();
  //找出每个索引的rowid集合，取交集，根据rowid取记录。
  set<RowId>final_rowids;
  for(auto it:indexes)
  {
    set<RowId> rowids;
    Index *index=it->GetIndex();
    BPlusTreeIndex *bpIndex=new BPlusTreeIndex(index->GetIndexId(),index->GetKeySchema(),index->GetKeySchema()->GetSerializedSize(),bpm);
    for(auto it=bpIndex->GetBeginIterator();it!=bpIndex->GetEndIterator();++it)
    {
      pair<GenericKey *, RowId> a = it.operator*();
      RowId rowid=a.second;
      rowids.insert(rowid);
    }
    set_intersection(rowids.begin(),rowids.end(),final_rowids.begin(),final_rowids.end(),inserter(final_rowids,final_rowids.begin()));
  }
  for(auto it = final_rowids.begin();it!=final_rowids.end();it++)
  {
    *rid=*it;
    row->SetRowId(*rid);
    table_heap->GetTuple(row,nullptr);
    if((!plan_->need_filter_)||(predicate==nullptr))//不需要过滤
    {
      return true;
    }
    else//需要过滤
    {
      if(predicate->Evaluate(row).CompareEquals(Field(kTypeInt,1))==kTrue)
      {
        return true;
      }
    }
  }
  return false;

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
}
