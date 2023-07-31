#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    LOG(INFO) << "in here";
    executor->Init();
    LOG(INFO) << "out of here";
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      LOG(INFO) << "enter";
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string database_name = ast->child_->val_;
  if (dbs_.find(database_name) == dbs_.end()) {
    dbs_.emplace(database_name, new DBStorageEngine("./"+database_name));
    // context->related_row_num_ += 1;作用：n rows affected
    cout << "Create database " << database_name << " successfully." << endl;
    return DB_SUCCESS;
  } 
  else {
    cout << "Error:Database " << database_name << " already exists." << endl;
    return DB_FAILED;
  }
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string database_name = ast->child_->val_;
  if (dbs_.find(database_name) != dbs_.end()) 
  {
    if(current_db_==database_name)
    {
      current_db_="";
    }//drop当前使用的db的话current_db_也要改变
    remove(("./database/" + database_name + ".db").c_str());
    dbs_.erase(database_name);
    //context->related_row_num_ += 1;
    cout << "Drop database " << database_name << " successfully." << endl;
    return DB_SUCCESS;
  } 
  else
  {
    cout << "Error:Database " << database_name << " doesn't exist." << endl;
    return DB_FAILED;
  }
}

/**
 * Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if(dbs_.empty())
  {
    cout<<"No database."<<endl;
    return DB_SUCCESS;
  }
  else
  {
    int width=0;
    for(unordered_map<string,DBStorageEngine*>::iterator it = dbs_.begin();it!=dbs_.end();it++)
    {
      width=width>it->first.length()?width:it->first.length();
    }
    cout << "+" << setw(width+3) << setfill('-')<<"+"<<endl;
    for(unordered_map<string,DBStorageEngine*>::iterator it =dbs_.begin();it!=dbs_.end();it++)
    {
      cout<<"|"<<setw(width)<<setfill(' ')<<it->first<<setw(width+1)<<setfill(' ')<<"|"<<endl;
    }
    cout << "+" << setw(width+3) << setfill('-')<<"+"<<endl;
    return DB_SUCCESS;
  }
}

/**
 * Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string database_name=ast->child_->val_;
  if(dbs_.find(database_name)!=dbs_.end())
  {
    cout<<"Use database "<<database_name<<" successfully."<<endl;
    current_db_=database_name;
    return DB_SUCCESS;
  }
  else
  {
    cout<<"Error:Database "<<database_name<<" doesn't exist."<<endl;
    return DB_FAILED;
  }
}

/**
 * Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (dbs_.find(current_db_) != dbs_.end()) {
    vector<TableInfo *> tables;
    if(dbs_[current_db_]->catalog_mgr_->GetTables(tables)==DB_TABLE_NOT_EXIST)
    {
      return DB_FAILED;
    }
    else 
    {
      int width = 0;
      for (vector<TableInfo*>::iterator it=tables.begin();it!=tables.end();it++) {
        width = width > it[0]->GetTableName().size()?width:it[0]->GetTableName().size();
      }
      cout << "+" << setw(width + 3) << setfill('-') << "+" << endl;
      for (vector<TableInfo*>::iterator it=tables.begin();it!=tables.end();it++) {
        cout << "|" << setw(width) << setfill(' ') << it[0]->GetTableName() << setw(width+1) << setfill(' ') <<"|" << endl;
      }
      cout << "+" << setw(width + 3) << setfill('-') << "+" << endl;
      return DB_SUCCESS;
    }
  }
  else
  {
    cout<<"Error:No using database."<<endl;
    return DB_FAILED;
  }
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (dbs_.find(current_db_) != dbs_.end()&&ast!=nullptr) {
    string table_name=ast->child_->val_;
    TableInfo *table_info;
    if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name,table_info)==DB_SUCCESS)
    {
      cout<< "Error:Table already exist."<<endl;
      return DB_FAILED;
    }
    else
    {
      SyntaxNode *node = ast->child_->next_->child_;
      vector<Column*>columns;
      vector<vector<string> > unique_indexes;
      vector<string> primary_keys;
      for(uint32_t column_index=0;node!=nullptr&&node->type_==kNodeColumnDefinition;column_index++,node=node->next_)
      {
        string column_name=node->child_->val_;
        string column_type=node->child_->next_->val_;
        
        bool unique=false;
        //unique index
        string str=(node->val_==nullptr)?"":node->val_;
        if(str=="unique")
        {
          unique=true;
          vector<string> unique_index;
          unique_index.push_back(node->child_->val_);
          unique_indexes.push_back(unique_index);
        }

        //table
        if(column_type=="int")
        {
          Column* column=new Column(column_name, kTypeInt, column_index, true, unique);
          columns.push_back(column);
        }
        else if(column_type=="float")
        {
          Column* column=new Column(column_name, kTypeFloat, column_index, true, unique);
          columns.push_back(column);
        }
        else if(column_type=="char")
        {
          uint32_t length = atoi(node->child_->next_->child_->val_);
          Column* column=new Column(column_name, kTypeChar, length, column_index, true, unique);
          columns.push_back(column);
        }
        else
        {
          cout<<"Error:Invalid type."<<endl;
          return DB_FAILED;
        }
      }
      Schema *schema = new Schema(columns,true);
      if(dbs_[current_db_]->catalog_mgr_->CreateTable(table_name,schema,nullptr,table_info)!=DB_SUCCESS)
      {
        return DB_FAILED;
      }

      //primary_key_index
      if(node!=nullptr)
      {
        SyntaxNode* PK_node=node->child_;
        while(PK_node!=nullptr)
        {
          string primary_key=PK_node->val_;
          primary_keys.push_back(primary_key);
          PK_node=PK_node->next_;
        }
        if(!primary_keys.empty())
        {
          string pk_index_name=table_name+"_pk_index";
          IndexInfo *index_info;
          dberr_t result=dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name,pk_index_name,primary_keys,nullptr,index_info,"bptree");
          if(result!=DB_SUCCESS)
          {
            cout<<"Error:Primary key index "<<pk_index_name<<" create fail."<<endl;
            return DB_FAILED;
          }
        }
      }
      

      //unique_index
      for(auto it=unique_indexes.begin();it!=unique_indexes.end();it++)
      {
        string uq_index_name=table_name+"_uq_"+(*it)[0];
        IndexInfo *index_info;
        dberr_t result=dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name,uq_index_name,*it,nullptr,index_info,"bptree");
        if(result!=DB_SUCCESS)
        {
          cout<<"Error:Unique index "<<uq_index_name<<" create fail."<<endl;
          return DB_FAILED;
        }
      }
    }
    return DB_SUCCESS;
  } 
  else 
  {
    cout << "Error:No using database." << endl;
    return DB_FAILED;
  }
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (dbs_.find(current_db_) != dbs_.end()) {
    string table_name = ast->child_->val_;
    dberr_t result = dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
    if (result == DB_FAILED) {
      cout << "Error:Drop failed." << endl;
      return DB_FAILED;
    } 
    else if (result == DB_TABLE_NOT_EXIST) {
      cout << "Error:Table " << table_name << " not exist." << endl;
      return DB_FAILED;
    } 
    else {
      cout << "Drop table " << table_name << " successfully." << endl;
      return DB_SUCCESS;
    }
  } 
  else {
    cout << "Error:No using database." << endl;
    return DB_FAILED;
  }
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (dbs_.find(current_db_) != dbs_.end()) {
    vector<TableInfo*> tables;
    vector<IndexInfo*> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTables(tables);
    if(tables.empty())
    {
      cout<<"No table."<<endl;
      return DB_FAILED;
    } 
    else {
      for (vector<TableInfo *>::iterator it = tables.begin(); it != tables.end(); it++) 
      {
        dbs_[current_db_]->catalog_mgr_->GetTableIndexes(it[0]->GetTableName(), indexes);
      }
      if(indexes.empty())
      {
        cout<<"No index."<<endl;
        return DB_FAILED;
      } 
      else {
        int width = 0;
        for (vector<IndexInfo *>::iterator it = indexes.begin(); it != indexes.end(); it++) {
          width = width > it[0]->GetIndexName().length() ? width : it[0]->GetIndexName().length();
        }
        cout << "+" << setw(width + 3) << setfill('-') << "+" << endl;
        for (vector<IndexInfo *>::iterator it = indexes.begin(); it != indexes.end(); it++) {
          cout << "|" << setw(width) << setfill(' ') << it[0]->GetIndexName()<< "|" << endl;
        }
        cout << "+" << setw(width + 3) << setfill('-') << "+" << endl;
      }
    }
  } 
  else {
    cout << "Error:No using database." << endl;
    return DB_FAILED;
  }
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  SyntaxNode *node = ast->child_;
  if(dbs_.find(current_db_)!=dbs_.end()&&ast!=nullptr)
  {
    string index_name=node->val_;
    string table_name=node->next_->val_;
    TableInfo *table_info;
    IndexInfo *index_info;
    if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name,table_info)!=DB_SUCCESS)
    {
      cout<<"Error:Table not exist."<<endl;
      return DB_FAILED;
    }
    else if(dbs_[current_db_]->catalog_mgr_->GetIndex(table_name,index_name,index_info)==DB_SUCCESS)
    {
      cout<<"Error:Index already exist."<<endl;
      return DB_FAILED;
    }
    else
    {
      node=node->next_->next_;
      if(node->type_!=kNodeColumnList)
      {
        cout<<"Error:Index key not exist."<<endl;
        return DB_FAILED;
      }
      node=node->child_;
      vector<string> index_keys;
      while(node!=nullptr)
      {
        index_keys.push_back(node->val_);
        node=node->next_;
      }
      vector<string>::iterator it;
      for(it=index_keys.begin();it!=index_keys.end();it++)
      {
        uint32_t column_index;//初始化
        if(table_info->GetSchema()->GetColumnIndex(*it,column_index)==DB_COLUMN_NAME_NOT_EXIST)
        {
          cout<<"Eror:Column not exist."<<endl;
          return DB_FAILED;
        }
        if(table_info->GetSchema()->GetColumn(column_index)->IsUnique())
        {
          break;
        }
      }
      if(it==index_keys.end())//无unique索引
      {
        cout<<"Error:At least one unique index shuold exist."<<endl;
        return DB_FAILED;
      }
      IndexInfo *index_info_1;
      dberr_t result=dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name,index_name,index_keys,nullptr,index_info_1,"bptree");
      if(result!=DB_SUCCESS)
      {
        cout<<"Error: Create index fail."<<endl;
        return DB_FAILED;
      }
      else
      {
        uint32_t *column_index=new uint32_t[index_keys.size()];
        for(int i=0;i<index_keys.size();i++)
        {
          table_info->GetSchema()->GetColumnIndex(index_keys[i],column_index[i]);
        }
        auto table_heap= table_info->GetTableHeap();
        for(auto iter = table_heap->Begin(nullptr);iter!=table_heap->End();++iter)
        {
          vector<Field>fields;
          for(int i =0;i<index_keys.size();i++)
          {
            fields.push_back((*iter->GetField(column_index[i])));
          }
          Row row(fields);
          RowId rid = iter->GetRowId();
          index_info->GetIndex()->InsertEntry(row,rid,nullptr);
        }
        cout<<"Create index successfully."<<endl;

        return DB_SUCCESS;
      }
    }
  }
  else
  {
    cout<<"Error:No using database."<<endl;
    return DB_FAILED;
  }
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (dbs_.find(current_db_) != dbs_.end()) {
    string index_name_drop=ast->child_->val_;
    vector<TableInfo*> tables;
    dbs_[current_db_]->catalog_mgr_->GetTables(tables);
    if(tables.empty())
    {
      cout<<"Error:No tables."<<endl;
      return DB_FAILED;
    } 
    else {
      for (auto it = tables.begin(); it != tables.end(); it++) 
      {
        string table_name= (*it)->GetTableName();
        vector<IndexInfo*> indexes;
        dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
        for (vector<IndexInfo *>::iterator it2 = indexes.begin(); it2 != indexes.end(); it2++) 
        {
          string index_name= (*it2)->GetIndexName();
          if (index_name == index_name_drop) 
          {
            dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name_drop);
            cout << "Drop index " << index_name_drop << " successfully." << endl;
            return DB_SUCCESS;
          }
        }
      }
      cout << "Error:Index " << index_name_drop << " not found." << endl;
      return DB_FAILED;
    }
  } 
  else {
    cout << "Error:No using database." << endl;
    return DB_FAILED;
  }
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}


/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  ifstream file;
  file.open(ast->child_->val_,ios::in);
  if(!file.is_open())
  {
    cout<<"Error:File fail to open."<<endl;
    return DB_FAILED;
  }
  char c;
  char *statement=new char[BUFSIZ];
  while(file.get(c))
  {
    int i=0;
    while (c != ';')
    {
      statement[i]=c;
      file.get(c);
    }
    statement[i]=c;

    MinisqlParserInit();
    
    if(MinisqlParserGetError())
    {
      cout<<MinisqlParserGetErrorMessage()<<endl;
    }

    this->Execute(MinisqlGetParserRootNode());

    MinisqlParserFinish();
    
  }
  return DB_FAILED;
}

/**
 *  Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  if(ast->type_==kNodeQuit)
  {
    cout<<"Quit!"<<endl;
    return DB_QUIT;
  }
  else
  {
    cout<<"Quit fail."<<endl;
    return DB_FAILED;
  }
}
