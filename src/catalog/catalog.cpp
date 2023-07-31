#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    LOG(INFO)<<"seri"<<(void *)buf;
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    LOG(INFO)<<"deseri"<<(void *)buf;
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    LOG(INFO)<<magic_num;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 12+8*(table_meta_pages_.size()+index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() 
{
  LOG(INFO) << "catalogmeta ctor";
  index_meta_pages_.clear();
  table_meta_pages_.clear();
}

/**
 *  Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if(init)
  {
    LOG(INFO)<<"init";
    catalog_meta_=CatalogMeta::NewInstance();
    next_table_id_=catalog_meta_->GetNextTableId();
    next_index_id_=catalog_meta_->GetNextIndexId();
  } 
  else
  {
    LOG(INFO)<<"call deserializeFrom";
    char* buf = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID)->GetData();
    catalog_meta_=CatalogMeta::DeserializeFrom(buf);
    next_table_id_=catalog_meta_->GetNextTableId();
    next_index_id_=catalog_meta_->GetNextIndexId();
    for(map<table_id_t,page_id_t>::iterator it=catalog_meta_->table_meta_pages_.begin();it!=catalog_meta_->table_meta_pages_.end();it++)
    {
      dberr_t result=LoadTable(it->first,it->second);
      if(result!=DB_SUCCESS)
      {
        cout<<"Error:Loadtable failed.Table already exist."<<endl;
      }
    }        
    for(map<table_id_t,page_id_t>::iterator it=catalog_meta_->index_meta_pages_.begin();it!=catalog_meta_->index_meta_pages_.end();it++)
    {
      dberr_t result=LoadIndex(it->first,it->second);
      if(result!=DB_SUCCESS)
      {
        cout<<"Error:Loadindex failed.Index already exist."<<endl;
      }
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test
  **/
  FlushCatalogMetaPage();
  delete catalog_meta_;
  // for (auto iter : tables_) {
  //   delete iter.second;
  // }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
*  Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if(table_names_.find(table_name)!=table_names_.end())
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  else
  {
    table_id_t table_id=catalog_meta_->GetNextTableId();
    table_info=TableInfo::Create();
    TableHeap* table_heap=TableHeap::Create(buffer_pool_manager_,INVALID_PAGE_ID,schema,log_manager_,lock_manager_);
    auto root_page_id=table_heap->GetFirstPageId();
    TableMetadata* table_meta=TableMetadata::Create(table_id,table_name,table_heap->GetFirstPageId(),schema);
    page_id_t page_id;
    Page *table_meta_page=buffer_pool_manager_->NewPage(page_id);
    char *buf=table_meta_page->GetData();
    table_meta->SerializeTo(table_meta_page->GetData());
    table_info->Init(table_meta,table_heap);

    //目标
    tables_[table_id]=table_info;
    table_names_[table_name]=table_id;

    catalog_meta_->table_meta_pages_[table_id]=page_id;
    buffer_pool_manager_->UnpinPage(page_id,true);
    FlushCatalogMetaPage();
    return DB_SUCCESS;
  }
}

/**
 * Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name)!=table_names_.end())
  {
    table_info=tables_.find(table_names_.find(table_name)->second)->second;
    return DB_SUCCESS;
  }
  else
  {
    return DB_TABLE_NOT_EXIST;
  }
}

/**
 *  Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if(tables_.empty())
  {
    cout<<"Error:No exist table."<<endl;
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    for(unordered_map<table_id_t,TableInfo*>::const_iterator it=tables_.begin();it!=tables_.end();it++)
    {
      tables.push_back(it->second);
    }
    return DB_SUCCESS;
  }
}

/**
 * Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  if(index_names_[table_name].find(index_name)!=index_names_[table_name].end())
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  else if(table_names_.find(table_name)==table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    index_info=IndexInfo::Create();
    page_id_t table_id=table_names_[table_name];
    TableInfo *table_info=tables_[table_id];
    page_id_t index_id=catalog_meta_->GetNextIndexId();
    vector<uint32_t> key_map;
    auto schema=table_info->GetSchema();
    for(vector<string>::const_iterator it =index_keys.begin();it!=index_keys.end();it++)
    {
      uint32_t column_index;
      dberr_t result=schema->GetColumnIndex(it->data(),column_index);
      if(result==DB_SUCCESS)
      {
        key_map.push_back(column_index);
      }
      else
      {
        return DB_COLUMN_NAME_NOT_EXIST;
      }
    }
    IndexMetadata *meta_data=IndexMetadata::Create(index_id,index_name,table_id,key_map);
    page_id_t page_id;
    Page *index_meta_page=buffer_pool_manager_->NewPage(page_id);
    char* buf=index_meta_page->GetData();
    meta_data->SerializeTo(buf);
    index_info->Init(meta_data,table_info,buffer_pool_manager_);
    index_names_[table_name][index_name]=index_id;
    indexes_[index_id]=index_info;
    catalog_meta_->index_meta_pages_[index_id]=page_id;
    buffer_pool_manager_->UnpinPage(page_id,true);
    return DB_SUCCESS;
  }
}


/**
 *  Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name)==table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else if(index_names_.find(table_name)==index_names_.end()||index_names_.find(table_name)->second.find(index_name)==index_names_.find(table_name)->second.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  else
  {
    index_id_t index=index_names_.find(table_name)->second.find(index_name)->second;
    if(indexes_.find(index)==indexes_.end())
    {
      return DB_FAILED;
    }
    else
    {
      index_info=indexes_.find(index)->second;
      return DB_SUCCESS;
    }
  }
}

/**
 * Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(table_names_.find(table_name)==table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else if(index_names_.find(table_name)==index_names_.end())
  {
    return DB_INDEX_NOT_FOUND;
  }
  else
  {
    for(unordered_map<string,table_id_t>::const_iterator it=index_names_.find(table_name)->second.begin();it!=index_names_.find(table_name)->second.end();it++)
    {
      indexes.push_back(indexes_.find(it->second)->second);
    }
    return DB_SUCCESS;
  }
}

/**
 *  Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name)==table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    page_id_t table_id=table_names_[table_name];
    tables_.erase(table_id);
    table_names_.erase(table_name);
    catalog_meta_->table_meta_pages_.erase(table_id);
    if(index_names_.find(table_name)!=index_names_.end())
    {
      for(unordered_map<string,index_id_t>::iterator it=index_names_[table_name].begin();it!=index_names_[table_name].end();it++)
      {
        catalog_meta_->index_meta_pages_.erase(it->second);
        indexes_.erase(it->second);
      }
      index_names_.erase(table_name);
    }
    FlushCatalogMetaPage();
    return DB_SUCCESS;
  }
}

/**
 *  Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(table_names_.find(table_name)==table_names_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    if(index_names_.find(table_name)==index_names_.end()||index_names_.find(table_name)->second.find(index_name)==index_names_.find(table_name)->second.end())
    {
      return DB_INDEX_NOT_FOUND;
    }
    else
    {
      page_id_t index_id=index_names_[table_name][index_name];
      IndexInfo *index_info=indexes_[index_id];
      index_info->GetIndex()->Destroy();
      indexes_.erase(index_id);
      index_names_[table_name].erase(index_name);
      if(index_names_[table_name].empty())//表没有索引了
      {
        index_names_.erase(table_name);
      }
      catalog_meta_->index_meta_pages_.erase(catalog_meta_->index_meta_pages_.find(index_id));
      FlushCatalogMetaPage();
      return DB_SUCCESS;
    }
  }
}

/**
 * Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* catalogMetaPage=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalogMetaPage->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  bool result=buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return result?DB_SUCCESS:DB_FAILED;
}

/**
 *  Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.find(table_id)!=tables_.end())
  {
    return DB_TABLE_ALREADY_EXIST;
  }
  else
  {       
    Page* tableMetaPage=buffer_pool_manager_->FetchPage(page_id);
    TableMetadata *tableMetaData;
    TableMetadata::DeserializeFrom(tableMetaPage->GetData(), tableMetaData);
    table_names_[tableMetaData->GetTableName()]=table_id;
    TableHeap *tableHeap=TableHeap::Create(buffer_pool_manager_,page_id,tableMetaData->GetSchema(),log_manager_,lock_manager_);
    TableInfo *tableInfo = TableInfo::Create();
    tableInfo->Init(tableMetaData,tableHeap);
    tables_[table_id]=tableInfo;
    catalog_meta_->table_meta_pages_[table_id]=page_id;
    return DB_SUCCESS;
  }
}

/**
 * Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.find(index_id)!=indexes_.end())
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  else
  {
    Page* indexMetaPage=buffer_pool_manager_->FetchPage(page_id);
    IndexMetadata *indexMetaData;
    IndexMetadata::DeserializeFrom(indexMetaPage->GetData(), indexMetaData);

    table_id_t table_id = indexMetaData->GetTableId();
    TableInfo *tableInfo=tables_[table_id];
    string tableName=tableInfo->GetTableName();

    string indexName=indexMetaData->GetIndexName();
    index_names_[tableName][indexName]=index_id;
    IndexInfo *indexInfo = IndexInfo::Create();
    indexInfo->Init(indexMetaData,tableInfo,buffer_pool_manager_);
    indexes_[index_id]=indexInfo;
    catalog_meta_->table_meta_pages_[index_id]=page_id;
    return DB_SUCCESS;
  }
}

/**
 * Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id)==tables_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    table_info=tables_.find(table_id)->second;
    return DB_SUCCESS;
  }
}