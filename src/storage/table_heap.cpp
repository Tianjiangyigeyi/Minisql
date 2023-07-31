#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // LOG(INFO) << "insert tuple";
  if (row.GetSerializedSize(schema_) > PAGE_SIZE - 32) {
    return false;
  }
  page_id_t page_id = first_page_id_;
  page_id_t last_page_id = INVALID_PAGE_ID;
  TablePage *page = nullptr;
  while (page_id != INVALID_PAGE_ID && page_id != last_page_id) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr) {
      return false;
    }
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(page_id, true);
      return true;
    } else {
      last_page_id = page_id;
      page_id = page->GetNextPageId();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    }
  }
  // no page, need a new page
  LOG(INFO) << "ksejrhgkj";
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  if (page != nullptr) {
    page->SetNextPageId(page_id);
    new_page->Init(page_id, page->GetPageId(), log_manager_, txn);
  } else {
    first_page_id_ = new_page->GetPageId();
    new_page->Init(page_id, INVALID_PAGE_ID, log_manager_, txn);
  }
  LOG(INFO) << "ksejrhgkj";
  if(new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){
    LOG(INFO) << "insdet tuple";
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page_id, true);
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row(row);
  if (!GetTuple(&old_row, txn)) {
    return false;
  }
  page->WLatch();
  if (page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  } else {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->RLatch();
  if (page->GetTuple(row, schema_, txn, lock_manager_)) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  } else {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId* rid = new RowId;
  first_page->RLatch();
  // first_page->GetFirstTupleRid(rid);
  bool validFirstTuple = first_page->GetFirstTupleRid(rid);
  if(not validFirstTuple)return this->End();
  Row *row = new Row(*rid);
  first_page->GetTuple(row, schema_, txn, lock_manager_);
  first_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, row);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this);
}