#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *h1, Row *r1) : table_heap_(h1), row_(r1) {}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_ = other.row_;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (row_ == nullptr && itr.row_ == nullptr) {
    return true;
  } else if (row_ == nullptr || itr.row_ == nullptr) {
    if (row_ == nullptr) {
      return itr.row_->GetRowId() == INVALID_ROWID;
    } else {
      return row_->GetRowId() == INVALID_ROWID;
    }
  }
  return row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(this->operator==(itr)); }

const Row &TableIterator::operator*() { return *row_; }

Row *TableIterator::operator->() { return row_; }

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->table_heap_ = itr.table_heap_;
  this->row_ = itr.row_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  // return *this;
  // 若为空或非法则返回非法
  LOG(INFO) << "dsadas";
  if (row_ == nullptr || row_->GetRowId() == INVALID_ROWID) {
    delete row_;
    row_ = new Row(INVALID_ROWID);
    return *this;
  }
  LOG(INFO) << "dsadasadds";
  TablePage *table_page =
      reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(this->row_->GetRowId().GetPageId()));
  RowId *next_rid = new RowId();
  table_page->RLatch();
  // 若当页能找到row
  LOG(INFO) << "dsadasadds";

  if (table_page->GetNextTupleRid(row_->GetRowId(), next_rid)) {
    LOG(INFO) << "dsadasadds";
    delete row_;
    row_ = new Row(*next_rid);
    table_heap_->GetTuple(row_, nullptr);
    table_heap_->buffer_pool_manager_->UnpinPage(table_page->GetPageId(), false);
    table_page->RUnlatch();
    return *this;
  } else {
    LOG(INFO) << "dsadasdghtrhdds";
    // 当页没找到,找下一页, 直到找到可用的
    auto next_page_id = table_page->GetNextPageId();
    LOG(INFO) << "d62651dds";
    while (next_page_id != INVALID_PAGE_ID) {
      LOG(INFO) << "d62dsfwwv231ds";

      table_page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(table_page->GetPageId(), false);
      table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      table_page->RLatch();
      if (table_page->GetFirstTupleRid(next_rid)) {
        LOG(INFO) << "if";
        // 找到了可用的
        delete row_;
        row_ = new Row(*next_rid);
        table_heap_->GetTuple(row_, nullptr);
        table_heap_->buffer_pool_manager_->UnpinPage(table_page->GetPageId(), false);
        table_page->RUnlatch();
        return *this;
      } else {
        LOG(INFO) << "else";
        // 没有找到, 继续找
        next_page_id = table_page->GetNextPageId();
      }
    }
    LOG(INFO) << "2";
    // 没有可用的tuple, 返回非法
    delete row_;
    row_ = new Row(INVALID_ROWID);
    table_heap_->buffer_pool_manager_->UnpinPage(table_page->GetPageId(), false);
    table_page->RUnlatch();
    return *this;
  }
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator it(*this);
  this->operator++();
  return TableIterator(it);
}
