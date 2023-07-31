#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  std::pair<GenericKey *, RowId> ret;
  ret.first = this->page->KeyAt(this->item_index);
  ret.second = this->page->ValueAt(this->item_index);
  return ret;
}

IndexIterator &IndexIterator::operator++() {
  // still in this leaf
  if (this->item_index < this->page->GetSize() - 1) {
    this->item_index++;
    return *this;
  }
  // end
  if (this->page->GetNextPageId() == INVALID_PAGE_ID) {
    this->item_index++;
    return *this;
  }
  // next page
  LeafPage* next_page = reinterpret_cast<LeafPage*>(this->buffer_pool_manager->FetchPage(this->page->GetNextPageId())->GetData());
  this->page = next_page;
  this->current_page_id = next_page->GetPageId();
  this->item_index = 0;
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}
