#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_ + LEAF_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetKeySize(key_size);
  this->SetMaxSize(max_size);
  this->SetNextPageId(INVALID_PAGE_ID);
} 

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int i;
  for (i = 0; i < this->GetSize(); i++) {
    if (KM.CompareKeys(this->KeyAt(i), key) >= 0) {
      break;
    }
  }
  return i;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(nullptr, RowId());
}

// TODO: finish this
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int new_index = this->KeyIndex(key, KM);
  // if (KeyManager::CompareKeys(this->KeyAt(new_index), key) == 0) {
  //   return this->GetSize();
  // }
  for (int i = this->GetSize(); i > new_index; i--) {
    this->SetKeyAt(i,this->KeyAt(i - 1));
    this->SetValueAt(i, this->ValueAt(i - 1));
  }
  this->SetKeyAt(new_index, key);
  this->SetValueAt(new_index, value);
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int copy_start = this->GetMinSize();
  int copy_size = this->GetSize() - this->GetMinSize();
  recipient->CopyNFrom(this->PairPtrAt(copy_start), copy_size);
  this->IncreaseSize(-1 * copy_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  PairCopy(this->PairPtrAt(this->GetSize()), src, size);
  this->IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  for (int i = 0; i < this->GetSize(); i++) {
    if (KM.CompareKeys(key, this->KeyAt(i)) == 0) {
      value = this->ValueAt(i);
      return true;
    }
  }
  return false;  
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  bool found = false;
  int i;
  for (i = 0; i < this->GetSize(); i++) {
    if (KM.CompareKeys(key, this->KeyAt(i)) == 0) {
      found = true;
      break;
    }
  }
  if (not found) {
    return this->GetSize();
  }
  for (int j = i; j < this->GetSize() - 1; j++) {
    this->PairCopy(this->PairPtrAt(j), this->PairPtrAt(j + 1));
  }
  this->IncreaseSize(-1);
  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(this->PairPtrAt(0), this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(this->KeyAt(0), this->ValueAt(0));
  for (int i = 1; i < this->GetSize(); i++) {
    this->SetKeyAt(i - 1, this->KeyAt(i));
    this->SetValueAt(i - 1, this->ValueAt(i));
  }
  this->IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  this->SetKeyAt(this->GetSize(), key);
  this->SetValueAt(this->GetSize(), value);
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(this->KeyAt(this->GetSize() - 1), 
    this->ValueAt(this->GetSize() - 1));
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  for (int i = this->GetSize() - 1; i >= 0; i--) {
    this->SetKeyAt(i + 1, this->KeyAt(i));
    this->SetValueAt(i + 1, this->ValueAt(i));
  }
  this->SetKeyAt(0, key);
  this->SetValueAt(0, value);
  this->IncreaseSize(1);
}
