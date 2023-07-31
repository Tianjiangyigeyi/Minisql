#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_ + INTERNAL_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetKeySize(key_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  // LOG(INFO) << "in here";
  for (int i = 1; i < this->GetSize(); i++) {
    // LOG(INFO) << "the round " << i;
    //     if (i == 1) {
    //   LOG(INFO) << "first loopkup " << this->KeyAt(i);
    // }
    // if (i == 2) {
    //   LOG(INFO) << "second loopkup " << this->KeyAt(i);
    // }
    if (KM.CompareKeys(this->KeyAt(i), key) > 0) {
      // LOG(INFO) << "and im here";
      return this->ValueAt(i - 1);
    }
  }
  return this->ValueAt(this->GetSize() - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  this->SetValueAt(0, old_value);
  this->SetValueAt(1, new_value);
  this->SetKeyAt(1, new_key);
  this->SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int old_index = this->ValueIndex(old_value);
  for (int i = this->GetSize(); i > old_index + 1; i--) {
    this->PairCopy(this->PairPtrAt(i), this->PairPtrAt(i - 1));
  }
  this->SetKeyAt(old_index + 1, new_key);
  this->SetValueAt(old_index + 1, new_value);
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int new_size = this->GetMinSize();
  recipient->CopyNFrom(this->PairPtrAt(new_size), this->GetMaxSize() - new_size, buffer_pool_manager);
  this->SetSize(new_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  this->PairCopy(this->PairPtrAt(this->GetSize()), src, size);
  for (int i = 0; i < size; i++) {
    auto *page = buffer_pool_manager->FetchPage(this->ValueAt(i + this->GetSize()));
    if (page != nullptr) {
      auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      node->SetParentPageId(this->GetPageId());
      /* do something */
      buffer_pool_manager->UnpinPage(page->GetPageId(), true);
    }
  }
  this->IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  if (index >= 0 and index < this->GetSize()) {
    for (int i = index; i < this->GetSize() - 1; i++) {
      this->SetKeyAt(i, this->KeyAt(i + 1));
      this->SetValueAt(i, this->ValueAt(i + 1));
    }
    // std::move(this->PairPtrAt(index + 1), this->PairPtrAt(this->GetSize()), this->PairPtrAt(index));
    this->IncreaseSize(-1);
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  this->SetSize(0);
  return this->ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  recipient->CopyNFrom(this->PairPtrAt(0), this->GetSize(), buffer_pool_manager);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(this->KeyAt(0), this->ValueAt(0), buffer_pool_manager);
  this->Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(this->GetSize(), key);
  this->SetValueAt(this->GetSize(), value);
  this->IncreaseSize(1);
  auto *page = buffer_pool_manager->FetchPage(value);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    node->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(this->ValueAt(this->GetSize() - 1), buffer_pool_manager);          
  recipient->SetKeyAt(1, middle_key);
  recipient->SetKeyAt(0, this->KeyAt(this->GetSize() - 1));
  this->IncreaseSize(-1);                       
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  for (int i = this->GetSize(); i > 0; i--) {
    this->SetKeyAt(i, this->KeyAt(i - 1));
    this->SetValueAt(i, this->ValueAt(i - 1));
  }
  this->SetValueAt(0, value);
  this->IncreaseSize(1);
  auto *page = buffer_pool_manager->FetchPage(value);
  if (page != nullptr) {
    auto *node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    node->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
}
