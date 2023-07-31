#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // get root page id
  if (leaf_max_size == UNDEFINED_SIZE) {
    this->leaf_max_size_ = ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId)) - 1);
  }
  if (internal_max_size == UNDEFINED_SIZE) {
    this->internal_max_size_ = ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t))) - 1;
  }
  auto* roots_index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (not roots_index_page->GetRootId(index_id, &this->root_page_id_)) {
    this->root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  // buffer_pool_manager->UnpinPage(this->root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  auto buffer_pool_manager = this->buffer_pool_manager_;
  if (this->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  auto* root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(this->root_page_id_)->GetData());
  buffer_pool_manager->UnpinPage(this->root_page_id_, false);
  std::queue<page_id_t> pageQueue;
  pageQueue.push(root->GetPageId());
  while (not pageQueue.empty()) {
    page_id_t thisPage = pageQueue.front();
    pageQueue.pop();
    auto* tmpPage = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(thisPage)->GetData());
    if (tmpPage->IsLeafPage()) {
      buffer_pool_manager->UnpinPage(tmpPage->GetPageId(), false);
      buffer_pool_manager->DeletePage(tmpPage->GetPageId());
    } else {
      auto* tmpInternalPage = reinterpret_cast<BPlusTreeInternalPage *>(tmpPage);
      for (int i = 0; i < tmpInternalPage->GetSize(); i++) {
        pageQueue.push(tmpInternalPage->ValueAt(i));
      }
      buffer_pool_manager->UnpinPage(tmpInternalPage->GetPageId(), false);
      buffer_pool_manager->DeletePage(tmpInternalPage->GetPageId());
    }
  }
  this->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if (this->root_page_id_ == INVALID_PAGE_ID) {
    return true;
  } else {
    return false;
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if (this->IsEmpty()) {
    return false;
  }
  RowId tmpResult;
  page_id_t current_page_id = this->root_page_id_;
  while (true) {
    auto* current_page = reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(current_page_id)->GetData());
    if (current_page->IsLeafPage()) {
      this->buffer_pool_manager_->UnpinPage(current_page_id, false);
      BPlusTreeLeafPage* current_page = reinterpret_cast<BPlusTreeLeafPage *>(this->buffer_pool_manager_->FetchPage(current_page_id)->GetData());
      if (current_page->Lookup(key, tmpResult, this->processor_)) {
        result.push_back(tmpResult);
        this->buffer_pool_manager_->UnpinPage(current_page_id, false);
        return true;
      } else {
        this->buffer_pool_manager_->UnpinPage(current_page_id, false);
        return false;
      }
    } else {
      this->buffer_pool_manager_->UnpinPage(current_page_id, false);
      InternalPage* current_page = reinterpret_cast<BPlusTreeInternalPage*>(this->buffer_pool_manager_->FetchPage(current_page_id)->GetData());
      page_id_t tmp_id = current_page_id;
      current_page_id = current_page->Lookup(key, this->processor_);
      this->buffer_pool_manager_->UnpinPage(tmp_id, false);
    }
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if (this->IsEmpty()) {
    this->StartNewTree(key, value);
    return true;
  } else {
    return this->InsertIntoLeaf(key, value, transaction);
  }
  // Page* root = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  // BPlusTreePage* root_page = reinterpret_cast<BPlusTreePage*>(root->GetData());
  // ofstream outfile;
  // outfile.open("file.dat");
  // this->ToGraph(root_page, buffer_pool_manager_, outfile);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  Page* new_page = this->buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    throw "out of memory";
  }
  auto* root = reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());
  root->Init(new_page_id, INVALID_PAGE_ID, this->processor_.GetKeySize(), this->leaf_max_size_);
  root->Insert(key, value, this->processor_);
  this->root_page_id_ = new_page_id;
  this->UpdateRootPageId(1);
  this->buffer_pool_manager_->UnpinPage(new_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  // LOG(INFO) << "i m here";
  Page* page = this->FindLeafPage(key, INVALID_PAGE_ID, false);
  // LOG(INFO) << "and im here";
  BPlusTreeLeafPage* leafPage = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  RowId dummyRow;
  // duplicate
  if (leafPage->Lookup(key, dummyRow, this->processor_)) {
    this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
    return false;
  }
  leafPage->Insert(key, value, this->processor_);
  // not max size yet
  if (leafPage->GetSize() < this->leaf_max_size_) {
    this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
    return true;
  }
  // split
  auto* splitPage = this->Split(leafPage, transaction);
  splitPage->SetNextPageId(leafPage->GetNextPageId());
  leafPage->SetNextPageId(splitPage->GetPageId());
  GenericKey* middleKey = splitPage->KeyAt(0);
  this->InsertIntoParent(leafPage, middleKey, splitPage, transaction);
  this->buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  this->buffer_pool_manager_->UnpinPage(splitPage->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = this->buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    throw "out of memory";
  }
  InternalPage* split_page = reinterpret_cast<InternalPage*>(new_page->GetData());
  split_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), this->internal_max_size_);
  node->MoveHalfTo(split_page, this->buffer_pool_manager_);
  return split_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page = this->buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    throw "out of memory";
  }
  LeafPage* split_page = reinterpret_cast<LeafPage*>(new_page->GetData());
  split_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), this->leaf_max_size_);
  node->MoveHalfTo(split_page);
  return split_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if (this->root_page_id_ == old_node->GetPageId()) {
    Page* new_page = this->buffer_pool_manager_->NewPage(this->root_page_id_);
    if (new_page == nullptr) {
      throw "out of memory";
    }
    InternalPage* new_root = reinterpret_cast<InternalPage*>(new_page->GetData());
    new_root->Init(new_page->GetPageId(), INVALID_PAGE_ID, old_node->GetKeySize(), this->internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    this->UpdateRootPageId(0);
    this->buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return;
  }
  // not a root
  Page* parent_page = this->buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage* parent_internal_page = reinterpret_cast<InternalPage*>(parent_page->GetData());
  
  parent_internal_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (parent_internal_page->GetSize() < this->internal_max_size_) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  // keep splitting
  InternalPage* parent_sib_internal_page = this->Split(parent_internal_page, transaction);
  GenericKey* new_key = parent_sib_internal_page->KeyAt(0);
  this->InsertIntoParent(parent_internal_page, new_key, parent_sib_internal_page, transaction);
  buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_sib_internal_page->GetPageId(), true);
  return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if (this->IsEmpty()) {
    return;
  }
  Page* leaf_page = this->FindLeafPage(key, INVALID_PAGE_ID, false);
  // LOG(INFO) << leaf_page->GetPageId();
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  RowId dummyRow;
  if (not leaf_node->Lookup(key, dummyRow, this->processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  // found exist
  leaf_node->RemoveAndDeleteRecord(key, this->processor_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  if (this->CoalesceOrRedistribute(leaf_node, transaction)) {
    this->buffer_pool_manager_->DeletePage(leaf_node->GetPageId());
    if (leaf_node->GetPageId() == this->root_page_id_) {
      root_page_id_ = INVALID_PAGE_ID;
    }
  }
  return;
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  // if (node->IsLeafPage()) {
  //   return false;
  // }
  if (node->IsRootPage()) {
    return this->AdjustRoot(node); 
  }
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  // get sibling
  Page* parent_page = this->buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage* parent_internal_page = reinterpret_cast<InternalPage*>(parent_page->GetData());
  int index = parent_internal_page->ValueIndex(node->GetPageId());
  int sibling_index;

  if (index == 0) {
    sibling_index = 1;
  } else {
    sibling_index = index - 1;
  }
  Page* sibling_page = this->buffer_pool_manager_->FetchPage(parent_internal_page->ValueAt(sibling_index));
  N* sibling_n_page = reinterpret_cast<N*>(sibling_page->GetData());
  // redistribute
  if (node->GetSize() + sibling_n_page->GetSize() > node->GetMaxSize()) {
    this->Redistribute(sibling_n_page, node, index);
    this->buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(sibling_n_page->GetPageId(), true);
    return false;
  }
  // merge
  bool delete_parent = this->Coalesce(sibling_n_page, node, parent_internal_page, index, transaction);
  if (delete_parent) {
    this->buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
    this->buffer_pool_manager_->DeletePage(parent_internal_page->GetPageId());
  } else {
    this->buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(sibling_n_page->GetPageId(), true);
  return true;
  // return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              index of this page in its parent values
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  // ! not quite sure about this
  if (index == 0) {
    LeafPage* tmp = neighbor_node;
    neighbor_node = node;
    node = tmp;
    index = 1;
  }
  LeafPage* leaf_node = reinterpret_cast<LeafPage*>(node);
  LeafPage* leaf_sibling_node = reinterpret_cast<LeafPage*>(neighbor_node);
  leaf_node->MoveAllTo(leaf_sibling_node);
  buffer_pool_manager_->UnpinPage(leaf_sibling_node->GetPageId(), true);
  parent->Remove(index);
  return this->CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  // ! not quite sure about this
  if (index == 0) {
    InternalPage* tmp = neighbor_node;
    neighbor_node = node;
    node = tmp;
    index = 1;
  }
  InternalPage* internal_node = reinterpret_cast<InternalPage*>(node);
  InternalPage* internal_sibling_node = reinterpret_cast<InternalPage*>(neighbor_node);
  internal_node->MoveAllTo(internal_sibling_node, parent->KeyAt(index), this->buffer_pool_manager_);
  parent->Remove(index);
  return this->CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  LeafPage* sibling_leaf_page = reinterpret_cast<LeafPage*>(neighbor_node);
  LeafPage* leaf_page = reinterpret_cast<LeafPage*>(node);
  InternalPage* parent_internal_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {
    sibling_leaf_page->MoveFirstToEndOf(leaf_page);
    parent_internal_page->SetKeyAt(1, sibling_leaf_page->KeyAt(0));  // leaf page
  } else {
    // GenericKey* sib_last = sibling_leaf_page->KeyAt(sibling_leaf_page->GetSize() - 1); 
    sibling_leaf_page->MoveLastToFrontOf(leaf_page);
    // GenericKey* new_first = leaf_page->KeyAt(leaf_page->GetSize() - 1);
    // if (processor_.CompareKeys(sib_last, new_first)) LOG(INFO) << "holy!";
    parent_internal_page->SetKeyAt(index, leaf_page->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_leaf_page->GetPageId(), true);
  return;
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  InternalPage* sibling_internal_page = reinterpret_cast<InternalPage*>(neighbor_node);
  InternalPage* internal_page = reinterpret_cast<InternalPage*>(node);
  InternalPage* parent_internal_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (index == 0) {
    sibling_internal_page->MoveFirstToEndOf(internal_page, parent_internal_page->KeyAt(1), buffer_pool_manager_); // first key in index 1
    parent_internal_page->SetKeyAt(1, sibling_internal_page->KeyAt(0));  // this is the old index 1
  } else {
    sibling_internal_page->MoveLastToFrontOf(internal_page, parent_internal_page->KeyAt(index), buffer_pool_manager_);
    parent_internal_page->SetKeyAt(index, internal_page->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_internal_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_internal_page->GetPageId(), true);
  return;
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // last deleted
  if (old_root_node->IsLeafPage() and old_root_node->GetSize() == 0) {
    return true;
  }
  // last child
  if ((not old_root_node->IsLeafPage()) and old_root_node->GetSize() == 1) {
    InternalPage* old_root_internal_page = reinterpret_cast<InternalPage*>(old_root_node);
    BPlusTreePage* only_child_page = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(old_root_internal_page->ValueAt(0))->GetData());
    only_child_page->SetParentPageId(INVALID_PAGE_ID);
    this->root_page_id_ = only_child_page->GetPageId();
    this->UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(only_child_page->GetPageId(), true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  LeafPage* left_most_leaf = this->FindLeftOrRightMostLeaf();
  return IndexIterator(left_most_leaf->GetPageId(), this->buffer_pool_manager_);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page* leaf_page = this->FindLeafPage(key);
  LeafPage* leaf_leaf_page = reinterpret_cast<LeafPage*>(leaf_page->GetData());
  int index = leaf_leaf_page->KeyIndex(key, this->processor_);
  return IndexIterator(leaf_page->GetPageId(), this->buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  LeafPage* right_most_leaf = this->FindLeftOrRightMostLeaf(false);
  return IndexIterator(right_most_leaf->GetPageId(), this->buffer_pool_manager_, right_most_leaf->GetSize());
}

LeafPage* BPlusTree::FindLeftOrRightMostLeaf(bool left) {
  if (this->IsEmpty()) {
    return nullptr;
  }
  page_id_t current_page_id = this->root_page_id_;
  while (true) {
    Page* current_page = this->buffer_pool_manager_->FetchPage(current_page_id);
    BPlusTreePage* bPTPage = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
    if (bPTPage->IsLeafPage()) {
      this->buffer_pool_manager_->UnpinPage(current_page_id, false);
      return reinterpret_cast<LeafPage*>(current_page->GetData());
    } else {
      InternalPage* internalPage = reinterpret_cast<BPlusTreeInternalPage *>(current_page->GetData());
      this->buffer_pool_manager_->UnpinPage(current_page_id, false);
      if (left) {
        current_page_id = internalPage->ValueAt(0);
      } else {
        current_page_id = internalPage->ValueAt(internalPage->GetSize() - 1);
      }
    }
  }
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (this->IsEmpty()) {
    return nullptr;
  }
  // LOG(INFO) << "im here";
  page_id_t current_page_id = this->root_page_id_;
  // LOG(INFO) << "and im here";
  while (true) {
    auto* bPTPage = reinterpret_cast<BPlusTreePage *>(this->buffer_pool_manager_->FetchPage(current_page_id)->GetData());
    if (bPTPage->IsLeafPage()) {
      // this->buffer_pool_manager_->UnpinPage(current_page_id, false);
      return this->buffer_pool_manager_->FetchPage(current_page_id);
    } else {
      this->buffer_pool_manager_->UnpinPage(bPTPage->GetPageId(), false);
      auto* internalPage = reinterpret_cast<BPlusTreeInternalPage *>(this->buffer_pool_manager_->FetchPage(current_page_id)->GetData());
      page_id_t tmp_id = current_page_id;
      // LOG(INFO) << "and im hereh";
      if (leftMost) {
        current_page_id = internalPage->ValueAt(0);
      } else {
        current_page_id = internalPage->Lookup(key, this->processor_);
      }
      this->buffer_pool_manager_->UnpinPage(tmp_id, false);
    }
  }
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto* indexPage = reinterpret_cast<IndexRootsPage*>(this->buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (this->root_page_id_ == INVALID_PAGE_ID) {
    indexPage->Delete(this->index_id_);
  } else if (insert_record) {
    indexPage->Insert(this->index_id_, this->root_page_id_);
  } else {
    indexPage->Update(this->index_id_, this->root_page_id_);
  }
  this->buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}