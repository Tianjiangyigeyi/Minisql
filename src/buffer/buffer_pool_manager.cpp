#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * //TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock<std::recursive_mutex> lock(latch_);
  // 1.1
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    //LOG(INFO)<<"1.1";
    ++pages_[it->second].pin_count_;
    replacer_->Pin(it->second);
    //LOG(INFO)<<"&pages_[it->second]:"<<&pages_[it->second];
    return &pages_[it->second];
  }
  // 1.2
  frame_id_t R_frame_id;
  if (!free_list_.empty()) {
    R_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&R_frame_id)) {
    LOG(INFO)<<"1.2";
    return nullptr;
  }
  // 2.
  auto R_page_id = pages_[R_frame_id].GetPageId();
  if (pages_[R_frame_id].IsDirty()) {
    FlushPage(R_page_id);
  }
  // 3.
  page_table_.erase(R_page_id);
  page_table_[page_id] = R_frame_id;
  // 4.
  replacer_->Pin(R_frame_id);
  Page &P_page = pages_[R_frame_id];
  P_page.page_id_ = page_id;
  P_page.is_dirty_ = false;
  P_page.pin_count_ = 1;
  disk_manager_->ReadPage(page_id, P_page.GetData());
  char* str=pages_[R_frame_id].GetData();
  return &P_page;
}

/**
 * //TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  // 2.
  frame_id_t P_frame_id;
  if (!free_list_.empty()) {
    P_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&P_frame_id)) {
    return nullptr;
  } else {
    page_table_.erase(pages_[P_frame_id].GetPageId());
  }
  // 3.
  Page &P_page = pages_[P_frame_id];
  // 0.
  page_id = this->AllocatePage();
  page_table_[page_id] = P_frame_id;
  P_page.page_id_ = page_id;
  P_page.is_dirty_ = true;
  P_page.pin_count_ = 1;
  P_page.ResetMemory();

  // 4.
  return &P_page;
}

/**
 * //TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  // 0.
  this->DeallocatePage(page_id);
  // 1.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  auto P_frame_id = page_table_[page_id];
  // 2.
  if (pages_[P_frame_id].pin_count_) {
    return false;
  }
  // 3.
  page_table_.erase(page_id);
  replacer_->Pin(P_frame_id);
  Page &P_page = pages_[P_frame_id];
  P_page.is_dirty_ = false;
  P_page.pin_count_ = 0;
  P_page.page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(P_frame_id);
  return true;
}

/**
 * //TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  Page &page = pages_[it->second];
  if (page.pin_count_ > 0) {
    --page.pin_count_;
    if (page.pin_count_ == 0) {
      replacer_->Unpin(it->second);
    }
    page.is_dirty_ |= is_dirty;
    return true;
  }
  return false;
}

/**
 * //TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock<std::recursive_mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  Page &page = pages_[it->second];
  disk_manager_->WritePage(page_id, page.GetData());
  page.is_dirty_ = false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
