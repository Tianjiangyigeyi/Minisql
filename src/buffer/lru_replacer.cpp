#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * //TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::recursive_mutex> lock(LRU_latch_);
  if (LRU_list.empty()) {
    return false;
  }
  *frame_id = LRU_list.front();
  LRU_list.pop_front();
  auto it = LRU_hash.find(*frame_id);
  if (it != LRU_hash.end()) {
    LRU_hash.erase(it);
    return true;
  }
  return false;
}

/**
 * //TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::recursive_mutex> lock(LRU_latch_);
  if (LRU_hash.find(frame_id) != LRU_hash.end()) {
    LRU_list.erase(LRU_hash[frame_id]);
    LRU_hash.erase(frame_id);
  }
}

/**
 * //TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::recursive_mutex> lock(LRU_latch_);
  if (LRU_hash.find(frame_id) == LRU_hash.end()) {
    LRU_list.push_back(frame_id);
    LRU_hash[frame_id] = prev(LRU_list.end());
  }
}

/**
 * //TODO: Student Implement
 */
size_t LRUReplacer::Size() { return LRU_list.size(); }
