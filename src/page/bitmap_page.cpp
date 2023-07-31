#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * //TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= GetMaxSupportedSize()) {
    return false;
  }
  ++page_allocated_;
  page_offset = next_free_page_;
  auto byte_index = page_offset / 8;
  auto bit_index = page_offset % 8;
  bytes[byte_index] |= 1 << bit_index;
  for (size_t i = 0; i < MAX_CHARS; ++i) {
    if (bytes[i] != 0xff) {
      for (uint32_t j = 0; j < 8; ++j) {
        if (!((bytes[i] >> j) & 0x01)) {
          next_free_page_ = i * 8 + j;
          break;
        }
      }
      break;
    }
  }
  return true;
}

/**
 * //TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  auto byte_index = page_offset / 8;
  auto bit_index = page_offset % 8;
  if ((bytes[byte_index] >> bit_index) & 0x01) {
    --page_allocated_;
    bytes[byte_index] -= (0x01 << bit_index);
    for (size_t i = 0; i < MAX_CHARS; ++i) {
      if (bytes[i] != 0xff) {
        for (uint32_t j = 0; j < 8; ++j) {
          if (!((bytes[i] >> j) & 0x01)) {
            next_free_page_ = i * 8 + j;
            break;
          }
        }
        break;
      }
    }
    return true;
  }
  return false;
}

/**
 * //TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (8 * byte_index + bit_index >= GetMaxSupportedSize()) {
    return false;
  }
  if ((bytes[byte_index] >> bit_index) & 0x01) {
    return false;
  }
  return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
