#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // just as in column, write size first
  uint32_t tmp = columns_.size();
  MACH_WRITE_TO(uint32_t, buf + offset, tmp);
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < tmp; ++i) {
    offset += columns_[i]->SerializeTo(buf + offset);
  }
  MACH_WRITE_TO(bool, buf + offset, is_manage_);
  offset += sizeof(bool);
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t sum = sizeof(uint32_t) * 2 + sizeof(bool);
  for (auto &it : columns_) {
    sum += it->GetSerializedSize();
  }
  return sum;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  ASSERT(MACH_READ_FROM(uint32_t, buf) == SCHEMA_MAGIC_NUM, "Wrong Schema Serialized.");
  uint32_t offset = 0;
  offset += sizeof(uint32_t);
  std::vector<Column *> columns;
  uint32_t columns_size = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < columns_size; ++i) {
    Column *tmp = nullptr;
    offset += Column::DeserializeFrom(buf + offset, tmp);
    columns.push_back(tmp);
  }
  auto is_manage = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  schema = new Schema(columns, is_manage);
  return offset;
}