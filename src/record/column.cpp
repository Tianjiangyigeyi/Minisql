#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
 * TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  // First, write the magic num
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // Then, write the private data one by one. tips: write the size of name_ before.
  MACH_WRITE_TO(uint32_t, buf + offset, name_.size());
  offset += sizeof(uint32_t);
  MACH_WRITE_STRING(buf + offset, name_);
  offset += name_.size();
  MACH_WRITE_TO(TypeId, buf + offset, type_);
  offset += sizeof(TypeId);
  MACH_WRITE_TO(uint32_t, buf + offset, len_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf + offset, nullable_);
  offset += sizeof(bool);
  MACH_WRITE_TO(bool, buf + offset, unique_);
  offset += sizeof(bool);
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(uint32_t) * 4 + name_.size() + sizeof(bool) * 2 + sizeof(TypeId);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  ASSERT(MACH_READ_FROM(uint32_t, buf) == COLUMN_MAGIC_NUM, "Wrong Column Serialized.");
  uint32_t offset = 0;
  offset += sizeof(uint32_t);

  uint32_t name_size = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  std::string name;
  for (uint32_t i = 0; i < name_size; ++i) {
    name += MACH_READ_FROM(char, buf + offset);
    offset += sizeof(char);
  }
  auto type = MACH_READ_FROM(TypeId, buf + offset);
  offset += sizeof(TypeId);
  auto len = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  auto table_ind = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  auto nullable = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  auto unique = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  if(type==kTypeChar)
  {
      column = new Column(name, type, len,table_ind, nullable, unique);
  }
  else
  {
      column = new Column(name, type, table_ind, nullable, unique);
  }
  
  return offset;
}
