#include "record/row.h"
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  ASSERT(buf != nullptr, "buf can't be null");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t field_num = fields_.size();
  
  // Write field numbers to the buffer
 
  MACH_WRITE_UINT32(buf+offset, field_num);
  offset += 4;
  if (field_num == 0) {
    return offset;
  }
  // Write the null bitmap to the buffer
  uint32_t null_bitmap_size = (field_num-1) / 8 + 1;
  char *null_bitmap = new char[null_bitmap_size]();

  for (uint32_t i = 0; i < null_bitmap_size; ++i) {
    for (uint32_t j = 0; (j < 8) && (i * 8 + j < field_num); ++j) {
      if (fields_[i * 8 + j] == nullptr || fields_[i * 8 + j]->IsNull()) {
        null_bitmap[i] |= (0x01 << j);
      }
    }
    MACH_WRITE_TO(char, buf + offset, null_bitmap[i]);
    offset += sizeof(char);
    
  }
  // Write fields to the buffer
  for (uint32_t i = 0; i < field_num; ++i) {
    if (fields_.at(i) != nullptr && !fields_.at(i)->IsNull()) {
      offset += fields_.at(i)->SerializeTo(buf + offset);
    }
  }
  delete[] null_bitmap;
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t field_num = MACH_READ_FROM(uint32_t, buf);
  // LOG(INFO) << "field_num = " << field_num;
  offset += sizeof(uint32_t);
  if(field_num == 0){
    return offset;
  }
  //LOG(INFO) << "DeserializeFrom1";
  uint32_t null_bitmap_size = (field_num-1) / 8 + 1;
  char *null_bitmap = new char[null_bitmap_size]();
  for (uint32_t i = 0; i < null_bitmap_size; ++i) {
    null_bitmap[i] = MACH_READ_FROM(char, buf + offset);
    offset += sizeof(char);
  }
  //LOG(INFO) << "DeserializeFrom2";
  //LOG(INFO) << "schema->GetColumnCount() = " << schema->GetColumnCount();
  for (uint32_t i = 0; i < field_num; ++i) {
    Field *f_tmp = nullptr;
    offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &f_tmp, null_bitmap[i / 8] & (0x01 << (i % 8)));
    fields_.push_back(f_tmp);
  }
  //LOG(INFO) << "DeserializeFrom3";
  delete[] null_bitmap;
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = 0;
  uint32_t field_num = fields_.size();
  if (field_num == 0) {
    return 4;
  }
  size += 4 + ((field_num-1) / 8 + 1);
  for (uint32_t i = 0; i < field_num; ++i) {
    if (fields_.at(i) == nullptr) {
      continue;
    }
    if (!fields_.at(i)->IsNull()) {
      size += fields_.at(i)->GetSerializedSize();
    }
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}