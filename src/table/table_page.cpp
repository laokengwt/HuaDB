#include "table/table_page.h"

#include <sstream>

namespace huadb {

TablePage::TablePage(std::shared_ptr<Page> page) : page_(page) {
  page_data_ = page->GetData();
  db_size_t offset = 0;
  page_lsn_ = reinterpret_cast<lsn_t *>(page_data_);
  offset += sizeof(lsn_t);
  next_page_id_ = reinterpret_cast<pageid_t *>(page_data_ + offset);
  offset += sizeof(pageid_t);
  lower_ = reinterpret_cast<db_size_t *>(page_data_ + offset);
  offset += sizeof(db_size_t);
  upper_ = reinterpret_cast<db_size_t *>(page_data_ + offset);
  offset += sizeof(db_size_t);
  assert(offset == PAGE_HEADER_SIZE);
  slots_ = reinterpret_cast<Slot *>(page_data_ + PAGE_HEADER_SIZE);
}

void TablePage::Init() {
  *page_lsn_ = 0;
  *next_page_id_ = NULL_PAGE_ID;
  *lower_ = PAGE_HEADER_SIZE;
  *upper_ = DB_PAGE_SIZE;
  page_->SetDirty();
}

slotid_t TablePage::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid) {
  // 在记录头添加事务信息（xid 和 cid）
  // LAB 3 BEGIN

  // LAB 1 BEGIN
  slotid_t slot_id = GetRecordCount();
  // lower需要往后移动一个槽的大小;upper需要往前移动一个记录的大小
  Slot *slot = reinterpret_cast<Slot *>(page_data_ + *lower_);
  slot->size_ = record->GetSize();
  // 维护 lower 和 upper 指针
  *lower_ += sizeof(Slot);
  *upper_ -= slot->size_;
  // 设置 slots 数组
  slot->offset_ = *upper_;
  char *record_begin = reinterpret_cast<char *>(page_data_ + *upper_);
  // 将 record 写入 page data
  record->SerializeTo(record_begin);
  // 将 page 标记为 dirty
  page_->SetDirty();
  // 返回插入的 slot id
  return slot_id;
}

void TablePage::DeleteRecord(slotid_t slot_id, xid_t xid) {
  // 更改实验 1 的实现，改为通过 xid 标记删除
  // LAB 3 BEGIN

  // LAB 1 BEGIN
  // 利用slot_id计算record_data所在位置并获取record
  Slot *slot = slots_ + slot_id;
  char *record_data = reinterpret_cast<char *>(page_data_ + slot->offset_);
  std::unique_ptr<Record> record = std::make_unique<Record>();
  // 可使用 Record::DeserializeHeaderFrom 函数读取记录头
  record->DeserializeHeaderFrom(record_data);
  // 将 slot_id 对应的 record 标记为删除
  record->SetDeleted(true);
  record->SerializeHeaderTo(record_data);
}

void TablePage::UpdateRecordInPlace(const Record &record, slotid_t slot_id) {
  record.SerializeTo(page_data_ + slots_[slot_id].offset_);
  page_->SetDirty();
}

std::shared_ptr<Record> TablePage::GetRecord(Rid rid, const ColumnList &column_list) {
  // LAB 1 BEGIN
  // 新建 record 并设置 rid
  std::shared_ptr<Record> record = std::shared_ptr<Record>(new Record());
  record->SetRid(rid);
  // 根据 slot_id 获取 record
  // slot_id -> record_data_begin_ptr
  char *record_begin = reinterpret_cast<char *>(page_data_ + slots_[rid.slot_id_].offset_);
  record->DeserializeFrom(record_begin, column_list);
  return record;
}

void TablePage::UndoDeleteRecord(slotid_t slot_id) {
  // 修改 undo delete 的逻辑
  // LAB 3 BEGIN

  // 清除记录的删除标记
  // 将页面设为 dirty
  // LAB 2 BEGIN
}

void TablePage::RedoInsertRecord(slotid_t slot_id, char *raw_record, db_size_t page_offset, db_size_t record_size) {
  // 将 raw_record 写入 page data
  // 注意维护 lower 和 upper 指针，以及 slots 数组
  // 将页面设为 dirty
  // LAB 2 BEGIN
}

db_size_t TablePage::GetRecordCount() const { return (*lower_ - PAGE_HEADER_SIZE) / sizeof(Slot); }

lsn_t TablePage::GetPageLSN() const { return *page_lsn_; }

pageid_t TablePage::GetNextPageId() const { return *next_page_id_; }

db_size_t TablePage::GetLower() const { return *lower_; }

db_size_t TablePage::GetUpper() const { return *upper_; }

db_size_t TablePage::GetFreeSpaceSize() const {
  if (*upper_ < *lower_ + sizeof(Slot)) {
    return 0;
  } else {
    return *upper_ - *lower_ - sizeof(Slot);
  }
}

void TablePage::SetNextPageId(pageid_t page_id) {
  *next_page_id_ = page_id;
  page_->SetDirty();
}

void TablePage::SetPageLSN(lsn_t page_lsn) {
  *page_lsn_ = page_lsn;
  page_->SetDirty();
}

std::string TablePage::ToString() const {
  std::ostringstream oss;
  oss << "TablePage[" << std::endl;
  oss << "  page_lsn: " << *page_lsn_ << std::endl;
  oss << "  next_page_id: " << *next_page_id_ << std::endl;
  oss << "  lower: " << *lower_ << std::endl;
  oss << "  upper: " << *upper_ << std::endl;
  if (*lower_ > *upper_) {
    oss << "\n***Error: lower > upper***" << std::endl;
  }
  oss << "  slots: " << std::endl;
  for (size_t i = 0; i < GetRecordCount(); i++) {
    oss << "    " << i << ": offset " << slots_[i].offset_ << ", size " << slots_[i].size_ << " ";
    if (slots_[i].size_ <= RECORD_HEADER_SIZE) {
      oss << "***Error: record size smaller than header size***" << std::endl;
    } else if (slots_[i].offset_ + RECORD_HEADER_SIZE >= DB_PAGE_SIZE) {
      oss << "***Error: record offset out of page boundary***" << std::endl;
    } else {
      RecordHeader header;
      header.DeserializeFrom(page_data_ + slots_[i].offset_);
      oss << header.ToString() << std::endl;
    }
  }
  oss << "]\n";
  return oss.str();
}

}  // namespace huadb
