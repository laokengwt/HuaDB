#include "log/log_records/insert_log.h"

namespace huadb {

InsertLog::InsertLog(xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t page_offset,
                     db_size_t record_size, std::shared_ptr<char> record)
    : LogRecord(LogType::INSERT, xid, prev_lsn),
      oid_(oid),
      page_id_(page_id),
      slot_id_(slot_id),
      page_offset_(page_offset),
      record_size_(record_size),
      record_(std::move(record)) {
  size_ +=
      sizeof(oid_) + sizeof(page_id_) + sizeof(slot_id_) + sizeof(page_offset_) + sizeof(record_size_) + record_size_;
}

size_t InsertLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  memcpy(data + offset, &oid_, sizeof(oid_));
  offset += sizeof(oid_);
  memcpy(data + offset, &page_id_, sizeof(page_id_));
  offset += sizeof(page_id_);
  memcpy(data + offset, &slot_id_, sizeof(slot_id_));
  offset += sizeof(slot_id_);
  memcpy(data + offset, &page_offset_, sizeof(page_offset_));
  offset += sizeof(page_offset_);
  memcpy(data + offset, &record_size_, sizeof(record_size_));
  offset += sizeof(record_size_);
  memcpy(data + offset, record_.get(), record_size_);
  offset += record_size_;
  assert(offset == size_);
  return offset;
}

std::shared_ptr<InsertLog> InsertLog::DeserializeFrom(const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  oid_t oid;
  pageid_t page_id;
  slotid_t slot_id;
  db_size_t page_offset, record_size;
  char *record;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  memcpy(&oid, data + offset, sizeof(oid));
  offset += sizeof(oid);
  memcpy(&page_id, data + offset, sizeof(page_id));
  offset += sizeof(page_id);
  memcpy(&slot_id, data + offset, sizeof(slot_id));
  offset += sizeof(slot_id);
  memcpy(&page_offset, data + offset, sizeof(page_offset));
  offset += sizeof(page_offset);
  memcpy(&record_size, data + offset, sizeof(record_size));
  offset += sizeof(record_size);
  record = new char[record_size];
  memcpy(record, data + offset, record_size);
  offset += record_size;
  return std::make_shared<InsertLog>(xid, prev_lsn, oid, page_id, slot_id, page_offset, record_size,
                                     std::shared_ptr<char>(record));
}

void InsertLog::Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn) {
  // 将插入的记录删除
  // LAB 2 BEGIN
}

void InsertLog::Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn) {
  // 如果 oid_ 不存在，表示该表已经被删除，无需 redo
  // LAB 2 BEGIN
}

oid_t InsertLog::GetOid() const { return oid_; }

pageid_t InsertLog::GetPageId() const { return page_id_; }

}  // namespace huadb
