#include "log/log_records/delete_log.h"

namespace huadb {

DeleteLog::DeleteLog(xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t page_id, slotid_t slot_id)
    : LogRecord(LogType::DELETE, xid, prev_lsn), oid_(oid), page_id_(page_id), slot_id_(slot_id) {
  size_ += sizeof(oid_) + sizeof(page_id_) + sizeof(slot_id_);
}

size_t DeleteLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  memcpy(data + offset, &oid_, sizeof(oid_));
  offset += sizeof(oid_);
  memcpy(data + offset, &page_id_, sizeof(page_id_));
  offset += sizeof(page_id_);
  memcpy(data + offset, &slot_id_, sizeof(slot_id_));
  offset += sizeof(slot_id_);
  assert(offset == size_);
  return offset;
}

std::shared_ptr<DeleteLog> DeleteLog::DeserializeFrom(const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  oid_t oid;
  pageid_t page_id;
  slotid_t slot_id;
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
  return std::make_shared<DeleteLog>(xid, prev_lsn, oid, page_id, slot_id);
}

void DeleteLog::Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn) {
  // LAB 2 BEGIN
}

void DeleteLog::Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn) {
  // 如果 oid_ 不存在，表示该表已经被删除，无需 redo
  // LAB 2 BEGIN
}

oid_t DeleteLog::GetOid() const { return oid_; }

pageid_t DeleteLog::GetPageId() const { return page_id_; }

}  // namespace huadb
