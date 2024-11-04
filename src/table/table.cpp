#include "table/table.h"

#include "table/table_page.h"

namespace huadb {

Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
             bool new_table, bool is_empty)
    : buffer_pool_(buffer_pool),
      log_manager_(log_manager),
      oid_(oid),
      db_oid_(db_oid),
      column_list_(std::move(column_list)) {
  if (new_table || is_empty) {
    first_page_id_ = NULL_PAGE_ID;
  } else {
    first_page_id_ = 0;
  }
}

Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
  if (record->GetSize() > MAX_RECORD_SIZE) {
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }

  // 当 write_log 参数为 true 时开启写日志功能
  // 在插入记录时增加写 InsertLog 过程
  // 在创建新的页面时增加写 NewPageLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // LAB 1 BEGIN
  // 在实验初始的Table初始化时，first_page_id_的值会被初始化为NULL_PAGE_ID，导致下面的GetPage函数执行失败
  // 即在GetPage前，需要正确的对first_page_id_进行初始化，即需要对其进行检查
  // 如果first_page_id_ == NULL_PAGE_ID，说明Table中还没有Page来存数据，则需要进行NewPage和对Page的Init操作
  if (first_page_id_ == NULL_PAGE_ID) {
    first_page_id_ = 0;
    auto new_page = buffer_pool_.NewPage(db_oid_, oid_, first_page_id_);
    auto new_table_page = std::make_unique<TablePage>(new_page);
    new_table_page->Init();
  }
  // 使用 buffer_pool_ 获取页面
  pageid_t page_id = first_page_id_;
  auto page = buffer_pool_.GetPage(db_oid_, oid_, first_page_id_);
  // 使用 TablePage 类操作记录页面
  auto table_page = std::make_unique<TablePage>(page);
  // 遍历表的页面，判断页面是否有足够的空间插入记录，如果没有则通过 buffer_pool_ 创建新页面
  while (table_page->GetFreeSpaceSize() < record->GetSize() && table_page->GetNextPageId() != NULL_PAGE_ID) {
    page_id++;
    page = buffer_pool_.GetPage(db_oid_, oid_, page_id);
    table_page = std::make_unique<TablePage>(page);
  }
  // 如果 first_page_id_ 为 NULL_PAGE_ID，说明表还没有页面，需要创建新页面
  if (table_page->GetFreeSpaceSize() < record->GetSize() && table_page->GetNextPageId() == NULL_PAGE_ID) {
    page_id++;
    // 创建新页面时需设置前一个页面的 next_page_id，并将新页面初始化
    table_page->SetNextPageId(page_id);
    table_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, page_id));
    table_page->Init();
  }
  // 找到空间足够的页面后，通过 TablePage 插入记录
  slotid_t slot_id = table_page->InsertRecord(record, xid, cid);
  // 返回插入记录的 rid
  return {page_id, slot_id};
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // LAB 1 BEGIN
  // 获取TablePage
  auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(this->db_oid_, this->oid_, rid.page_id_));
  // 使用 TablePage 操作页面
  table_page->DeleteRecord(rid.slot_id_, xid);
}

Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
  DeleteRecord(rid, xid, write_log);
  return InsertRecord(record, xid, cid, write_log);
}

void Table::UpdateRecordInPlace(const Record &record) {
  auto rid = record.GetRid();
  auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  table_page->UpdateRecordInPlace(record, rid.slot_id_);
}

pageid_t Table::GetFirstPageId() const { return first_page_id_; }

oid_t Table::GetOid() const { return oid_; }

oid_t Table::GetDbOid() const { return db_oid_; }

const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
