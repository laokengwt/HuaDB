#include "table/table_scan.h"

#include "table/table_page.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // LAB 1 BEGIN
  // 注意处理扫描空表的情况（rid_.page_id_ 为 NULL_PAGE_ID）
  if (rid_.page_id_ == NULL_PAGE_ID) {
    // 扫描结束时，返回空指针
    return nullptr;
  }
  std::shared_ptr<Page> page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
  auto table_page = std::make_unique<TablePage>(page);
  // 每次调用读取一条记录
  std::shared_ptr<Record> record = table_page->GetRecord({rid_.page_id_, rid_.slot_id_}, table_->GetColumnList());
  record->SetRid({rid_.page_id_, rid_.slot_id_});
  // 读取时更新 rid_ 变量，避免重复读取
  if (rid_.slot_id_ + 1 < table_page->GetRecordCount()) {
    rid_.slot_id_++;
  } else {
    rid_.page_id_ = table_page->GetNextPageId();
    rid_.slot_id_ = 0;
  }
  // 判断记录是否已经被标记为删除，不再返回已经删除的数据
  if (record->IsDeleted()) {
    // 即递归下去找下一记录
    return GetNextRecord(xid, isolation_level, cid, active_xids);
  }
  return record;
}

}  // namespace huadb
