#pragma once

#include "storage/buffer_strategy.h"

namespace huadb {

class LRUBufferStrategy : public BufferStrategy {
 public:
  LRUBufferStrategy();
  void Access(size_t frame_no) override;
  size_t Evict() override;

 private:
  // 缓存页面存在时间数组
  size_t *time;
};

}  // namespace huadb
