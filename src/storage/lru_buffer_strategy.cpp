#include "storage/lru_buffer_strategy.h"

#include "common/constants.h"

namespace huadb {

LRUBufferStrategy::LRUBufferStrategy() {
  // 初始化time数组
  time = new size_t[BUFFER_SIZE];
  for (size_t i = 0; i < BUFFER_SIZE; ++i) {
    time[i] = -1;
  }
}

void LRUBufferStrategy::Access(size_t frame_no) {
  // LAB 1 BEGIN
  // 缓存页面访问
  for (size_t i = 0; i < BUFFER_SIZE; i++) {
    // 没访问到的页面存在时间+1
    if (time[i] != -1) {
      time[i]++;
    }
  }
  // 访问到的页面重置存在时间
  time[frame_no] = 0;
  return;
};

size_t LRUBufferStrategy::Evict() {
  // LAB 1 BEGIN
  // 缓存页面淘汰，返回淘汰的页面在 buffer pool 中的下标
  size_t max_index = 0;
  // 遍历time数组获取最久没有访问的页面的下标
  for (size_t i = 1; i < BUFFER_SIZE; ++i) {
    if (time[max_index] < time[i]) {
      max_index = i;
    }
  }
  return max_index;
}

}  // namespace huadb
