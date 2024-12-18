#pragma once

#include <functional>
#include <queue>
#include <unordered_map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  /** 正在进行的事务中最小的读取时间戳 */
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }

  /** 应该是最新的提交时间戳 */
  timestamp_t commit_ts_;

  /** 最小的读取时间戳 */
  timestamp_t watermark_;

  /** 当前所有读取事务的时间戳及其对应的计数。哈希表的键是读取时间戳
   * 值是一个计数器，表示有多少个事务在该时间戳上读取数据。（多线程） */
  std::unordered_map<timestamp_t, int> current_reads_;
  std::priority_queue<timestamp_t, std::vector<timestamp_t>, std::greater<>> small_top_heap_;
};

};  // namespace bustub
