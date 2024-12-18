#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) == current_reads_.end()) {
    current_reads_.insert({read_ts, 1});
    small_top_heap_.push(read_ts);
  } else {
    current_reads_[read_ts]++;
  }

  if (read_ts < watermark_) {
    watermark_ = read_ts;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) == current_reads_.end() || current_reads_[read_ts] == 0) {
    throw Exception("read_ts doesn't exists in watermark!");
  }

  current_reads_[read_ts]--;
  // 恰好删除的是最小读时间戳且数量变成0
  if (read_ts == watermark_ && current_reads_[read_ts] == 0) {
    while (!small_top_heap_.empty() && current_reads_[small_top_heap_.top()] == 0) {
      small_top_heap_.pop();
    }

    if (small_top_heap_.empty()) {
      watermark_ = commit_ts_;
    } else {
      watermark_ = small_top_heap_.top();
    }
  }
}

}  // namespace bustub
