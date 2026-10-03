#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>

#include "exchange/v1/sequencer.pb.h"

namespace sequencer {

namespace pb = exchange::v1;
using Clock = std::function<std::int64_t()>;

enum class AppendStatus { Accepted, Busy };

struct AppendResult {
    AppendStatus status;
    std::uint64_t seq = 0;
};

class Partition {
  public:
    Partition(std::size_t capacity, Clock clock);
    Partition(const Partition &) = delete;
    Partition &operator=(const Partition &) = delete;

    AppendResult append(const pb::SubmitRequest &req);
    std::optional<pb::SequencedCommand> wait_front(std::chrono::milliseconds timeout);
    void pop_front();
    std::uint64_t next_unsent() const;
    bool try_claim_subscriber();
    void release_subscriber();

  private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<pb::SequencedCommand> queue_;
    std::size_t capacity_;
    Clock clock_;
    std::uint64_t next_seq_ = 1;
    std::int64_t last_ts_ = 0;
    bool subscribed_ = false;
};

} // namespace sequencer
