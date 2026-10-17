#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

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

    // (account_id, client_order_id) -> the seq it was given. Per partition is
    // enough (contract 4.3): a genuine resend always carries the same symbol,
    // so it lands in the same partition; reusing an id for a different symbol
    // is caught earlier by the cash owner's own dedupe. Memory only, unbounded
    // for now: lost on restart and not cleared at session boundaries (both
    // [later], with the journal).
    struct DedupeKeyHash {
        std::size_t operator()(const std::pair<std::uint64_t, std::string> &k) const noexcept {
            return std::hash<std::string>{}(k.second) ^ (std::hash<std::uint64_t>{}(k.first) * 0x9e3779b97f4a7c15ULL);
        }
    };
    std::unordered_map<std::pair<std::uint64_t, std::string>, std::uint64_t, DedupeKeyHash> seen_;
};

} // namespace sequencer
