#include "sequencer/partition.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace sequencer {

Partition::Partition(std::size_t capacity, Clock clock) : capacity_(capacity), clock_(std::move(clock)) {
    if (capacity_ == 0)
        throw std::invalid_argument("partition capacity must be positive");
    if (!clock_)
        throw std::invalid_argument("partition clock is required");
}

AppendResult Partition::append(const pb::SubmitRequest &req) {
    std::uint64_t seq = 0;
    {
        // One lock keeps sequence assignment, timestamping, and enqueueing inseparable.
        std::lock_guard lock(mu_);
        if (queue_.size() >= capacity_)
            return {AppendStatus::Busy, 0};

        // A wall clock can move backward; timestamps must not as sequences increase.
        const std::int64_t ts = std::max(clock_(), last_ts_);
        last_ts_ = ts;
        seq = next_seq_++;

        pb::SequencedCommand command;
        command.set_seq(seq);
        command.set_ts(ts);
        command.set_symbol(req.symbol());
        command.set_request_id(req.request_id());
        command.set_gateway_id(req.gateway_id());
        command.set_payload(req.payload());
        queue_.push_back(std::move(command));
    }
    cv_.notify_one();
    return {AppendStatus::Accepted, seq};
}

std::optional<pb::SequencedCommand> Partition::wait_front(std::chrono::milliseconds timeout) {
    std::unique_lock lock(mu_);
    if (!cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); }))
        return std::nullopt;
    return queue_.front();
}

void Partition::pop_front() {
    std::lock_guard lock(mu_);
    if (!queue_.empty())
        queue_.pop_front();
}

std::uint64_t Partition::next_unsent() const {
    std::lock_guard lock(mu_);
    return queue_.empty() ? next_seq_ : queue_.front().seq();
}

bool Partition::try_claim_subscriber() {
    std::lock_guard lock(mu_);
    if (subscribed_)
        return false;
    subscribed_ = true;
    return true;
}

void Partition::release_subscriber() {
    std::lock_guard lock(mu_);
    subscribed_ = false;
}

} // namespace sequencer
