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
        // A resend answers with the original seq and is not appended again.
        // Checked before the capacity test: a duplicate needs no room. The
        // lookup, the append and the insert below all happen under this one
        // lock, or two concurrent copies would both be appended.
        // Cancels/modifies send an empty client_order_id and are never deduped.
        if (!req.client_order_id().empty()) {
            if (const auto it = seen_.find({req.account_id(), req.client_order_id()}); it != seen_.end())
                return {AppendStatus::Accepted, it->second};
        }
        if (queue_.size() >= capacity_)
            return {AppendStatus::Busy, 0}; // leaves no entry: a retry is a fresh submit

        // A wall clock can move backward; timestamps must not as sequences increase.
        const std::int64_t ts = std::max(clock_(), last_ts_);
        last_ts_ = ts;
        seq = next_seq_++;

        pb::SequencedCommand command;
        command.set_seq(seq);
        command.set_ts(ts);
        command.set_symbol(req.symbol());
        command.set_client_order_id(req.client_order_id());
        command.set_gateway_id(req.gateway_id());
        command.set_account_id(req.account_id());
        command.set_payload(req.payload());
        queue_.push_back(std::move(command));
        if (!req.client_order_id().empty())
            seen_.emplace(std::make_pair(req.account_id(), req.client_order_id()), seq);
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
