#include "sequencer/service.hpp"

#include <stdexcept>
#include <string>
#include <utility>

namespace sequencer {

SequencerService::SequencerService(SequencerConfig config, Clock clock) : config_(std::move(config)) {
    if (config_.partition_count == 0)
        throw std::invalid_argument("partition_count must be positive");
    if (config_.poll_interval <= std::chrono::milliseconds::zero())
        throw std::invalid_argument("poll_interval must be positive");
    for (const auto &[symbol, partition] : config_.symbol_to_partition) {
        static_cast<void>(symbol);
        if (partition >= config_.partition_count)
            throw std::invalid_argument("symbol maps to an unknown partition");
    }

    partitions_.reserve(config_.partition_count);
    for (std::uint32_t i = 0; i < config_.partition_count; ++i)
        partitions_.push_back(std::make_unique<Partition>(config_.queue_capacity, clock));
}

grpc::Status SequencerService::Submit(grpc::ServerContext *,
                                      grpc::ServerReaderWriter<pb::SubmitAck, pb::SubmitRequest> *stream) {
    pb::SubmitRequest req;
    while (stream->Read(&req)) {
        if (req.gateway_id().empty())
            return {grpc::StatusCode::INVALID_ARGUMENT, "gateway_id is required"};

        pb::SubmitAck ack;
        ack.set_request_id(req.request_id());
        if (!running_) {
            ack.set_status(pb::SUBMIT_STATUS_NOT_RUNNING);
        } else {
            const auto symbol = config_.symbol_to_partition.find(req.symbol());
            if (symbol == config_.symbol_to_partition.end()) {
                ack.set_status(pb::SUBMIT_STATUS_UNKNOWN_SYMBOL);
            } else {
                const AppendResult result = partitions_[symbol->second]->append(req);
                if (result.status == AppendStatus::Busy) {
                    ack.set_status(pb::SUBMIT_STATUS_BUSY);
                } else {
                    ack.set_status(pb::SUBMIT_STATUS_ACCEPTED);
                    ack.set_seq(result.seq);
                }
            }
        }
        if (!stream->Write(ack))
            break;
    }
    return grpc::Status::OK;
}

grpc::Status SequencerService::Subscribe(grpc::ServerContext *ctx, const pb::SubscribeRequest *req,
                                         grpc::ServerWriter<pb::SequencedCommand> *writer) {
    if (req->partition() >= config_.partition_count)
        return {grpc::StatusCode::NOT_FOUND, "unknown partition"};

    Partition &partition = *partitions_[req->partition()];
    if (!partition.try_claim_subscriber())
        return {grpc::StatusCode::ALREADY_EXISTS, "partition already has a subscriber"};

    // A partition has one live consumer so successful writes preserve queue order.
    struct SubscriberGuard {
        Partition &partition;
        ~SubscriberGuard() { partition.release_subscriber(); }
    } guard{partition};

    const std::uint64_t from = req->from_seq();
    if (from != 0 && from != partition.next_unsent()) {
        return {grpc::StatusCode::OUT_OF_RANGE,
                "relay keeps no history; next seq is " + std::to_string(partition.next_unsent())};
    }

    while (running_ && !ctx->IsCancelled()) {
        // Timed waits let shutdown and cancellation be observed without a wake-up channel.
        const auto command = partition.wait_front(config_.poll_interval);
        if (!command)
            continue;
        if (!writer->Write(*command))
            break;
        // Keep an item until its synchronous gRPC write succeeds.
        partition.pop_front();
    }

    if (running_)
        return grpc::Status::OK;
    return {grpc::StatusCode::UNAVAILABLE, "sequencer shutting down"};
}

void SequencerService::stop() { running_ = false; }

} // namespace sequencer
