#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "exchange/v1/sequencer.grpc.pb.h"
#include "sequencer/partition.hpp"

namespace sequencer {

struct SequencerConfig {
    std::unordered_map<std::uint32_t, std::uint32_t> symbol_to_partition;
    std::uint32_t partition_count = 0;
    std::size_t queue_capacity = 10'000;
    std::chrono::milliseconds poll_interval{100};
};

class SequencerService final : public pb::Sequencer::Service {
  public:
    SequencerService(SequencerConfig config, Clock clock);
    grpc::Status Submit(grpc::ServerContext *ctx,
                        grpc::ServerReaderWriter<pb::SubmitAck, pb::SubmitRequest> *stream) override;
    grpc::Status Subscribe(grpc::ServerContext *ctx, const pb::SubscribeRequest *req,
                           grpc::ServerWriter<pb::SequencedCommand> *writer) override;
    void stop();

  private:
    SequencerConfig config_;
    std::vector<std::unique_ptr<Partition>> partitions_;
    std::atomic<bool> running_{true};
};

} // namespace sequencer
