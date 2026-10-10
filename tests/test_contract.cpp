// Contract tests: a real sequencer on a real port, driven by a real gRPC client,
// exactly the way the gateway (Submit) and engine_node (Subscribe) will use it.
//
// They only look at what crosses the wire, so they must keep passing, unchanged,
// when the in-memory relay is replaced by the journaled sequencer. Tests tagged
// [relay-only] are the exception: they pin down a limit of the relay and will be
// replaced when the journal adds history.

#include "sequencer/service.hpp"

#include <catch2/catch_test_macros.hpp>
#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
namespace pb = exchange::v1;
using sequencer::SequencerConfig;
using sequencer::SequencerService;

namespace {

constexpr std::uint32_t MOOG = 1;
constexpr std::uint32_t BANANA = 2;
constexpr std::uint32_t TESLO = 3;
constexpr std::uint32_t MACROHARD = 4;

// Every call gets a deadline, so a broken server fails a test instead of hanging it.
constexpr auto kCallDeadline = 10s;

SequencerConfig test_config() {
    SequencerConfig c;
    c.symbol_to_partition = {{MOOG, 0}, {BANANA, 0}, {TESLO, 1}, {MACROHARD, 1}};
    c.partition_count = 2;
    c.queue_capacity = 10'000;
    c.poll_interval = 10ms; // short, so shutdown and hang-ups are noticed quickly
    return c;
}

// A clock the test controls. Declare it before the TestServer that uses it.
struct FakeClock {
    std::atomic<std::int64_t> now{1'000};
    sequencer::Clock fn() {
        return [this] { return now.load(); };
    }
};

// The service on a port picked by the OS ("127.0.0.1:0"), plus a client stub pointed at it.
class TestServer {
  public:
    TestServer(SequencerConfig config, sequencer::Clock clock) : service_(std::move(config), std::move(clock)) {
        int port = 0;
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        if (!server_ || port == 0)
            throw std::runtime_error("test server failed to start");
        stub_ = pb::Sequencer::NewStub(
            grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials()));
    }
    ~TestServer() {
        service_.stop();
        server_->Shutdown(std::chrono::system_clock::now() + 1s);
        server_->Wait();
    }
    TestServer(const TestServer &) = delete;
    TestServer &operator=(const TestServer &) = delete;

    pb::Sequencer::Stub &stub() { return *stub_; }
    SequencerService &service() { return service_; }

  private:
    SequencerService service_; // declared first: destroyed last, after the server stops using it
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<pb::Sequencer::Stub> stub_;
};

// One gateway connection: one long-lived Submit stream.
class Gateway {
  public:
    Gateway(pb::Sequencer::Stub &stub, std::string id) : id_(std::move(id)) {
        ctx_.set_deadline(std::chrono::system_clock::now() + kCallDeadline);
        stream_ = stub.Submit(&ctx_);
    }
    ~Gateway() {
        if (!finished_) {
            ctx_.TryCancel();
            stream_->Finish();
        }
    }
    Gateway(const Gateway &) = delete;
    Gateway &operator=(const Gateway &) = delete;

    // No Catch2 assertions inside, so worker threads may call it (Catch2 asserts are not thread-safe).
    std::optional<pb::SubmitAck> try_submit(std::uint64_t client_request_id, std::uint32_t symbol,
                                            const std::string &payload = "body", std::uint64_t account_id = 17) {
        pb::SubmitRequest req;
        req.set_gateway_id(id_);
        req.set_client_request_id(client_request_id);
        req.set_symbol(symbol);
        req.set_payload(payload);
        req.set_account_id(account_id);
        if (!stream_->Write(req))
            return std::nullopt;
        pb::SubmitAck ack;
        if (!stream_->Read(&ack))
            return std::nullopt;
        return ack;
    }

    pb::SubmitAck submit(std::uint64_t client_request_id, std::uint32_t symbol, const std::string &payload = "body",
                         std::uint64_t account_id = 17) {
        auto ack = try_submit(client_request_id, symbol, payload, account_id);
        REQUIRE(ack.has_value());
        return *ack;
    }

    // Closes our side, then returns how the server ended the stream.
    grpc::Status finish() {
        stream_->WritesDone();
        pb::SubmitAck ignored;
        while (stream_->Read(&ignored)) {
        }
        finished_ = true;
        return stream_->Finish();
    }

  private:
    std::string id_;
    grpc::ClientContext ctx_; // must outlive stream_, so it is declared before it
    std::unique_ptr<grpc::ClientReaderWriter<pb::SubmitRequest, pb::SubmitAck>> stream_;
    bool finished_ = false;
};

// One engine connection: one Subscribe stream for one partition.
class Engine {
  public:
    Engine(pb::Sequencer::Stub &stub, std::uint32_t partition, std::uint64_t from_seq = 0) {
        ctx_.set_deadline(std::chrono::system_clock::now() + kCallDeadline);
        pb::SubscribeRequest req;
        req.set_partition(partition);
        req.set_from_seq(from_seq);
        reader_ = stub.Subscribe(&ctx_, req);
    }
    ~Engine() {
        if (!finished_)
            hang_up();
    }
    Engine(const Engine &) = delete;
    Engine &operator=(const Engine &) = delete;

    // No Catch2 assertions inside, so worker threads may call it.
    std::optional<pb::SequencedCommand> try_next() {
        pb::SequencedCommand command;
        if (!reader_->Read(&command))
            return std::nullopt;
        return command;
    }

    std::vector<pb::SequencedCommand> take(std::size_t n) {
        std::vector<pb::SequencedCommand> out;
        while (out.size() < n) {
            auto command = try_next();
            REQUIRE(command.has_value());
            out.push_back(*command);
        }
        return out;
    }

    // Waits for the server to end the stream and returns why.
    // Only call it when the server is expected to end the stream.
    grpc::Status finish() {
        pb::SequencedCommand ignored;
        while (reader_->Read(&ignored)) {
        }
        finished_ = true;
        return reader_->Finish();
    }

    // The engine disconnects, as if it crashed.
    void hang_up() {
        ctx_.TryCancel();
        finish();
    }

  private:
    grpc::ClientContext ctx_;
    std::unique_ptr<grpc::ClientReader<pb::SequencedCommand>> reader_;
    bool finished_ = false;
};

// How the server answers a Subscribe that it rejects straight away.
grpc::Status subscribe_rejection(pb::Sequencer::Stub &stub, std::uint32_t partition, std::uint64_t from_seq) {
    Engine engine(stub, partition, from_seq);
    return engine.finish();
}

// After a subscriber disconnects, the server frees its seat on the next poll, not instantly.
// Probe with an impossible from_seq: ALREADY_EXISTS = still taken, OUT_OF_RANGE = free.
void wait_until_seat_free(pb::Sequencer::Stub &stub, std::uint32_t partition) {
    for (int attempt = 0; attempt < 200; ++attempt) {
        const auto status = subscribe_rejection(stub, partition, std::numeric_limits<std::uint64_t>::max());
        if (status.error_code() == grpc::StatusCode::OUT_OF_RANGE)
            return;
        REQUIRE(status.error_code() == grpc::StatusCode::ALREADY_EXISTS);
        std::this_thread::sleep_for(10ms);
    }
    FAIL("the subscriber seat was never released");
}

} // namespace

// ---------------------------------------------------------------------------------------------
// Submit: what the gateway can rely on
// ---------------------------------------------------------------------------------------------

TEST_CASE("contract: accepted orders get seq 1, 2, 3 and acks come back in request order", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");

    for (std::uint64_t i = 1; i <= 3; ++i) {
        const auto ack = gateway.submit(100 + i, MOOG);
        CHECK(ack.client_request_id() == 100 + i);
        CHECK(ack.account_id() == 17);
        CHECK(ack.status() == pb::SUBMIT_STATUS_ACCEPTED);
        CHECK(ack.seq() == i);
    }
}

TEST_CASE("contract: an unknown symbol is refused, gets no seq, and uses none up", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");

    const auto refused = gateway.submit(1, 99);
    CHECK(refused.client_request_id() == 1);
    CHECK(refused.account_id() == 17);
    CHECK(refused.status() == pb::SUBMIT_STATUS_UNKNOWN_SYMBOL);
    CHECK(refused.seq() == 0);

    const auto accepted = gateway.submit(2, MOOG);
    CHECK(accepted.status() == pb::SUBMIT_STATUS_ACCEPTED);
    CHECK(accepted.seq() == 1);

    Engine engine(server.stub(), 0);
    const auto got = engine.take(1);
    CHECK(got[0].seq() == 1);
    CHECK(got[0].client_request_id() == 2); // the refused request never reached the engine
    CHECK(got[0].account_id() == 17);
}

TEST_CASE("contract: a full partition answers BUSY without using up a seq", "[contract]") {
    auto config = test_config();
    config.queue_capacity = 2;
    FakeClock clock;
    TestServer server(config, clock.fn());
    Gateway gateway(server.stub(), "gw-1");

    CHECK(gateway.submit(1, MOOG).seq() == 1);
    CHECK(gateway.submit(2, MOOG).seq() == 2);
    const auto busy = gateway.submit(3, MOOG);
    CHECK(busy.status() == pb::SUBMIT_STATUS_BUSY);
    CHECK(busy.client_request_id() == 3);
    CHECK(busy.account_id() == 17);
    CHECK(busy.seq() == 0);

    // TESLO lives on the other partition, which still has room.
    CHECK(gateway.submit(4, TESLO).status() == pb::SUBMIT_STATUS_ACCEPTED);

    // Once seq 2 has arrived, seq 1 has been removed from the queue: room for one more.
    Engine engine(server.stub(), 0);
    engine.take(2);
    const auto retry = gateway.submit(5, MOOG);
    CHECK(retry.status() == pb::SUBMIT_STATUS_ACCEPTED);
    CHECK(retry.seq() == 3); // not 4: the BUSY request took no number

    const auto third = engine.take(1);
    CHECK(third[0].seq() == 3);
    CHECK(third[0].client_request_id() == 5);
}

TEST_CASE("contract: after stop(), new orders are answered NOT_RUNNING", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");
    CHECK(gateway.submit(1, MOOG).status() == pb::SUBMIT_STATUS_ACCEPTED);

    server.service().stop();

    const auto ack = gateway.submit(2, MOOG);
    CHECK(ack.client_request_id() == 2);
    CHECK(ack.account_id() == 17);
    CHECK(ack.status() == pb::SUBMIT_STATUS_NOT_RUNNING);
    CHECK(ack.seq() == 0);
}

TEST_CASE("contract: a request without a gateway_id ends the stream with INVALID_ARGUMENT", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway anonymous(server.stub(), "");

    CHECK_FALSE(anonymous.try_submit(1, MOOG).has_value()); // no ack
    CHECK(anonymous.finish().error_code() == grpc::StatusCode::INVALID_ARGUMENT);
}

// ---------------------------------------------------------------------------------------------
// Subscribe: what engine_node can rely on
// ---------------------------------------------------------------------------------------------

TEST_CASE("contract: the engine receives every field exactly as it was sent, with the sequencer's time", "[contract]") {
    FakeClock clock;
    clock.now = 1'696'330'000'123;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-alice");

    const auto first_ack = gateway.submit(41, MOOG, "first", 901);
    CHECK(first_ack.client_request_id() == 41);
    CHECK(first_ack.account_id() == 901);
    clock.now = 1'696'330'000'128;
    const auto second_ack = gateway.submit(42, BANANA, "second", 902);
    CHECK(second_ack.client_request_id() == 42);
    CHECK(second_ack.account_id() == 902);

    Engine engine(server.stub(), 0);
    const auto got = engine.take(2);

    CHECK(got[0].seq() == 1);
    CHECK(got[0].ts() == 1'696'330'000'123);
    CHECK(got[0].symbol() == MOOG);
    CHECK(got[0].client_request_id() == 41);
    CHECK(got[0].gateway_id() == "gw-alice");
    CHECK(got[0].account_id() == 901);
    CHECK(got[0].payload() == "first");

    CHECK(got[1].seq() == 2);
    CHECK(got[1].ts() == 1'696'330'000'128);
    CHECK(got[1].symbol() == BANANA);
    CHECK(got[1].client_request_id() == 42);
    CHECK(got[1].gateway_id() == "gw-alice");
    CHECK(got[1].account_id() == 902);
    CHECK(got[1].payload() == "second");
}

TEST_CASE("contract: the payload is opaque bytes and comes out byte-for-byte unchanged", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");

    // Not a valid CommandBody, with zero bytes and high bytes. The sequencer must not care.
    std::string garbage;
    garbage.push_back('\0');
    garbage += "\xff\x01not a CommandBody";
    garbage.push_back('\0');
    garbage += std::string(1000, '\x7f');

    gateway.submit(1, MOOG, garbage);
    gateway.submit(2, MOOG, "");

    Engine engine(server.stub(), 0);
    const auto got = engine.take(2);
    CHECK(got[0].payload() == garbage);
    CHECK(got[0].payload().size() == garbage.size());
    CHECK(got[1].payload().empty());
}

TEST_CASE("contract: each partition numbers its own commands 1, 2, 3 ...", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");

    CHECK(gateway.submit(1, MOOG).seq() == 1);      // partition 0
    CHECK(gateway.submit(2, TESLO).seq() == 1);     // partition 1
    CHECK(gateway.submit(3, BANANA).seq() == 2);    // partition 0
    CHECK(gateway.submit(4, MACROHARD).seq() == 2); // partition 1
    CHECK(gateway.submit(5, TESLO).seq() == 3);     // partition 1

    Engine first(server.stub(), 0);
    Engine second(server.stub(), 1);
    const auto zero = first.take(2);
    const auto one = second.take(3);

    CHECK(zero[0].seq() == 1);
    CHECK(zero[0].symbol() == MOOG);
    CHECK(zero[1].seq() == 2);
    CHECK(zero[1].symbol() == BANANA);

    CHECK(one[0].seq() == 1);
    CHECK(one[0].symbol() == TESLO);
    CHECK(one[1].seq() == 2);
    CHECK(one[1].symbol() == MACROHARD);
    CHECK(one[2].seq() == 3);
    CHECK(one[2].symbol() == TESLO);
}

TEST_CASE("contract: subscribing to a partition that does not exist is NOT_FOUND", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    CHECK(subscribe_rejection(server.stub(), 2, 0).error_code() == grpc::StatusCode::NOT_FOUND);
    CHECK(subscribe_rejection(server.stub(), 7, 0).error_code() == grpc::StatusCode::NOT_FOUND);
}

TEST_CASE("contract: a second subscriber on the same partition is ALREADY_EXISTS", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");
    gateway.submit(1, MOOG);

    Engine first(server.stub(), 0);
    REQUIRE(first.take(1)[0].seq() == 1); // proves `first` holds the seat

    CHECK(subscribe_rejection(server.stub(), 0, 0).error_code() == grpc::StatusCode::ALREADY_EXISTS);

    // The other partition is unaffected.
    gateway.submit(2, TESLO);
    Engine other(server.stub(), 1);
    CHECK(other.take(1)[0].symbol() == TESLO);
}

TEST_CASE("contract: when an engine disconnects, its seat is freed for the next one", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");
    gateway.submit(1, MOOG);

    {
        Engine first(server.stub(), 0);
        REQUIRE(first.take(1)[0].seq() == 1);
        first.hang_up();
    }
    wait_until_seat_free(server.stub(), 0);

    gateway.submit(2, MOOG);
    Engine second(server.stub(), 0, 2); // resume exactly where the first one stopped
    const auto got = second.take(1);
    CHECK(got[0].seq() == 2);
    CHECK(got[0].client_request_id() == 2);
    CHECK(got[0].account_id() == 17);
}

TEST_CASE("contract: a command whose delivery fails stays queued for the next subscriber", "[contract]") {
    auto config = test_config();
    config.poll_interval = 500ms; // keeps the first subscriber's handler parked inside its wait
    FakeClock clock;
    TestServer server(config, clock.fn());
    Gateway gateway(server.stub(), "gw-1");
    gateway.submit(1, MOOG);

    Engine first(server.stub(), 0);
    REQUIRE(first.take(1)[0].seq() == 1);
    first.hang_up();
    std::this_thread::sleep_for(100ms); // the server learns of the hang-up while the handler is still parked

    // Seq 2 wakes the parked handler. Its Write fails because the call is gone,
    // so seq 2 must stay queued instead of being thrown away.
    CHECK(gateway.submit(2, MOOG, "body", 903).seq() == 2);

    wait_until_seat_free(server.stub(), 0);
    Engine second(server.stub(), 0, 2);
    const auto got = second.take(1);
    CHECK(got[0].seq() == 2);
    CHECK(got[0].client_request_id() == 2);
    CHECK(got[0].account_id() == 903);
}

TEST_CASE("contract: four gateways at once still give one gap-free stream that matches the acks", "[contract]") {
    constexpr int kGateways = 4;
    constexpr std::uint64_t kPerGateway = 500;
    constexpr std::size_t kTotal = kGateways * kPerGateway;

    FakeClock clock;
    TestServer server(test_config(), clock.fn());

    std::vector<pb::SequencedCommand> received; // written only by the engine thread, read after join
    std::jthread engine_thread([&] {
        Engine engine(server.stub(), 0);
        while (received.size() < kTotal) {
            auto command = engine.try_next();
            if (!command)
                return;
            received.push_back(*command);
        }
    });

    std::vector<std::vector<pb::SubmitAck>> acks(kGateways); // one slot per gateway thread
    {
        std::vector<std::jthread> gateways;
        for (int g = 0; g < kGateways; ++g) {
            gateways.emplace_back([&, g] {
                Gateway gateway(server.stub(), "gw-" + std::to_string(g));
                for (std::uint64_t r = 1; r <= kPerGateway; ++r) {
                    auto ack = gateway.try_submit(r, r % 2 ? MOOG : BANANA);
                    if (!ack)
                        return;
                    acks[g].push_back(*ack);
                }
            });
        }
    } // gateway threads join here
    engine_thread.join();

    // From the acks: which (gateway, request) owns each seq. Each gateway's seqs must rise.
    std::map<std::uint64_t, std::pair<std::string, std::uint64_t>> owner;
    std::size_t bad_acks = 0;
    for (int g = 0; g < kGateways; ++g) {
        REQUIRE(acks[g].size() == kPerGateway);
        std::uint64_t last_seq = 0;
        for (std::uint64_t i = 0; i < kPerGateway; ++i) {
            const auto &ack = acks[g][i];
            if (ack.client_request_id() != i + 1 || ack.account_id() != 17 ||
                ack.status() != pb::SUBMIT_STATUS_ACCEPTED || ack.seq() <= last_seq)
                ++bad_acks;
            last_seq = ack.seq();
            owner[ack.seq()] = {"gw-" + std::to_string(g), ack.client_request_id()};
        }
    }
    CHECK(bad_acks == 0);
    REQUIRE(owner.size() == kTotal); // every seq handed out exactly once

    // The engine saw 1..2000 in order, and each seq carries the command its ack promised.
    REQUIRE(received.size() == kTotal);
    std::size_t mismatches = 0;
    for (std::size_t i = 0; i < kTotal; ++i) {
        const auto &command = received[i];
        const auto it = owner.find(command.seq());
        if (command.seq() != i + 1 || it == owner.end() || command.gateway_id() != it->second.first ||
            command.client_request_id() != it->second.second || command.account_id() != 17)
            ++mismatches;
    }
    CHECK(mismatches == 0);
}

TEST_CASE("contract: stop() ends a live subscription with UNAVAILABLE", "[contract]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");
    gateway.submit(1, MOOG);

    Engine engine(server.stub(), 0);
    REQUIRE(engine.take(1)[0].seq() == 1);

    server.service().stop();
    const auto start = std::chrono::steady_clock::now();
    CHECK(engine.finish().error_code() == grpc::StatusCode::UNAVAILABLE);
    CHECK(std::chrono::steady_clock::now() - start < 2s); // noticed within a poll, not left hanging
}

// ---------------------------------------------------------------------------------------------
// Relay-only: replaced when the journal adds history
// ---------------------------------------------------------------------------------------------

TEST_CASE("relay only: with no history, a subscriber can only resume at the next unsent seq",
          "[contract][relay-only]") {
    FakeClock clock;
    TestServer server(test_config(), clock.fn());
    Gateway gateway(server.stub(), "gw-1");
    gateway.submit(1, MOOG);
    gateway.submit(2, MOOG);

    // Skipping ahead: seq 1 has not been delivered yet.
    CHECK(subscribe_rejection(server.stub(), 0, 2).error_code() == grpc::StatusCode::OUT_OF_RANGE);

    {
        Engine engine(server.stub(), 0, 1); // exactly the next unsent seq is fine
        const auto got = engine.take(2);
        CHECK(got[1].seq() == 2);
        engine.hang_up();
    }
    wait_until_seat_free(server.stub(), 0);

    // Asking to replay already-delivered commands: the relay threw them away.
    // The journaled sequencer will replay these from disk instead.
    const auto replay = subscribe_rejection(server.stub(), 0, 1);
    CHECK(replay.error_code() == grpc::StatusCode::OUT_OF_RANGE);
    CHECK(replay.error_message().find("next seq is 3") != std::string::npos);
}
