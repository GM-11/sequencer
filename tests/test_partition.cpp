// Unit tests for one lane (Partition) and for the service's startup checks.
//
// These look at the relay's insides, not at the wire. When the in-memory queue is
// replaced by the journal, this file gets rewritten. test_contract.cpp does not.

#include "sequencer/partition.hpp"
#include "sequencer/service.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
using sequencer::AppendStatus;
using sequencer::Partition;
namespace pb = exchange::v1;

namespace {

sequencer::Clock fixed_clock(std::int64_t now) {
    return [now] { return now; };
}

pb::SubmitRequest request(std::uint64_t request_id, std::uint32_t symbol = 1, std::string gateway = "gw-1",
                          std::string payload = "body") {
    pb::SubmitRequest r;
    r.set_gateway_id(std::move(gateway));
    r.set_request_id(request_id);
    r.set_symbol(symbol);
    r.set_payload(std::move(payload));
    return r;
}

// Takes everything currently queued, front to back.
std::vector<pb::SequencedCommand> drain(Partition &p) {
    std::vector<pb::SequencedCommand> out;
    while (auto command = p.wait_front(0ms)) {
        out.push_back(*command);
        p.pop_front();
    }
    return out;
}

} // namespace

TEST_CASE("partition: seq starts at 1, has no gaps, and every field is copied", "[partition]") {
    Partition p(10, fixed_clock(5000));
    for (std::uint64_t i = 1; i <= 3; ++i) {
        const auto result = p.append(request(40 + i, 2, "gw-7", "payload-" + std::to_string(i)));
        REQUIRE(result.status == AppendStatus::Accepted);
        REQUIRE(result.seq == i);
    }

    const auto got = drain(p);
    REQUIRE(got.size() == 3);
    for (std::size_t i = 0; i < got.size(); ++i) {
        CHECK(got[i].seq() == i + 1);
        CHECK(got[i].ts() == 5000);
        CHECK(got[i].symbol() == 2);
        CHECK(got[i].request_id() == 41 + i);
        CHECK(got[i].gateway_id() == "gw-7");
        CHECK(got[i].payload() == "payload-" + std::to_string(i + 1));
    }
}

TEST_CASE("partition: a full lane answers Busy without using up a seq", "[partition]") {
    Partition p(2, fixed_clock(1));
    REQUIRE(p.append(request(1)).seq == 1);
    REQUIRE(p.append(request(2)).seq == 2);

    const auto busy = p.append(request(3));
    CHECK(busy.status == AppendStatus::Busy);
    CHECK(busy.seq == 0);

    p.pop_front(); // room for one more
    const auto next = p.append(request(4));
    CHECK(next.status == AppendStatus::Accepted);
    CHECK(next.seq == 3); // not 4: the Busy request took no number
}

TEST_CASE("partition: timestamps never go backwards, even when the clock does", "[partition]") {
    const std::vector<std::int64_t> readings{100, 90, 120, 120, 50};
    std::size_t next = 0;
    Partition p(10, [&] { return readings[next++]; });
    for (std::uint64_t i = 1; i <= readings.size(); ++i)
        REQUIRE(p.append(request(i)).status == AppendStatus::Accepted);

    const auto got = drain(p);
    REQUIRE(got.size() == 5);
    CHECK(got[0].ts() == 100);
    CHECK(got[1].ts() == 100); // clock said 90: hold at 100
    CHECK(got[2].ts() == 120);
    CHECK(got[3].ts() == 120);
    CHECK(got[4].ts() == 120); // clock said 50: hold at 120
}

TEST_CASE("partition: wait_front gives up after the timeout when the lane is empty", "[partition]") {
    Partition p(10, fixed_clock(1));
    const auto start = std::chrono::steady_clock::now();
    CHECK_FALSE(p.wait_front(30ms).has_value());
    CHECK(std::chrono::steady_clock::now() - start >= 20ms);
}

TEST_CASE("partition: the front item stays until pop_front removes it", "[partition]") {
    Partition p(10, fixed_clock(1));
    p.append(request(1));
    p.append(request(2));

    const auto first_look = p.wait_front(0ms);
    const auto second_look = p.wait_front(0ms);
    REQUIRE(first_look.has_value());
    REQUIRE(second_look.has_value());
    CHECK(first_look->seq() == 1);
    CHECK(second_look->seq() == 1); // looking twice does not consume it

    p.pop_front();
    const auto after_pop = p.wait_front(0ms);
    REQUIRE(after_pop.has_value());
    CHECK(after_pop->seq() == 2);

    p.pop_front();
    p.pop_front(); // popping an empty lane is a harmless no-op
    CHECK_FALSE(p.wait_front(0ms).has_value());
}

TEST_CASE("partition: next_unsent is the front's seq, or the next ticket when empty", "[partition]") {
    Partition p(10, fixed_clock(1));
    CHECK(p.next_unsent() == 1);
    p.append(request(1));
    p.append(request(2));
    CHECK(p.next_unsent() == 1);
    p.pop_front();
    CHECK(p.next_unsent() == 2);
    p.pop_front();
    CHECK(p.next_unsent() == 3); // empty: the next number to be handed out
}

TEST_CASE("partition: only one subscriber can hold the seat at a time", "[partition]") {
    Partition p(10, fixed_clock(1));
    CHECK(p.try_claim_subscriber());
    CHECK_FALSE(p.try_claim_subscriber());
    p.release_subscriber();
    CHECK(p.try_claim_subscriber());
}

TEST_CASE("partition: wait_front wakes up as soon as another thread appends", "[partition]") {
    Partition p(10, fixed_clock(1));
    std::jthread producer([&] {
        std::this_thread::sleep_for(50ms);
        p.append(request(1));
    });

    const auto start = std::chrono::steady_clock::now();
    const auto command = p.wait_front(5s);
    REQUIRE(command.has_value());
    CHECK(command->seq() == 1);
    CHECK(std::chrono::steady_clock::now() - start < 2s); // woken by the append, not by the 5 s timeout
}

TEST_CASE("partition: four threads appending at once still give 1..N in queue order", "[partition]") {
    constexpr int kThreads = 4;
    constexpr std::uint64_t kPerThread = 1000;
    Partition p(kThreads * kPerThread, fixed_clock(1));

    std::atomic<int> refused{0};
    {
        std::vector<std::jthread> threads;
        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&, t] {
                for (std::uint64_t i = 1; i <= kPerThread; ++i)
                    if (p.append(request(i, 1, "gw-" + std::to_string(t))).status != AppendStatus::Accepted)
                        ++refused;
            });
        }
    } // jthreads join here
    CHECK(refused == 0);

    const auto got = drain(p);
    REQUIRE(got.size() == kThreads * kPerThread);

    std::size_t out_of_order = 0;
    std::map<std::string, std::uint64_t> last_request; // per gateway
    for (std::size_t i = 0; i < got.size(); ++i) {
        if (got[i].seq() != i + 1)
            ++out_of_order;
        auto &last = last_request[got[i].gateway_id()];
        if (got[i].request_id() <= last)
            ++out_of_order; // one thread's requests must keep their own order
        last = got[i].request_id();
    }
    CHECK(out_of_order == 0);
}

TEST_CASE("partition: bad settings are rejected at construction", "[partition]") {
    CHECK_THROWS_AS(Partition(0, fixed_clock(1)), std::invalid_argument);
    CHECK_THROWS_AS(Partition(10, sequencer::Clock{}), std::invalid_argument);
}

TEST_CASE("service: a bad config is rejected at startup, not on the first order", "[service]") {
    sequencer::SequencerConfig ok;
    ok.symbol_to_partition = {{1, 0}, {2, 1}};
    ok.partition_count = 2;
    CHECK_NOTHROW(sequencer::SequencerService(ok, fixed_clock(1)));

    auto no_partitions = ok;
    no_partitions.partition_count = 0;
    no_partitions.symbol_to_partition.clear();
    CHECK_THROWS_AS(sequencer::SequencerService(no_partitions, fixed_clock(1)), std::invalid_argument);

    auto route_to_nowhere = ok;
    route_to_nowhere.symbol_to_partition[3] = 2; // only partitions 0 and 1 exist
    CHECK_THROWS_AS(sequencer::SequencerService(route_to_nowhere, fixed_clock(1)), std::invalid_argument);

    auto no_poll = ok;
    no_poll.poll_interval = 0ms;
    CHECK_THROWS_AS(sequencer::SequencerService(no_poll, fixed_clock(1)), std::invalid_argument);

    auto no_room = ok;
    no_room.queue_capacity = 0;
    CHECK_THROWS_AS(sequencer::SequencerService(no_room, fixed_clock(1)), std::invalid_argument);

    CHECK_THROWS_AS(sequencer::SequencerService(ok, sequencer::Clock{}), std::invalid_argument);
}
