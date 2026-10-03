# Sequencer

A synchronous gRPC relay that assigns per-partition sequence numbers to gateway commands and streams them to the engine. The wire API is final; durable journaling, replay, and business logic are intentionally deferred.

## Build

From `trading-project/`:

```sh
cmake -S sequencer -B sequencer/build
cmake --build sequencer/build -j2
```

## Run

```sh
./sequencer/build/sequencer [address]
```

The default address is `0.0.0.0:50051`. Send `SIGINT` or `SIGTERM` to shut it down cleanly.

## Tests

```sh
cmake -S sequencer -B sequencer/build
cmake --build sequencer/build -j2
ctest --test-dir sequencer/build
```

- `tests/test_contract.cpp` — a real server on a free port, driven by a real gRPC client, the way the
  gateway and engine_node use it. These must keep passing unchanged when the relay becomes the journaled
  sequencer. Tests tagged `[relay-only]` are the exception and will be replaced.
- `tests/test_partition.cpp` — the in-memory lane and startup checks. Rewritten when the journal arrives.

ThreadSanitizer: run only the Partition/service tests (`sequencer_tests "[partition],[service]"`). With
system gRPC packages, which are not built with TSan, the contract tests produce false reports from inside
gRPC and protobuf.
