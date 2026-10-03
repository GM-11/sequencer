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
