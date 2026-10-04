#include "config/instruments.hpp"
#include "sequencer/service.hpp"

#include <chrono>
#include <exception>
#include <iostream>
#include <pthread.h>
#include <signal.h>
#include <string>

int main(int argc, char *argv[]) {
    // Block before gRPC creates threads so sigwait is the sole signal receiver.
    sigset_t set;
    if (sigemptyset(&set) != 0 || sigaddset(&set, SIGINT) != 0 || sigaddset(&set, SIGTERM) != 0)
        return 1;
    if (pthread_sigmask(SIG_BLOCK, &set, nullptr) != 0)
        return 1;

    try {
        // The same instrument list every engine_node reads: one file, no private copies.
        const std::string instruments_path = config::instruments_path();
        const config::InstrumentConfig instruments = config::load_instruments(instruments_path);

        sequencer::SequencerConfig service_config;
        service_config.partition_count = instruments.partitions;
        for (const auto &instrument : instruments.instruments)
            service_config.symbol_to_partition.emplace(instrument.id, instrument.partition);

        const sequencer::Clock clock = [] {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
        };
        const std::string address = argc > 1 ? argv[1] : "0.0.0.0:50051";

        sequencer::SequencerService service(service_config, clock);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
        if (!server) {
            std::cerr << "failed to start sequencer on " << address << '\n';
            return 1;
        }

        std::cout << "sequencer listening on " << address << " (instruments v" << instruments.version << " from "
                  << instruments_path << ")\n";
        for (const auto &instrument : instruments.instruments)
            std::cout << instrument.ticker << "=" << instrument.id << " -> partition " << instrument.partition << '\n';
        std::cout << std::flush;

        int signal_number = 0;
        if (sigwait(&set, &signal_number) != 0) {
            std::cerr << "failed waiting for shutdown signal\n";
            return 1;
        }

        service.stop();
        server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        server->Wait();
        std::cout << "sequencer stopped\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "sequencer setup failed: " << error.what() << '\n';
        return 1;
    }
}
