# SmolDB

[![Language](https://img.shields.io/badge/Language-C%2B%2B20-blue.svg)](https://isocpp.org/)
[![Build](https://img.shields.io/badge/Build-CMake-green.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/License-GNU_GPL-yellow.svg)](LICENSE)

A C++20 Mini-RDBMS for High-Concurrency Workloads.

## About The Project

SmolDB is a from-scratch relational database management system built in modern C++. It is not intended to be a general-purpose database like PostgreSQL. Instead, it is designed for a very specific niche: OLTP workloads characterized by high concurrency on small, critical tables, inspired by the demands of core banking systems.

The entire project is architected around a "concurrency-first" philosophy. Every component is designed and battle-tested to function correctly and performantly under heavy, multi-threaded load.

### Core Philosophy & Architecture

SmolDB is built on a few key principles:

*   **ACID Guarantees:** The system provides strict serializability via Strict Two-Phase Locking (S2PL) and ensures durability through a Write-Ahead Log (WAL).
*   **Concurrency First:** Performance under contention is paramount. The system leverages modern C++ concurrency primitives and features a highly concurrent, sharded buffer pool designed to scale with CPU cores.
*   **Layered Design:** The architecture is cleanly separated into a Storage Engine (managing physical pages, WAL, and recovery), an Execution Engine (handling transactions and locking), and an Access Layer (providing logical table and row abstractions).
*   **Test-Driven Durability:** A durability guarantee is worthless if it cannot be verified. Instead of relying on flaky, non-deterministic methods like `kill -9` to test crash recovery, I implemented a **test-only injectable crash-point mechanism**. A `RecoveryCrashPoint` enum allows tests to instruct the `RecoveryManager` to throw an exception at a precise moment, deterministically simulating a power failure. This allows us to write fast, reliable, and exhaustive tests for the most critical ARIES invariant: that it can safely recover from a crash that occurs during a previous recovery attempt.

## Key Features & Current Status

The foundational layers of SmolDB are implemented and hardened.
SmolDB implements a complete transactional engine capable of guaranteeing the ACID properties.

*   **Transactional Core (ACID)**
    *   Full `BEGIN`, `COMMIT`, and `ABORT` lifecycle for transactions.
    *   Guarantees Atomicity, Consistency, Isolation, and Durability.

*   **Full DML Support**
    *   Transactional `INSERT`, `UPDATE`, and `DELETE` operations on rows.

*   **Concurrency Control**
    *   **Strict Two-Phase Locking (S2PL):** Row-level locking for serializable isolation.
    *   **High-Concurrency Lock Manager:** A sharded, blocking lock manager designed to minimize contention.

*   **Crash Recovery & Durability**
    *   **ARIES Protocol:** A full, industrial-strength implementation of the ARIES recovery algorithm (Analysis, Redo, Undo).
    *   **Write-Ahead Log (WAL):** A high-throughput, asynchronous WAL ensures all changes are durable before being applied to data pages.
    *   **Compensation Log Records (CLRs):** Guarantees that recovery is itself recoverable, ensuring atomicity even if a crash occurs during a rollback.
    *   **Durable Counters:** Critical metadata like `next_transaction_id` and `next_lsn` are persisted on a reserved header page in the database file to survive crashes.

*   **Storage Engine**
    *   **Heap File Organization:** Simple slotted-page layout for storing variable-length tuples.
    *   **Sharded Buffer Pool:** A high-concurrency buffer manager that shards pages across multiple LRU lists to eliminate a central bottleneck, prioritizing throughput.
    *   **Physical Page Latching:** Enforces thread safety at the memory level using a `std::shared_mutex` per page frame, managed by RAII `PageReader`/`PageWriter` accessors for clean, safe, and encapsulated access.

*   **Robust Testing**
    *   **Unit Tests (GoogleTest):** Deterministic tests for individual components.
    *   **Concurrency Fuzz Tests:** Chaotic, multi-threaded tests designed to expose hard-to-find race conditions and atomicity violations.
    *   **ARIES Crash Simulation Suite:** A deterministic test suite that systematically simulates power failures at every critical stage of the recovery process to validate its correctness and resilience.

## Building and Running Tests

SmolDB is built with CMake and requires a C++20 compliant compiler (g++ 13+ recommended).

### Prerequisites

*   CMake (3.28+)
*   g++ (13+) or another C++20 compiler
*   Boost (Serialization, Variant)
*   GoogleTest (for running tests)

**On Debian/Ubuntu:**
```bash
sudo apt-get update
sudo apt-get install build-essential cmake libboost-all-dev libgtest-dev
```

### Build Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/KamiliArsyad/smoldb.git
    cd smoldb
    ```

2.  **Configure with CMake:**
    ```bash
    cmake -B build
    ```

3.  **Build the project:**
    ```bash
    cmake --build build
    ```

4.  **Run the tests:**
    ```bash
    cd build
    ctest --verbose
    ```

#### Running with ThreadSanitizer (TSan)
The test suite is designed to be run with `-fsanitize=thread` to detect data races. On some Linux distributions, Address Space Layout Randomization (ASLR) can conflict with TSan's shadow memory. If you encounter a `FATAL: ThreadSanitizer: unexpected memory mapping` error, it is an environment issue, not a code bug.

You can resolve this for your local development session with the following command:
```bash
sudo sysctl -w vm.mmap_rnd_bits=30
```

## License

Distributed under the GNU GENERAL PUBLIC LICENSE License. See `LICENSE` for more information.
