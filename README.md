# SmolDB

[![Language](https://img.shields.io/badge/Language-C%2B%2B20-blue.svg)](https://isocpp.org/)
[![Build](https://img.shields.io/badge/Build-CMake-green.svg)](https://cmake.org/)
[![License](https://img.shields.io/badge/License-GNU_GPL-yellow.svg)](LICENSE)

A C++20 Mini-RDBMS for High-Concurrency Workloads.

## About The Project

SmolDB is a from-scratch relational database management system built in modern C++. It is not intended to be a general-purpose database like PostgreSQL. Instead, it is an exploration into database internals, designed for a very specific niche: OLTP workloads characterized by high concurrency on small, critical tables, inspired by the demands of core banking systems.

The entire project is architected around a "concurrency-first" philosophy. Every component is designed and battle-tested to function correctly and performantly under heavy, multi-threaded load.

### Core Philosophy & Architecture

SmolDB is built on a few key principles:

*   **ACID Guarantees:** The system provides strict serializability via Strict Two-Phase Locking (S2PL) and ensures durability through a Write-Ahead Log (WAL).
*   **Concurrency First:** Performance under contention is paramount. The system leverages modern C++ concurrency primitives and features a highly concurrent, sharded buffer pool designed to scale with CPU cores.
*   **Layered Design:** The architecture is cleanly separated into a Storage Engine (managing physical pages, WAL, and recovery), an Execution Engine (handling transactions and locking), and an Access Layer (providing logical table and row abstractions).

## Key Features & Current Status

The foundational layers of SmolDB are implemented and hardened.

*   ✅ **Transactional Core:** Full `BEGIN`, `COMMIT`, and `ABORT` lifecycle for transactions, managed by a handle-based `TransactionManager`.
*   ✅ **Row-Level Locking:** A sharded `LockManager` implementing S2PL to prevent data races.
*   ✅ **Concurrency-Hardened Storage Engine:**
    *   A high-throughput, **sharded buffer pool** that minimizes lock contention and is robust against livelocks.
    *   A thread-safe, queue-based **Write-Ahead Log (WAL)** Manager.
    *   A thread-safe `Disk_mgr` for safe concurrent I/O operations.
*   ✅ **Persistence & Recovery:** WAL-based recovery to restore state after a crash and a persistent catalog for table schemas.
*   ✅ **Basic Data Operations:** Transactional `INSERT` and `SELECT` (by RID) are fully supported.
*   ✅ **Comprehensive Testing:** Includes unit tests, deterministic concurrency tests, and a non-deterministic fuzz test to validate correctness under stress.

## Building and Running Tests

SmolDB is built with CMake and requires a C++20 compliant compiler (g++ 13+ recommended).

### Prerequisites

*   CMake (3.16+)
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
    git clone https://github.com/your_username/smoldb.git
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

## Roadmap

With the concurrent foundation now stable, development is focused on expanding the feature set of the execution engine.

*   ➡️ **Implement `UPDATE` and `DELETE` Operations:** Including proper exclusive locking and before/after-image logging.
*   ➡️ **Deadlock Detection:** Move beyond simple timeouts to a waits-for graph-based deadlock detector.
*   ➡️ **Full ARIES Recovery:** Implement the complete ARIES protocol with a Dirty Page Table (DPT) and Active Transaction Table (ATT) for optimized, fast restarts.

## License

Distributed under the GNU GENERAL PUBLIC LICENSE License. See `LICENSE` for more information.
