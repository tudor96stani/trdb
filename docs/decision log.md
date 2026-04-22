---
created: 2026-04-21
---

**A log of various technical decisions made during development. The note IDs might link to another note in [implementation notes](implementation%20notes.md) or elsewhere.**

## 260417-0640 Reorganize storage crates into a single one
Initial choice for project structure was to separate each major component (storage engine, query engine, etc) into separate small crates and define an `*-api` crate (e.g. `storage-api`) that acted as the gateway into that component. This might be too complex and overengineered.

**Decision**: Simplify things by doing a single crate/component **now**. Aggregate existing crates under `crates/storage` into a single one and remove unnecessary structs, enums, error types, etc. 

## 260214-1852 Keep binaries in `src/apps` and libraries in `crates`
**Decision**: Keep application binaries under `src/apps` and `crates/` for engine.

## 260213-0633 Use a hybrid async plus worker-thread server model
The server needs to handle many TCP connections efficiently without forcing each query execution path into a fully async design. So we'll use async tasks for accepting connections and socket I/O, then dispatching actual query work onto blocking worker threads.

**Decision**: Use Tokio for connection handling and async network I/O, and execute request/query work on blocking worker threads via a bounded worker pool. Additionally, we will limit concurrent requests to a predefined number using a semaphore in front of the worker pool
