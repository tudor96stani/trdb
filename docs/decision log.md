---
created: 2026-04-21
---
**A log of various technical decisions made during development. Context either points to the note in [implementation notes](implementation%20notes.md) on which the decision was based (and where the full context is provided), or describes it in-place**

## 260218-1308 add test-only code snippets to cover race condition tests in buffer
**Decision**: add some extra fields and methods on the buffer manager, hidden behind a `#[cfg(test)]`, to allow tests to setup various scenarios to verify race conditions. 
avoid (for now at least) external crates that do concurrent test setups or hiding the real implementation of the buffer behind a test one.
#### Context
- [implementation notes](implementation%20notes.md#260218-1308%20testing,%20cfg%20and%20buffer)
---
## 260417-0640 Reorganize storage crates into a single one
**Decision**: Simplify architecture by doing a single crate/component for **now**. Aggregate existing crates under `crates/storage` (into a single one) and remove unnecessary structs, enums, error types, etc. 
#### Context
Initial choice for project structure was to separate each major component (storage engine, query engine, etc) into separate small crates and define an `*-api` crate (e.g. `storage-api`) that acted as the gateway into that component. This might be too complex and overengineered.

---
## 260214-1852 Keep binaries in `src/apps` and libraries in `crates`
**Decision**: Keep application binaries under `src/apps` and `crates/` for engine.
#### Context
- [260214-1852 small reorg of crates](implementation%20notes.md#260214-1852%20small%20reorg%20of%20crates)

---
## 260213-0633 Use a hybrid async plus worker-thread server model
**Decision**: Use Tokio for connection handling and async network I/O, and execute request/query work on blocking worker threads via a bounded worker pool. Additionally, we will limit concurrent requests to a predefined number using a semaphore in front of the worker pool
#### Context
- [260213-0633 async or threads](implementation%20notes.md#260213-0633%20async%20or%20threads)
