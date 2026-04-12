# TRDB - Relational Database Engine

This is a learning project implementing a relational database engine in Rust, rewritten from a Java original. The project implements a page-based storage engine with support for heap and B+ tree structures, buffer management, and eventually a full SQL query engine with transactions.

## Build, Test, and Lint

```bash
# Build the entire workspace
cargo build

# Run all tests
cargo test

# Run a specific test by name
cargo test <test_name>

# Run tests for a specific crate
cargo test -p page
cargo test -p buffer
cargo test -p storage-api

# Format code (required before commit)
cargo fmt --all

# Lint with clippy (required before commit)
cargo clippy --all-targets --all-features -- -D warnings

# Generate and view documentation
cargo doc --open

# Generate coverage report
cargo llvm-cov --summary-only
```

### Git Hooks

Enable pre-commit hooks (rustfmt, clippy, tests) with:
```bash
git config core.hooksPath .githooks
```

## Architecture

### Workspace Structure

This is a Cargo workspace with a layered architecture. Dependencies flow upward—lower layers never depend on higher layers:

```
trdb/                    # Main binary (top layer)
└── crates/storage/
    ├── storage-api/     # Public API layer (depends on: buffer, file, page)
    ├── buffer/          # Buffer pool manager (depends on: file, page)
    ├── file/            # File I/O operations (depends on: page)
    ├── page/            # Page structures and operations (depends on: binary-helpers)
    └── binary-helpers/  # Low-level byte manipulation (no dependencies)
```

**Dependency Rules:**
- `trdb` binary depends on all storage crates
- `storage-api` orchestrates buffer, file, and page layers
- `buffer` manages in-memory page cache and delegates I/O to `file`
- `file` handles disk operations and uses `page` structures
- `page` is the foundational data structure layer
- `binary-helpers` provides primitive serialization utilities

### Core Components

#### Page Layer (`crates/storage/page`)
- Implements **slotted page** format with header and data area
- Supports fixed and variable-length records
- Two page types: `Unsorted` (heap) and `Sorted` (index, not yet implemented)
- Key modules:
  - `page/api.rs`: Main `Page` trait with all operations
  - `page/insert.rs`, `page/delete.rs`, `page/update.rs`: Data modification
  - `page/read_row.rs`: Row retrieval
  - `page/plan_insert.rs`: Determines where new rows fit
  - `header.rs`: Page metadata (slot count, free space tracking)
  - `slot_array.rs`: Maps slot IDs to row offsets/lengths

#### Buffer Layer (`crates/storage/buffer`)
- Fixed-size buffer pool for caching pages in memory
- Thread-safe page access via `PageReadGuard` and `PageWriteGuard` (RAII guards)
- Page loading mechanism with `PageState` enum (Loading/Ready) to coordinate concurrent access
- `BufferManager::read_page()` and `read_page_mut()` are primary entry points
- **Not yet implemented:** LRU replacement, dirty page tracking, pinning, auto-flushing

#### File Layer (`crates/storage/file`)
- `FileManager` trait abstracts file system operations
- `InMemoryFileManager` is the current implementation (for testing)
- `FileCatalog` tracks which files store which tables/indexes
- Each table/index will eventually be stored in a separate file

#### Storage API Layer (`crates/storage/storage-api`)
- `StorageManager` provides unified interface to storage subsystem
- Wraps `BufferManager` and `FileManager`
- Primary methods: `read_page()`, `write_page()`, `new_page()`

## Key Conventions

### Page IDs
- `PageId` is a `(file_id, page_number)` tuple
- Use `PageId::new(file_id, page_number)` to create

### Page Initialization
Always initialize new pages before use:
```
let mut page = storage_manager.new_page(page_id);
page.initialize(page_id, PageType::Unsorted)?;
```

### Row Insertion Pattern
```
// 1. Plan the insertion to find where row fits
let plan = page.plan_insert(row_size)?;

// 2. Execute insertion with actual data
page.insert_heap(plan, row_data)?;
```
This two-step pattern separates layout logic from data writing.

### Thread-Safe Page Access
Pages are accessed through RAII guards that automatically handle locks:
```
// Read-only access
let page: PageReadGuard = storage_manager.read_page(page_id);
// Guard automatically releases read lock when dropped

// Mutable access
let mut page: PageWriteGuard = storage_manager.write_page(page_id);
// Guard automatically releases write lock when dropped
```

### Error Handling
- Use `thiserror` for error types (workspace dependency)
- Each layer defines its own error enum in `errors.rs`
- Propagate errors with `?` operator—no panics in library code
- The binary (`trdb/src/main.rs`) currently uses `panic!()` during early development, but this will be replaced with proper error handling

### Testing
- Unit tests live in `#[cfg(test)] mod tests` within the same file as the code
- Integration-style tests are in `crates/storage/page/src/tests/` with descriptive names:
  - `insert_heap_tests.rs`
  - `delete_row_tests.rs`
  - `update_row_tests.rs`
  - `plan_insert_tests.rs`
- Test helper methods on `Page` use `test_` prefix (e.g., `Page::test_create_empty_heap()`)

### Linting Configuration
Workspace-wide lints in root `Cargo.toml`:
- Clippy warnings enabled for everything
- `unsafe_code` is **forbidden** (entire codebase must be safe Rust)
- `missing_docs` and `missing_debug_implementations` are warnings
- Code is formatted with `rustfmt` using edition 2024, max width 100

### Documentation
- All public APIs require doc comments (`missing_docs` lint enforced)
- Docs are built and deployed to GitHub Pages via `.github/workflows/docs.yml`
- Use `//!` for module-level docs, `///` for item-level docs

## Development Status

The project is in early stages. Current focus areas:
1. **Completed:** Slotted page format, heap page operations, basic buffer pool
2. **In Progress:** Buffer pool enhancements (LRU, dirty page tracking)
3. **Next:** B+ tree indexes, file-backed storage (replacing in-memory)
4. **Future:** SQL parser, query execution engine, WAL, transactions

See README.md for the full roadmap with detailed checkboxes.
