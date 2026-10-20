# Repository instructions

## Requirements
When requirements are mentioned, locate them in `docs/reqs.md`. Only requirements located in the `Active` section are to be taken fully into consideration. The ones from `Pending review` can be consulted, but not assumed final, while the ones from `Rejected` and `Superseded` can only be consulted to understand context and what is not required. 

Do not load the file for tasks that do not mandate it. 

## Documentation

When a task involves writing, editing or reviewing technical documentation under `docs/technical_docs/`, or updating the documentation instructions, read and follow [Documentation instructions](.agents/rules/documentation.md) before starting that work.

Do not load that file for tasks that do not involve technical documentation or its instructions.

## Project overview

TRDB is a relational database engine written in Rust, rewriting an earlier Java learning project. The workspace uses Rust edition 2024 and Cargo resolver 3. It is under active development; check the implementation before assuming a complete SQL engine or client protocol exists.

## Repository layout

- `apps/trdb`: Tokio-based database server, configuration loading, logging, and engine environment setup.
- `apps/trdbcmd`: Tokio-based CLI client for the server.
- `crates/storage`: Storage manager, buffer cache and guards, file management, and slotted data pages. Pages are 4096 bytes. Page code separates `layout`, `metadata`, and `behavior`.
- `crates/engine`: Relational engine code, currently including row encoding and decoding.
- `crates/shared`: Shared schema and row types, data types, and binary helpers.
- `docs/README.md`: Documentation index. `docs/roadmap.md`, `docs/decision log.md`, and `docs/implementation notes.md` provide project context.
- `.github/workflows`: CI linting, test coverage, and Rustdoc publishing.
- `.githooks/pre-commit`: Local validation hook.

Keep changes within the relevant crate or module. Shared types and binary utilities belong in `shared`; storage internals belong in `storage`; row interpretation belongs in `engine`.

## Development and validation

Run commands from the workspace root. CI uses stable Rust with Rustfmt and Clippy.

- Build: `cargo build --workspace`
- Test the workspace: `cargo test --workspace`
- Test a specific crate: `cargo test -p storage` (or `engine`, `shared`, `trdb`, `trdbcmd`)
- Check formatting: `cargo fmt --all -- --check`
- Apply formatting: `cargo fmt --all`
- Lint as CI does: `cargo clippy --all-targets --all-features -- -D warnings`
- Coverage, with `cargo-llvm-cov` installed: `cargo llvm-cov --summary-only`
- Build documentation as the publishing workflow does: `cargo doc --no-deps --release --workspace --all-features --document-private-items`

For code changes, run relevant tests and the formatting and lint checks. Tests generally live beside the implementation in `#[cfg(test)]` modules; storage page scenarios also live in `crates/storage/src/page/tests/`. Follow existing test organization and use temporary directories for tests that write database files.

Follow the workspace lints in `Cargo.toml` and formatting settings in `rustfmt.toml` (100-column width and field initialization shorthand). Unsafe code is forbidden. Add Rustdoc for public APIs and preserve existing error-handling conventions using the crate error types and `thiserror`.

## Running locally

Start one server with `cargo run --bin trdb`, then start clients with `cargo run --bin trdbcmd`. The current server listens on `127.0.0.1:8080`.

The server reads `trdb.toml` from the current working directory. Its required `[storage]` settings are `data_dir`, `logs_dir`, and a nonzero `buffer_pages`; the checked-in configuration uses `./DATA`, `./LOGS`, and 100 pages. Logs are application logs, not transactional write-ahead logs. Server startup currently sets up test data; use disposable data directories when exercising it. Keep generated database files, logs, and build artifacts out of changes.

## Coding guidelines
- avoid `static &str`
- do not touch explanatory comments throughout the body of methods, unless the implementation no longer matches the comment, in which case the comment should be updated. never removed