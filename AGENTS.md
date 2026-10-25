# Repository instructions

## Requirements
When requirements are mentioned, locate them in `docs/reqs.md`. Only requirements located in the `Active` section are to be taken fully into consideration. The ones from `Pending review` can be consulted, but not assumed final, while the ones from `Rejected` and `Superseded` can only be consulted to understand context and what is not required. 

Do not load the file for tasks that do not mandate it. 

## Grill-me sessions

Past grill-me sessions are archived outside this repo: from the repo root, go one folder back, then into `ai/trdb/grillme/`. They hold the reasoning behind decisions, the options that were rejected and what was left open.

Do not load them by default. Consult them only when you need the reasoning or rejected options behind a decision, or before a grill-me session on a related topic. Read the `README.md` index there first, then open only the matching sessions, and within a session only the relevant area or its `Outcome` section. The decision log, the implementation notes and the current docs take precedence over a session.

## Documentation

When a task involves writing, editing or reviewing technical documentation under `docs/technical_docs/`, or updating the documentation instructions, read and follow [Documentation instructions](.agents/rules/documentation.md) before starting that work.

Do not load that file for tasks that do not involve technical documentation or its instructions.

## Project overview

TRDB is a relational database engine written in Rust, rewriting an earlier Java learning project. It is under active development; check the implementation before assuming a complete SQL engine or client protocol exists.

## Repository layout

- `crates/storage`: Storage manager, buffer cache and guards, file management, and slotted data pages. Pages are 4096 bytes. Page code separates `layout`, `metadata`, and `behavior`.
- `docs/README.md`: Documentation index. `docs/roadmap.md`, `docs/decision log.md`, and `docs/implementation notes.md` provide project context.

Keep changes within the relevant crate or module. Shared types and binary utilities belong in `shared`; storage internals belong in `storage`; row interpretation belongs in `engine`.

## Development and validation

Run commands from the workspace root.

- Lint as CI does: `cargo clippy --all-targets --all-features -- -D warnings`
- Coverage, with `cargo-llvm-cov` installed: `cargo llvm-cov --summary-only`
- Build documentation as the publishing workflow does: `cargo doc --no-deps --release --workspace --all-features --document-private-items`

For code changes, run relevant tests and the formatting and lint checks. Tests generally live beside the implementation in `#[cfg(test)]` modules; storage page scenarios also live in `crates/storage/src/page/tests/`. Follow existing test organization and use temporary directories for tests that write database files.

Unsafe code is forbidden. Add Rustdoc for public APIs and preserve existing error-handling conventions using the crate error types and `thiserror`.

## Running locally

Start one server with `cargo run --bin trdb`, then start clients with `cargo run --bin trdbcmd`. The current server listens on `127.0.0.1:8080`.

The server reads `trdb.toml` from the current working directory. Its required `[storage]` settings are `data_dir`, `logs_dir`, and a nonzero `buffer_pages`; the checked-in configuration uses `./DATA`, `./LOGS`, and 100 pages. Logs are application logs, not transactional write-ahead logs. Server startup currently sets up test data; use disposable data directories when exercising it. Keep generated database files, logs, and build artifacts out of changes.

## Coding guidelines
- avoid `static &str`
- Preserve explanatory comments during refactors. When moving, extracting, or inlining code, carry over every explanatory comment that still describes the moved logic, keeping its wording unchanged and placing it beside the relevant code. Delete a comment only if the behavior it describes has changed so the comment is no longer true; update it in that case rather than silently removing it. Before finishing, review the diff specifically for deleted comments and restore any that still apply.