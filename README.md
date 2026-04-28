# TRDB
A relational db engine. 

## High level
Originally implemented this database engine in Java as a learning exercise. The Rust version is a rewrite to explore Rust's capabilities and performance benefits, along with an opportunity to refactor and improve the original design.

## Tech Stack
- Standard `Rust` setup
  - `nextest` can be used for nicer test results, but CI uses `cargo test`
  - `LLVM-cov` for coverage
- GitHub Action for CI - mostly used to validate changes by running `clippy`
  - `clippy` configured to treat all warnings as errors
  - see [workspace Cargo.toml](Cargo.toml) for all workspace lints
  - git pre-commit hook to verify changes (`clippy` + format + test)
  - see `.github/workflows` for details on the pipeline
- Documentation:
  - Rustdoc (`cargo doc`), hosted on [GitHub Pages](https://tudor96stani.github.io/trdb/)
  - `/docs` contains technical docs + implementation notes + decision log + anything else, see [docs index](docs/README.md)

## Usage
There are two binaries:
- `apps/trdb`: the main server binary
- `apps/trdbcmd`: a CLI client binary

Configuration for the server can be found in the [trdb.toml](trdb.toml) file, where you need to specify the relative path for the data directory (where the database files will be stored, defaults to `./DATA`), log directory (where server logs are stored - **not** write-ahead transactional logging; defaults to `./LOGS`) and the size of the buffer cache in number of pages (1 page = 4KB, defaults to 100).

To start the server, simply execute `cargo run --bin trdb` from the root directory. Run a single server instance.
To start a client, execute `cargo run --bin trdbcmd` from the root directory. Multiple client instances can run at the same time.

## Roadmap
Current goal is to reach feature parity with the Java implementation. See [roadmap](docs/roadmap.md) for a list of what is completed & what still needs to be done