---
created: 2026-10-10
---

The runtime sets up the services that live for the whole server run. Configuration describes how to construct them; `EngineEnvironment` holds the configuration and the shared service references. Connection handlers use this environment rather than building a separate cache and catalog for each request.

We load `trdb.toml`, validate it and construct services in dependency order. The environment is shared through `Arc`, Rust's reference-counted owner for values used across threads. Cloning an `Arc` shares the same service rather than copying its contents.

## Configuration

We load configuration before setting up logging or starting the listener:

1. `EngineConfig::load_from_file("trdb.toml")` reads the file from the process's working directory.
2. We deserialize its contents into `EngineConfig`.
3. We validate the configuration and return it to `main`.

The file has this structure:

```toml
[storage]
data_dir = "data"
logs_dir = "logs"
buffer_pages = 32
```

`data_dir` and `logs_dir` are `PathBuf`s. `buffer_pages` is a `NonZeroUsize`, so zero fails during deserialization. Configuration validation also requires a nonempty `data_dir`.

Read failures return `ConfigError::Io`; TOML failures return `ParseToml`. Both keep the configuration path and underlying error. Validation returns `Invalid` with a message. On failure, `main` prints the error and exits with status 2.

## Service ownership

`EngineEnvironment` owns the configuration and shared references to the storage services. We construct them in dependency order:

1. Create the `FileCatalog`.
2. Create `DiskFileManager` with the catalog's `Arc`.
3. Create `BufferManager` with the file manager and configured frame count.
4. Create `StorageManager` with the file and buffer managers.

The environment keeps an `Arc` to each service. Handlers and workers clone the environment's `Arc`. This follows the ownership described in the implementation notes: services live for the server run, while each request uses them for its own shorter lifetime.

## See also

- [Server lifecycle](server.md)
- [Storage manager](../../storage/storage_manager/storage_manager.md)
