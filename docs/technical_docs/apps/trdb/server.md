---
created: 2026-10-10
---

The server combines asynchronous network I/O with synchronous request work. An async task yields while waiting for a socket, allowing Tokio to run other tasks. A blocking worker runs synchronous work on a separate thread pool, so storage calls do not occupy the executor threads handling network I/O.

We keep one reader and writer per connection, hand request work to `spawn_blocking()` and bound it with a shared semaphore. A semaphore supplies a fixed number of permits; a request holds one while its worker runs.

## Accepting connections

After loading configuration and setting up services, we bind to `127.0.0.1:8080`. A bind failure panics. A `JoinSet` tracks the connection tasks so we can wait for them during shutdown. The shared cancellation token signals that they should stop accepting new work.

1. The accept loop waits for a connection or cancellation using `select!`.
2. On a connection, we clone the environment, semaphore and cancellation token, then start a handler in the connection `JoinSet`.
3. On an accept error, we log it and try again unless shutdown has started.
4. On cancellation, we leave the loop and wait for the connection tasks to finish.

## Handling a connection

We split the socket into a reader and writer. The writer runs in its own task and receives byte vectors through an unbounded channel, a queue connecting result producers to one consumer. It uses `write_all()` so writes from that connection go through one place. The implementation notes describe this split as preparation for streaming results; streaming is not implemented yet.

The reader handles repeated requests on the same connection:

1. We wait for a request, selecting against cancellation.
2. We acquire an owned permit from the shared semaphore, also selecting against cancellation.
3. We call `spawn_blocking()` for the request work and await its result.
4. We send the result bytes to the writer, release the permit and go back to reading.

The semaphore has eight permits. The reader holds its permit while awaiting the worker, so at most eight request workers can run across all connections. One connection's reader waits for its worker before reading the next request.

The current request handler is a temporary storage demonstration. SQL execution and a database wire protocol are not yet implemented.

## Shutdown

On Unix, SIGINT or SIGTERM cancels the shared token. On other platforms, the server waits for Ctrl+C.

When a normal read fails or the client disconnects, the handler drops its channel sender and awaits the writer. The writer drains queued messages and exits when the channel closes. A write error also stops the writer; later attempts to send results fail and are logged.

Cancellation while reading or acquiring a permit returns from the handler and drops its sender. The writer drains queued messages and exits when the channel closes.

We await an already-started blocking worker before the handler finishes its current request. After the accept loop exits, `main` drains the connection `JoinSet`.

## Logging

Logs go to the console for reading during a run and to JSON files for structured records. A nonblocking file writer queues records for a logging worker; the server holds its guard until shutdown so that worker stays alive.

`init_logging(logs_dir)` installs two outputs:

- Compact console logs to stdout, with levels and without targets.
- JSON logs through a nonblocking writer to a daily rolling `trdb.log` file, including the current span and span list.

The filter reads `RUST_LOG`, uses INFO by default and parses directives with `from_env_lossy()`.

The returned `WorkerGuard` keeps the file logging worker alive and drains it when dropped. `main` retains the returned `Result<WorkerGuard, ...>` until the connection tasks finish, then drops it during shutdown.

## See also

- [Configuration and service ownership](runtime.md)
