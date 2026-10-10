---
created: 2026-10-10
---

`StorageManager` is the entry point for working with stored pages. Callers request a page by ID and receive guarded access to its bytes; the buffer handles caching and the file manager handles disk I/O. Row placement belongs to `Page`, so the storage manager works at page granularity.

The implementation keeps shared references to the file and buffer managers. Its current methods delegate to the buffer and attach operation-specific storage errors.

## Reading pages

`read_page(id)` returns a `PageReadGuard`; `read_page_mut(id)` returns a `PageWriteGuard`. Both ask the buffer to find or load the page, and wrap failures in `StorageError::ReadPage`.

The guards dereference to `Page` and keep its latch held while we use it. We release them when done so another caller can acquire the page. Their lifetime and caching behavior are described in the [buffer doc](../buffer/buffer_manager.md).

## New pages

`new_page(id)` asks the buffer to claim a frame and return a write guard without reading disk. Failures become `StorageError::NewPage`.

We supply a new ID and call `initialize(id, page_type)` through the returned write guard before sharing the page. The buffer owns the memory, but the caller decides its contents; this boundary is described in the implementation notes.

## Writing pages

`write_page(id, guard)` consumes a write guard and delegates the file write through the buffer. Failures become `StorageError::WritePage`.

The destination comes from the supplied ID and the bytes come from the guard. We pass the guarded page's ID as the destination. Dropping a write guard releases the latch; `write_page()` persists the changes.

## Service ownership

`EngineEnvironment` constructs and shares the services, including the catalog. The storage manager keeps its references; it does not set up catalog paths. See [service ownership](../../apps/trdb/runtime.md).
