---
created: 2026-02-14
---

Storage moves fixed-size pages between files, cached memory and callers. A page contains rows, but the managers do not interpret their column values or choose which pages belong to a table.

Row placement belongs to `Page`. Table-wide heap and B+ tree behavior would belong to the relational engine: heaps organize unsorted table rows across pages, while B+ trees organize index keys for lookup. Those parts are not implemented yet.

## Current architecture

`EngineEnvironment` constructs the catalog, file manager, buffer and storage manager, then keeps shared `Arc`s to them for the server's lifetime.

- `StorageManager` provides the page API and delegates its current operations to the buffer.
- `BufferManager` owns frames, loads pages and returns guards over their bytes.
- `DiskFileManager` resolves file IDs through the catalog, caches handles and performs page I/O.
- `FileCatalog` stores file ID to path mappings.

The storage crate groups these components together. The decision log records this as a way to keep the architecture simple for now, without separate API crates for each part.

## Reasoning

The Java implementation put more heap and index behavior in the storage manager. The boundary used here keeps the managers concerned with whole pages, so they do not need to know how a table finds its pages or how an index arranges keys.

The storage manager mostly passes calls through to the buffer. It still holds a file-manager reference because some future page-level operations might need it; none of the current methods use that reference directly.

The environment owns the services because they live for the whole server run. A request only clones the environment's `Arc`, so it uses the existing buffer and file handles rather than constructing new ones.

Eviction, file deletion, WAL and transaction locking are not implemented. The earlier design sketches included some of these responsibilities, but they do not describe current behavior.

## See also

- [Storage manager](storage_manager/storage_manager.md)
- [Buffer manager](buffer/buffer_manager.md)
- [File manager](file/file_manager.md)
- [Service ownership](../apps/trdb/runtime.md)
