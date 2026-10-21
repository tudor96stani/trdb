---
created: 2026-02-14
---

The file manager translates page requests into file I/O. A `PageId` identifies a database, a file within it and a page number; the catalog maps the `(db_id, file_id)` pair to a filesystem path, and the page number determines the byte offset. Each transfer reads or writes one whole 4096-byte page.

`FileManager` defines that interface, while `DiskFileManager` implements it with cached operating-system file handles and positional I/O. This keeps filesystem details below the buffer's page API.

## Catalog

`FileCatalog` stores an `RwLock<HashMap<(DatabaseId, FileId), PathBuf>>`. File IDs are only unique within a database, so both are part of the key. `add_file()` inserts or replaces a path, and `get_file_name()` returns a cloned path or `None`.

Path construction belongs outside the file manager. `DiskFileManager` opens the path supplied by the catalog, which stores each `PathBuf` as supplied. Relative paths resolve from the process's working directory.

`EngineEnvironment` creates the catalog and shares it with the file manager. `StorageManager` does not register paths.

## Interface

`FileManager::new(Arc<FileCatalog>)` constructs a manager. Its read and write methods return `Result<(), FileManagerError>`:

- `read_page(id, destination)` fills the caller's page buffer.
- `write_page(id, page_data)` writes the supplied page buffer.

Both expect a caller-provided buffer of exactly 4096 bytes.

## Opening a file

A file handle is the operating-system resource returned by opening a file. `DiskFileManager` caches shared handles in `RwLock<HashMap<(DatabaseId, FileId), Arc<File>>>`, so subsequent page requests reuse the opened file.

1. We check for a cached handle under the map's read latch.
2. On a miss, we take the write latch and check again in case another caller opened it.
3. We ask the catalog for the path of `(db_id, file_id)`. A missing mapping returns `FilePathNotFound`.
4. We create the parent directories and open the file with read, write and create enabled, without truncation.
5. We cache the handle and return its `Arc`.

Existing content is retained, and a read can create a missing file. Directory-creation failure panics; open failures return `FileManagerError::Io`.

## Reading a page

The byte offset in the file is `page_number * 4096`. Page `(1, 1, 2)` (database 1, file 1, page number 2) starts at byte 8192 of the file registered as ID 1 in database 1.

1. We obtain the file handle.
2. We perform one positional read into the destination slice.
3. If the reported count is 4096, we return success. Otherwise, we return `DidNotReadAllBytes` with the page ID and count.

Positional I/O takes the byte offset with each operation rather than advancing a shared file cursor. We use Unix `read_at()` or Windows `seek_read()`, so concurrent page requests can specify their own positions on the cached handle. The returned byte count determines whether the full page was read.

## Writing a page

We use the same page offset and Unix `write_at()` or Windows `seek_write()`:

1. We obtain the file handle and start with a written count of zero.
2. We write the remaining slice at the page offset plus that count.
3. A zero-byte write returns `WroteZeroBytes`. Otherwise, we advance the count and repeat until at least 4096 bytes have been written.
4. A final count other than 4096 returns `DidNotWriteAllBytes`.

I/O failures return `FileManagerError::Io`.

## See also

- [Buffer manager](../buffer/buffer_manager.md)
- [Service ownership](../../apps/trdb/runtime.md)
