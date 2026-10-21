---
created: 2026-02-14
---

The buffer pool is a fixed collection of memory frames used to cache pages from data files. A page is the stored byte array; a frame is its place in the cache, including the metadata used to coordinate access. Callers share cached pages instead of each owning a separate copy.

The buffer owns the pages and lends access through guards. A guard holds a latch, a lock protecting the page bytes while we use them. Shared read latches allow several readers; an exclusive write latch allows one writer. The page stays in its frame when the guard is dropped.

## Frames and page map

`BufferManager` allocates a `Vec<BufferFrame>` once, then keeps its length fixed. Frames are stored by value. Each frame contains:

- `page: RwLock<Page>`, which owns the boxed 4096-byte page data.
- `page_id: RwLock<Option<PageId>>`, where `None` means the frame is available.
- `pin_count: AtomicU32` and `dirty: AtomicBool`, reserved for buffer management that is not implemented yet.

Pinning means keeping a cached page resident while it is needed. A dirty page has in-memory changes that need writing to disk. Those fields reserve the metadata for these concepts; the current access flow uses page latches and explicit writes.

`FrameId` is the frame's index in the vector. Since the vector does not grow, we can keep using that index while changing the frame's contents.

`page_map` maps a stored page ID (database, file and page number) to its cache entry. It is an `RwLock<HashMap<PageId, Arc<PageEntry>>>`. Each entry has a state mutex and a condition variable. The mutex protects `PageState`; the condition variable lets callers sleep until a loader signals a state change. The state is either `Loading` or `Ready(FrameId)`, so an entry can exist while disk I/O is still in progress.

![Buffer manager](../../../attachments/buffer%20manager%20in%20trdb%202026-02-14%2016.56.52.excalidraw.png)

## Reading a page

Finding the page in the map is a cache hit; having to load it is a cache miss. The entry coordinates callers requesting the same page, while the frame latch controls access to its bytes.

`read_page()` returns a shared guard; `read_page_mut()` returns an exclusive one. Both use the same lookup and load flow:

1. We look up the page ID and clone its entry. If it exists, we wait for `Ready`, acquire the requested page latch and return the guard.
2. On a miss, we take the map write latch and check again. If another caller inserted the entry, we wait on that entry instead.
3. Otherwise, we insert a `Loading` entry and claim a free frame.
4. We read the file directly into the frame's page bytes under its write latch, then set the page's internal ID.
5. We acquire the guard we will return, set the entry to `Ready` and notify waiting callers.

The map latch is released before disk I/O. Other page lookups can continue while this page loads. The [synchronization note](race_conditions.md) explains how callers coordinate frame claims and page loading.

### Claiming a frame

We scan the frames and call `try_write()` on each frame's `page_id`. Busy or poisoned identity latches are skipped. If the ID is `None`, we set it to the requested ID, set `pin_count = 1` and `dirty = false`, then return the frame index.

Setting the ID before releasing its latch prevents another caller from claiming the same frame. If no frame is available, we return `BufferFull`.

## Allocating a page

`allocate_new_page(id)` claims a frame without reading disk, inserts a `Ready` entry and returns a write guard. The caller supplies a new page ID and initializes the header and internal identity through that guard before sharing the page. The buffer handles ownership and allocation; the caller handles the contents, as described in the implementation notes.

## Guards

`PageReadGuard` dereferences to `&Page`; `PageWriteGuard` also provides `&mut Page`. The guard borrows the buffer and holds the page latch until dropped.

We release guards once we finish using the page, before requesting another incompatible guard on that page. These latches protect the bytes in memory.

## Writing a page

`write_page(id, guard)` consumes a write guard and passes its full byte array to the file manager. The caller supplies the guarded page's ID as the destination.

Dropping a guard releases its latch. We persist changes with an explicit `write_page()` call, while the page remains cached in its frame.

## See also

- [Storage manager](../storage_manager/storage_manager.md)
- [File manager](../file/file_manager.md)
