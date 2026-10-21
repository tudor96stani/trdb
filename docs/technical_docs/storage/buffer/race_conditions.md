---
created: 2026-02-14
---

Concurrent callers can reach the same cache miss or try to claim the same empty frame. We coordinate those actions so one caller claims each frame and one caller loads each requested page.

There are two boundaries here: assigning a frame's identity and publishing its loaded contents. Identity latches protect the first; page-map states and page latches protect the second. A latch is the lock held while accessing the corresponding in-memory state.

## Claiming a frame

We try to write-lock `frame.page_id`, check that it is `None` and set the requested ID before releasing the latch. The next caller sees the assigned ID and skips that frame.

This protects the claim, not the bytes. A claimed frame can still be waiting for disk I/O, so readers must use the page map rather than scan frame IDs.

## Loading a page

The map holds `Arc<PageEntry>`, so callers share the same loading state. Each entry has a mutex containing `PageState` and a condition variable. Waiting releases the state mutex while the caller sleeps, then reacquires it to check the state again. The state is `Loading` or `Ready(FrameId)`.

1. After a cache miss, we check again under the map's write latch. Only the caller that inserts `Loading` becomes the loader.
2. Other callers clone that entry and wait on its condition variable while the state is `Loading`.
3. The loader claims a frame, writes the disk bytes under the page latch and sets the page's internal ID.
4. It releases the loading latch and obtains the read or write guard it will return.
5. It sets the entry to `Ready` and notifies waiting callers.

We obtain the return guard before setting `Ready`, so a waiter cannot take an incompatible page latch before the loader gets its guard. A map entry can exist before its page is readable; the state is what tells callers when to proceed.

## Errors

If disk I/O fails, the loader resets the frame identity to `None`, removes the page-map entry and returns the file error. If no frame can be claimed, the buffer returns `BufferFull`.

## See also

- [Buffer manager](buffer_manager.md)
