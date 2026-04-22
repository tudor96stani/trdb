---
created: 2026-02-14
---
Initially considered having the buffer manager contain only a collection (like a `Map`) of `Page` values.

## Goals
- buffer is the owner of the pages
- buffer is the only place where they exist in memory (zero copy)
- buffer hands out references to the pages, but not ownership

## Problems with initial approach
- if we have a `Map` or `Vec`, we cannot give references outside of the methods that return them
- we cannot have granular locks very easily (without locking the entire collection)

## New solution
Instead of keeping the `Pages` directly in the buffer, allocate a set of frames in which to store the pages.

The buffer will allocate space for all the frames on the heap. Each page is loaded into the frame. The frame contains a lock on the page, allowing for granular control. This set of frames can be stored as a `Vec`, but each frame is an individual instance pre-allocated on the heap.

Additionally, a separate `Map` will store the mapping `PageID=>FrameID`, where `type FrameID = usize` is just the index of the frame in the vector. Because the frames vector is pre-allocated, we never mutate it, we just change the content of the frames.
![buffer manager in trdb 2026-02-14 16.56.52.excalidraw](buffer%20manager%20in%20trdb%202026-02-14%2016.56.52.excalidraw.png)

## What this achieves (I hope)
- buffer is the owner of the in memory pages
	- buffer frame struct consume the page as it is being loaded
	- when we read from disk, we do it directly into the `frame.page.data` byte array
	- this means that after the startup we rarely (if ever) need to call the constructor of a `Page` again?
- buffer hands out references to pages (not frames)
- these references as represented as guards
	- a guard keeps the lock on the frame page active while in scope
	- it provides access to the locked resource
- users get this guard
	- two types: read and write guard
- multiple read guards are allowed, but only 1 write guard
- while I might have used the word locks here, these are in fact **latches** - physically protect the in memory resource as it is being used. Short lived.
- locks will be handled by the lock manager and protect the logical consistency of the objects
