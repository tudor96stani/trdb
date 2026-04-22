---
created: 2026-02-14
---
This note does not contain any clear decisions or descriptions of the implementation, but more of a brainstorming process for the given topic.

## Thought process
The current storage module is quite onion-y:
![current java vs future trdb storage module 2026-02-14 15.45.04.excalidraw](current%20java%20vs%20future%20trdb%20storage%20module%202026-02-14%2015.45.04.excalidraw.png)

Not sure if the Rust approach should be the same. For the bottom two, it's easier, as they have clear responsibilities:

| Buffer Manager                             | File Manager                                            |
| ------------------------------------------ | ------------------------------------------------------- |
| Cache pages                                | Open & close file channels                              |
| Track pins and dirty flags, last update TS | Read files                                              |
| Evict                                      | Create, delete files                                    |
| Depends on: File Manager                   | Depends on: config containing file names(s) & directory |

The more problematic, to me, is the storage manager. It has several responsibilities, but no actual state. It mostly only depends on the buffer manager to provide pages, **BUT**, lesson learned from Java, for a few actions it requires access to the file manager (check if file exists, for instance). I think for others as well, like table header (if the table header is not a normal page).
So maybe a better diagram:
![current java vs future trdb storage module 2026-02-14 15.50.31.excalidraw](current%20java%20vs%20future%20trdb%20storage%20module%202026-02-14%2015.50.31.excalidraw.png)
Maybe this reframing helps a bit. Storage and buffer should be on almost equal terms, although the first point of view is still better cause it clearly shows the storage manager as an entry point into the whole subsystem.

In terms of its responsibilities, I think the Java version did a bit too much. I don't think it should have the logic about heaps and indexes anymore, but rather those should live in the relational engine (meaning, knowing how the heap is using a directory to keep track of pages, or how the B+tree works). While these use the idea of storage, they do no implement it any more than an index scan or a heap delete does. 
**So the storage manager should be only passthrough logic for the storage system.** Then maybe it does not need a struct? Unless for testing purposes. 
And another rule that was broken in the Java implementation, but should be respected here: **the storage engine managers should never deal with anything more granular than pages - never with rows**.
![current java vs future trdb storage module 2026-02-14 15.57.27.excalidraw](current%20java%20vs%20future%20trdb%20storage%20module%202026-02-14%2015.57.27.excalidraw.png)
