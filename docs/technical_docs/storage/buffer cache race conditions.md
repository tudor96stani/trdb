---
created: 2026-02-14
---
Identified 3 possible race conditions:

1. Two threads look for an empty frame
- we must ensure they don't both find, right one after the other, the same frame.
**Solution**
when going through the frames, write-lock the one you find empty and set its page_id => this way, even if you release the lock, you do not need to worry about others using it.

2. Right after one another, two threads want to read the same page. The first one sets the `frame.page_id`, but does not yet fill it. The other stumbles upon the frame, reading a zeroed page.
**Solution**
To determine if a page is cached, always check the `page_map`, never the `frames.page_id`. Also only publish that page is available in `page_map` after successfully loading it from disk.

3. Two threads want to load the same page in different frames at the same time
- here we need to ensure that the 2nd thread can differentiate between 'page not in cache' and 'page being loaded into cache'
**Solution**
Have the `page_map.value` be a `PageEntry` enum with two variants: `Loading` and `Ready(FrameId)`, protected by a mutex + cond_var.
The first thread inserts the value for the `page_id` in the map with the value `Loading`, before it starts the disk I/O. The second thread will see that and wait on the cond_var of the mutex. Once the first thread has finished reading the data from disk, it can change the state to `Ready(FrameId)` and notify the waiters via the cond_var.

**Note**: here, it is important to obtain the read/write guard on the page before updating the map and notifying the waiters. Otherwise, the 2nd thread might wake up and snatch the lock on the page from the first thread which did all the work of reading the data from the disk.
