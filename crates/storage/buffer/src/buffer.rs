//! Provides the implementation for the main buffer leveraged by the engine

use crate::errors::BufferError;
use crate::frame::{BufferFrame, FrameId};
use crate::guards::{PageReadGuard, PageWriteGuard};
use file::api::FileManager;
use page::page_id::PageId;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, RwLock, TryLockError};

/// The state of the page in the buffer
#[derive(Debug)]
enum PageState {
    /// Page is currently being loaded from disk
    Loading,
    /// Page has been loaded and available at the provided FrameId
    Ready(FrameId),
}

/// An entry in the `PageId`->`FrameId` map.
#[derive(Debug)]
struct PageEntry {
    /// State of the page (loaded or loading)
    state: Mutex<PageState>,
    /// Synchronization conditional variable
    cond_var: Condvar,
}

/// The buffer manager responsible for handling the cache pool of data pages.
#[derive(Debug)]
pub struct BufferManager<F: FileManager> {
    file_manager: Arc<F>,
    page_map: RwLock<HashMap<PageId, Arc<PageEntry>>>,
    frames: Vec<BufferFrame>,
}

impl<F: FileManager> BufferManager<F> {
    /// Creates a new empty buffer manager.
    /// Allocates a predefined number of buffer frames.
    /// Sets up internal structures required for managing the pool.
    pub fn new(file_manager: Arc<F>, pool_size: usize) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            frames.push(BufferFrame::default());
        }
        Self {
            file_manager,
            frames,
            page_map: RwLock::new(HashMap::new()),
        }
    }

    /// Retrieves a page from the buffer pool based on its page ID.
    /// If the page cannot be found in the buffer, it is first loaded from disk, cached, then returned.
    ///
    /// # Params
    /// - `page_id`: The identifier of the page to be retrieved.
    ///
    /// # Returns
    /// A `Result` where the `Ok` contains a `PageReadGuard`. A `PageReadGuard` encapsulates the latch
    /// needed to access the underlying `&Page`.
    pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard<'_>, BufferError> {
        self.get_or_load_buffered_page(page_id, |s, fid| s.read_guard_from_frame(fid))
    }

    /// Write guard
    pub fn read_page_mut(&self, page_id: PageId) -> Result<PageWriteGuard<'_>, BufferError> {
        self.get_or_load_buffered_page(page_id, |s, fid| s.write_guard_from_frame(fid))
    }

    /// Finds a free frame and claims it for a new page with the given page ID.
    ///
    /// Note that the frame might contain either a zeroed-page or a previous page that was flushed.
    /// Regardless, the caller is responsible for initializing the page and zero-ing it out.
    ///
    /// # Params
    /// - `page_id`: the ID for the new page.
    ///
    /// # Returns
    /// `PageWriteGuard` instance with write-access to the underlying page.
    pub fn allocate_new_page(&self, page_id: PageId) -> Result<PageWriteGuard<'_>, BufferError> {
        let frame_id = self
            .claim_free_frame(page_id)
            .ok_or(BufferError::BufferFull)?;

        {
            let mut map_guard = self.page_map.write().unwrap();
            map_guard.insert(
                page_id,
                Arc::new(PageEntry {
                    state: Mutex::new(PageState::Ready(frame_id)),
                    cond_var: Condvar::new(),
                }),
            );
        }

        Ok(self.write_guard_from_frame(frame_id))
    }

    /// Shared helper that contains the common logic for loading or returning a page from the buffer.
    /// The `make_guard` closure is responsible for converting a `FrameId` into the requested guard
    /// (either `PageReadGuard` or `PageWriteGuard`).
    fn get_or_load_buffered_page<'a, Guard, MakeGuard>(
        &'a self,
        page_id: PageId,
        make_guard: MakeGuard,
    ) -> Result<Guard, BufferError>
    where
        MakeGuard: Fn(&'a Self, FrameId) -> Guard,
        Guard: 'a,
    {
        // Check if there is a frame that holds this page
        let possible_page_entry = {
            let map_guard = self.page_map.read().unwrap();
            map_guard.get(&page_id).cloned()
        };

        // Happiest of flows - the page is already cached.
        // Note that if the page is either being loaded right now by another thread, or if there is
        // a write latch on the page, this will block.
        if let Some(page_entry) = possible_page_entry {
            let fid = Self::wait_until_ready(&page_entry);
            return Ok(make_guard(self, fid));
        }

        // From this point, we only have logic for cache miss.

        // First we have to lock the map again, this time for write, and check if no one added the entry
        // in the meantime. This will only temporarily lock the entire map.
        let (entry, is_loader_thread) = {
            let mut map = self.page_map.write().unwrap();

            if let Some(existing) = map.get(&page_id).cloned() {
                // We did find it this time - means someone else is about to load it into memory right now
                // We will declare ourselves as not_loaders, and only wait for the other thread to finish
                (existing, false)
            } else {
                // No one else inserted it. We will insert it ourselves, and mark it as Loading,
                // so others will know to wait in case they want this page.
                // We will not lock this mutex though, as others will know to wait while state is Loading
                let new_entry = Arc::new(PageEntry {
                    state: Mutex::new(PageState::Loading),
                    cond_var: Condvar::new(),
                });
                map.insert(page_id, new_entry.clone());
                // Mark ourselves as loaders
                (new_entry, true)
            }
        };

        // Someone else is doing the work, just wait here until they are done
        if !is_loader_thread {
            let frame_id = Self::wait_until_ready(&entry);
            return Ok(make_guard(self, frame_id));
        }

        // We gotta do the load from disk work ourselves.
        let frame_id = self
            .claim_free_frame(page_id)
            .ok_or(BufferError::BufferFull)?;
        {
            let mut page = self.frames[frame_id].page.write().unwrap();

            // Ask the file manager to load data from disk directly into the byte array of the page
            // instance from the buffer frame
            if !self.file_manager.read_page(page_id, page.data_mut()) {
                // rollback claim
                *self.frames[frame_id].page_id.write().unwrap() = None;
                return Err(BufferError::IoReadFailed(page_id));
            }

            // Also update the page's internal `page_id` field.
            page.set_page_id(page_id);
        }

        // Frame is loaded with page contents.
        // First get a latch on the page to be able to return it.
        let guard = make_guard(self, frame_id);

        // Lock the state mutex to set it to Ready (no need to add it in the map, already there).
        // Also notify all waiters that the condition has changed.
        {
            let mut st = entry.state.lock().unwrap();
            *st = PageState::Ready(frame_id);
            entry.cond_var.notify_all();
        }

        Ok(guard)
    }

    /// Goes through the `frames` to find an empty one that can be used
    /// This is done by iterating over the vector, probing for a write-latch without waiting if it
    /// is already taken. This ensures already-in-use frames are skipped.
    /// When a free frame is found (`frame.page_id = None`), the `page_id` is set to the provided one
    /// This ensures the frame is claimed as taken, even if the page has not yet been loaded
    ///
    /// # Params
    /// - `page_id`: the ID of the page we intend to load into the free frame
    ///
    /// # Returns
    /// The `FrameID` for the identified and claimed free frame.
    /// If no empty frame is found, `None`.
    fn claim_free_frame(&self, for_page_id: PageId) -> Option<FrameId> {
        for (frame_id, frame) in self.frames.iter().enumerate() {
            match frame.page_id.try_write() {
                Ok(mut page_id) => {
                    if page_id.is_none() {
                        // Mark it as claimed by setting its page ID, so others running this same
                        // flow in parallel will skip it.
                        *page_id = Some(for_page_id);
                        frame.pin_count.store(1, Ordering::Relaxed);
                        frame.dirty.store(false, Ordering::Relaxed);
                        return Some(frame_id);
                    }
                }
                Err(TryLockError::WouldBlock) => {
                    // Just skip the blocked ones
                    continue;
                }
                Err(TryLockError::Poisoned(_)) => {
                    // This sorta sucks, but it's not really our problem here
                    continue;
                }
            }
        }
        None
    }

    /// Computes a `PageReadGuard` for a frame.
    fn read_guard_from_frame(&self, frame_id: FrameId) -> PageReadGuard<'_> {
        let guard = self.frames[frame_id].page.read().unwrap();
        PageReadGuard { guard }
    }

    /// Computes a `PageWriteGuard` for a frame.
    fn write_guard_from_frame(&self, frame_id: FrameId) -> PageWriteGuard<'_> {
        let guard = self.frames[frame_id].page.write().unwrap();
        PageWriteGuard { guard }
    }

    /// Waits for the `Mutex` on a `PageEntry` to be free to access and the page is loaded into memory
    /// (`PageState = Ready(FrameId)`)
    fn wait_until_ready(entry: &Arc<PageEntry>) -> FrameId {
        let mut state = entry.state.lock().unwrap();
        loop {
            match *state {
                PageState::Ready(fid) => return fid,
                PageState::Loading => {
                    state = entry.cond_var.wait(state).unwrap();
                }
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::buffer::{BufferManager, PageEntry, PageState};
    use crate::frame::FrameId;
    use file::api::FileManager;
    use file::file_catalog::FileCatalog;
    use page::page_id::PageId;
    use page::page_type::PageType;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Condvar, Mutex, RwLock};
    use std::{
        thread,
        time::{Duration, Instant},
    };

    struct MockFileManager {
        requested_pages: RwLock<Vec<PageId>>,
    }

    impl FileManager for MockFileManager {
        fn new<P>(_: P, _: Arc<FileCatalog>) -> Self
        where
            P: Into<PathBuf>,
        {
            Self {
                requested_pages: RwLock::new(Vec::new()),
            }
        }

        fn read_page(&self, page_id: PageId, _: &mut [u8]) -> bool {
            self.requested_pages.write().unwrap().push(page_id);
            true
        }

        fn write_page(&self, _: PageId, _: &[u8]) {}
    }

    fn create_buffer_manager(of_size: usize) -> BufferManager<MockFileManager> {
        let fm = Arc::new(MockFileManager::new("", Arc::new(FileCatalog::new())));
        BufferManager::new(fm.clone(), of_size)
    }

    #[test]
    fn constructor_sets_fields() {
        // Arrange & Act
        let buffer = create_buffer_manager(10);

        // Assert
        assert_eq!(buffer.frames.len(), 10);
        assert!(
            buffer
                .frames
                .iter()
                .all(|f| f.page_id.read().unwrap().is_none())
        );
        assert!(
            buffer
                .frames
                .iter()
                .all(|f| f.page.read().unwrap().data().iter().all(|b| *b == 0u8))
        );
        assert!(
            buffer
                .frames
                .iter()
                .all(|f| f.pin_count.load(Ordering::Relaxed) == 0)
        );
        assert!(
            buffer
                .frames
                .iter()
                .all(|f| !f.dirty.load(Ordering::Relaxed))
        );

        assert!(buffer.page_map.read().unwrap().is_empty());
    }

    #[test]
    fn read_page_already_cached_returns_page_directly() {
        // Arrange
        let buffer = create_buffer_manager(10);
        let page_id = PageId::new(1, 1);
        let frame_id: FrameId = 0;

        {
            let frames = &buffer.frames;
            let frame = frames.get(frame_id).unwrap();
            let mut page_id_write_guard = frame.page_id.write().unwrap();
            *page_id_write_guard = Some(page_id);

            let mut page_write_guard = frame.page.write().unwrap();
            page_write_guard
                .initialize(page_id, PageType::Unsorted)
                .unwrap();
        }

        {
            let page_map = &buffer.page_map;
            let mut map_write_guard = page_map.write().unwrap();
            map_write_guard.insert(
                page_id,
                Arc::new(PageEntry {
                    state: Mutex::new(PageState::Ready(frame_id)),
                    cond_var: Condvar::new(),
                }),
            );
        }

        // Act
        let result = buffer.read_page(page_id).unwrap();

        // Assert
        assert_eq!(result.page_id(), page_id);
        assert_eq!(buffer.file_manager.requested_pages.read().unwrap().len(), 0);
    }

    #[test]
    fn read_page_not_cached_load_from_disk() {
        // Arrange
        let buffer = create_buffer_manager(10);
        let page_id = PageId::new(1, 1);

        assert_eq!(buffer.page_map.read().unwrap().len(), 0);

        // Act
        let result = buffer.read_page(page_id).unwrap();

        // Arrange
        // we got the right page
        assert_eq!(result.page_id(), page_id);
        // read page from file manager only once
        assert_eq!(buffer.file_manager.requested_pages.read().unwrap().len(), 1);
        // only one entry in the page map
        assert_eq!(buffer.page_map.read().unwrap().len(), 1);
        // frames[0] contains our page
        assert_eq!(buffer.frames[0].page_id.read().unwrap().unwrap(), page_id);
    }

    #[test]
    fn read_page_mut_not_cached_load_from_disk() {
        // Arrange
        let buffer = create_buffer_manager(10);
        let page_id = PageId::new(1, 1);

        assert_eq!(buffer.page_map.read().unwrap().len(), 0);

        // Act
        let result = buffer.read_page_mut(page_id).unwrap();

        // Arrange
        // we got the right page
        assert_eq!(result.page_id(), page_id);
        // read page from file manager only once
        assert_eq!(buffer.file_manager.requested_pages.read().unwrap().len(), 1);
        // only one entry in the page map
        assert_eq!(buffer.page_map.read().unwrap().len(), 1);
        // frames[0] contains our page
        assert_eq!(buffer.frames[0].page_id.read().unwrap().unwrap(), page_id);

        // Only an assertion to check that the mutable deref of the PageWriteGuard works.
        let mut page_mut = result;
        let _ = page_mut.data_mut();
    }

    #[test]
    fn wait_until_ready_blocks_until_another_thread_sets_ready() {
        use page::page_type::PageType;
        // Arrange
        let buffer = create_buffer_manager(1);
        let page_id = PageId::new(1, 1);
        let frame_id: FrameId = 0;

        // Insert an entry in Loading state into the page_map
        let entry = Arc::new(PageEntry {
            state: Mutex::new(PageState::Loading),
            cond_var: Condvar::new(),
        });
        {
            let mut map = buffer.page_map.write().unwrap();
            map.insert(page_id, entry.clone());
        }

        // Prepare the frame with the page so that when Ready is set, read_guard_from_frame works
        {
            let frame = &buffer.frames[frame_id];
            *frame.page_id.write().unwrap() = Some(page_id);
            let mut page_write = frame.page.write().unwrap();
            page_write.initialize(page_id, PageType::Unsorted).unwrap();
        }

        // Spawn a thread that will set the entry to Ready after a short sleep and notify
        let sleep_ms = 50u64;
        let entry_cloned = entry.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(sleep_ms));
            let mut st = entry_cloned.state.lock().unwrap();
            *st = PageState::Ready(frame_id);
            entry_cloned.cond_var.notify_all();
        });

        // Act: calling read_page should block until the other thread sets Ready
        let start = Instant::now();
        let guard = buffer.read_page(page_id).unwrap();
        let elapsed = start.elapsed();

        // Assert: it waited at least the sleep duration and returned the correct page
        assert!(elapsed >= Duration::from_millis(sleep_ms));
        assert_eq!(guard.page_id(), page_id);

        handle.join().unwrap();
    }

    #[test]
    fn claim_free_frame_skips_locked_and_claims_first_free() {
        // Arrange
        let buffer = create_buffer_manager(3);
        let target_page = PageId::new(9, 9);

        {
            // Simulate a locked frame (will cause try_write to return WouldBlock)
            // Need to wrap this whole part in a code block to ensure _locked_guard gets dropped before the 2nd part of the test
            let _locked_guard = buffer.frames[0].page_id.write().unwrap();

            // Simulate an occupied frame (page_id = Some)
            {
                let mut w = buffer.frames[1].page_id.write().unwrap();
                *w = Some(PageId::new(2, 2));
            }

            // frame 2 is free and should be claimed
            let claimed = buffer.claim_free_frame(target_page);
            assert_eq!(claimed, Some(2));

            // Assert frame metadata was initialized
            assert_eq!(
                buffer.frames[2].page_id.read().unwrap().unwrap(),
                target_page
            );
            assert_eq!(buffer.frames[2].pin_count.load(Ordering::Relaxed), 1);
            assert!(!buffer.frames[2].dirty.load(Ordering::Relaxed));
        } // frames[0] released here.

        // Now mark all frames as occupied and ensure None is returned
        for i in 0..3 {
            let mut w = buffer.frames[i].page_id.write().unwrap();
            *w = Some(PageId::new(i as u32, i as u32));
        }
        let none_result = buffer.claim_free_frame(PageId::new(99, 99));
        assert!(none_result.is_none());
    }

    #[test]
    fn claim_free_frame_skips_poisoned_frame() {
        // Arrange
        let buffer = create_buffer_manager(2);

        // Use a scoped thread so we can borrow `buffer.frames` safely without requiring 'static.
        // The spawned closure panics while holding the write lock, poisoning the RwLock.
        thread::scope(|s| {
            let handle = s.spawn(|| {
                let _guard = buffer.frames[0].page_id.write().unwrap();
                panic!("simulate panic while holding lock");
                // _guard gets dropped during unwind, poisoning the lock
            });
            // Join the scoped handle; it will return Err because the thread panicked.
            let _ = handle.join();
        });

        // Act
        let claimed = buffer.claim_free_frame(PageId::new(99, 99));
        assert_eq!(claimed, Some(1));
    }
}
