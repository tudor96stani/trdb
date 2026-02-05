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
