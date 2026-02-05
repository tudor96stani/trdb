use page::impls::Page;
use page::page_id::PageId;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU32};

/// The ID of a frame is basically just its index in the buffer's vector
pub(crate) type FrameId = usize;

/// A buffer frame is a memory allocation designed to store the contents of a data page in memory,
/// along with other metadata needed by the buffer manager.
/// The `page` is protected by a `RwLock` for concurrent access.
/// The `page_id` is also protected by a `RwLock`. Additionally, the `page_id` is optional (`Option<T>`),
/// to indicate that the frame is empty. An empty frame will contain a zeroed `Page`.
///
/// Access to the `BufferFrame` is not allowed outside the `BufferManager` - instead, `guard-like`
/// structs will be used to provide references to the underlying data.
///
/// The `BufferFrame` is the owner of the `Page` - it consumes it during creation.
/// The `Page` is never moved outside of the frame, only borrowed (either mutably or immutably)
#[derive(Debug)]
pub(crate) struct BufferFrame {
    /// The `PageId` corresponding to the `Page` stored in the `page` field.
    /// Optional. If frame is empty, this will be `None`
    /// Protected by a `RwLock`
    pub(crate) page_id: RwLock<Option<PageId>>,

    /// The actual `Page` instance.
    /// Protected by a `RwLock`
    pub(crate) page: RwLock<Page>,

    /// Atomic pin count for the `page`. Only to be used internally by the buffer manager's eviction policy.
    pub(crate) pin_count: AtomicU32,

    /// Dirtiness of the page flag, backed by an `AtomicBool`
    pub(crate) dirty: AtomicBool,
}

impl Default for BufferFrame {
    fn default() -> Self {
        Self {
            page_id: RwLock::new(None),
            page: RwLock::new(Page::new_zeroed(PageId::new(0, 0))),
            pin_count: AtomicU32::new(0),
            dirty: AtomicBool::new(false),
        }
    }
}
