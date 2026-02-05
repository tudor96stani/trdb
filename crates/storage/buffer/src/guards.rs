use page::impls::Page;
use std::ops::Deref;
use std::sync::RwLockReadGuard;

/// Provides read access to a `Page` instance stored in one of the buffer's frames.
/// Shared latch, allowing concurrent reads.
/// Free as soon as possible.
#[derive(Debug)]
pub struct PageReadGuard<'a> {
    /// The underlying `RoLockReadGuard` which will be dereferenced to `&Page`
    pub guard: RwLockReadGuard<'a, Page>,
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}
