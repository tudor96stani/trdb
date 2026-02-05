use page::impls::Page;
use std::ops::{Deref, DerefMut};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

/// Provides read access to a `Page` instance stored in one of the buffer's frames.
/// Shared latch, allowing concurrent reads.
/// Free as soon as possible.
#[derive(Debug)]
pub struct PageReadGuard<'a> {
    /// The underlying `RwLockReadGuard` which will be dereferenced to `&Page`
    pub guard: RwLockReadGuard<'a, Page>,
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// Provides write access to a `Page`instance stored in one of the buffer's frames.
/// Exclusive latch.
/// Free as soon as possible.
#[derive(Debug)]
pub struct PageWriteGuard<'a> {
    /// The underlying `RwLockWriteGuard` which will be dereferenced to `&Page`
    pub guard: RwLockWriteGuard<'a, Page>,
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = Page;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}
