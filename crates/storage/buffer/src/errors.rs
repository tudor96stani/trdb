use page::page_id::PageId;

/// Buffer error.
#[derive(Debug)]
pub enum BufferError {
    /// Buffer was full
    BufferFull,
    /// Could not read file from disk
    IoReadFailed(PageId),
}
