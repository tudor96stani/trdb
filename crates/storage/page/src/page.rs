const PAGE_SIZE: usize = 4096;

struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    fn new_zeroed() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }
}
