use std::convert::TryFrom;

/// Enumeration of different page types in the storage system.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Simple slotted unsorted data page.
    Unsorted = 1,
    /// B+ tree index root page.
    IndexRoot = 2,
    /// B+ tree index internal page.
    IndexInternal = 3,
    /// B+ tree index leaf data page.
    IndexLeaf = 4,
}

impl From<PageType> for u16 {
    /// Converts a `PageType` enum variant to its corresponding `u16` value.
    fn from(p: PageType) -> Self {
        p as u16
    }
}

impl TryFrom<u16> for PageType {
    type Error = ();

    /// Attempts to convert a `u16` value to its corresponding `PageType` enum variant.
    fn try_from(v: u16) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(PageType::Unsorted),
            2 => Ok(PageType::IndexRoot),
            3 => Ok(PageType::IndexInternal),
            4 => Ok(PageType::IndexLeaf),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_from_u16() {
        assert_eq!(u16::from(PageType::Unsorted), 1);
        assert_eq!(u16::from(PageType::IndexRoot), 2);
        assert_eq!(u16::from(PageType::IndexInternal), 3);
        assert_eq!(u16::from(PageType::IndexLeaf), 4);

        assert_eq!(PageType::try_from(1).unwrap(), PageType::Unsorted);
        assert_eq!(PageType::try_from(2).unwrap(), PageType::IndexRoot);
        assert_eq!(PageType::try_from(3).unwrap(), PageType::IndexInternal);
        assert_eq!(PageType::try_from(4).unwrap(), PageType::IndexLeaf);

        assert!(PageType::try_from(99).is_err());
    }
}
