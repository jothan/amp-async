mod de;
mod ser;
mod types;

pub use de::from_bytes;
pub use ser::*;
pub use types::*;

pub(crate) const AMP_LIST_COOKIE: &str = "AmpList-450784";
pub(crate) const AMP_KEY_LIMIT: usize = 0xff;
pub(crate) const AMP_VALUE_LIMIT: usize = 0xffff;
pub(crate) const AMP_LENGTH_SIZE: usize = std::mem::size_of::<u16>();
