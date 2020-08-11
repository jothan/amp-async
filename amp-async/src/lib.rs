#![warn(rust_2018_idioms)]
// Clippy does not like using Bytes as keys.
#![allow(clippy::mutable_key_type)]

mod codecs;
mod error;
mod frame;
mod server;

pub use amp_serde::AmpList;
pub use codecs::Dec as Decoder;
pub use error::*;
pub use frame::*;
pub use server::*;
