#![warn(rust_2018_idioms)]
// Clippy does not like using Bytes as keys.
#![allow(clippy::mutable_key_type)]

use bytes::{Bytes, BytesMut};

mod codecs;
mod error;
mod frame;
mod server;

pub use amp_serde::{AmpList, V1, V2};
pub use codecs::Dec as Decoder;
pub use error::*;
pub use frame::*;
pub use server::*;

pub trait AmpVersion: amp_serde::AmpEncoder + amp_serde::AmpDecoder
where
    Self: Sized,
{
    fn handle_value<D: std::iter::Extend<(Bytes, Bytes)>>(
        dec: &mut Decoder<Self, D>,
        segment: BytesMut,
    );
}
