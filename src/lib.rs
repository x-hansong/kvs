//! A simple key/value store.

pub use kv::KvStore;
pub use error::{KvsError, Result};

mod kv;
mod error;