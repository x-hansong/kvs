//! A simple key/value store.

pub use engine::kv::KvStore;
pub use engine::KvsEngine;
pub use engine::SledKvsEngine;
pub use server::KvsServer;
pub use client::KvsClient;
pub use error::{KvsError, Result};

mod error;
pub mod engine;
mod server;
mod common;
mod client;