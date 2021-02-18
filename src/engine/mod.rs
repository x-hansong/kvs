pub mod kv;
pub mod sled;

pub use self::kv::KvStore;
pub use self::sled::SledKvsEngine;
use crate::Result;

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}
