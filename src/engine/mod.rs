pub mod kv;
pub mod sled;

pub use self::kv::KvStore;
pub use self::sled::SledKvsEngine;
use crate::Result;

pub trait KvsEngine {

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}
