use std::collections::HashMap;
use std::path::PathBuf;
use crate::{Result, KvsError};
use serde::{Serialize, Deserialize};

/// The `KvStore` stores string key/value pairs.
#[derive(Default)]
pub struct KvStore {
    map: HashMap<String, String>
}

impl KvStore {

    /// Open a `KvStore` with the given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        Ok(KvStore::new())
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(key.as_str()).cloned())
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(key.as_str());
        Ok(())
    }


}

#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set {key: String, value: String},
    Remove {key: String}
}