use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get {key: String},
    Set {key: String, value: String},
    Remove {key: String}
}

#[derive(Serialize, Deserialize, Debug)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SetResponse {
    Ok(()),
    Err(String)
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RemoveResponse {
    Ok(()),
    Err(String)
}
