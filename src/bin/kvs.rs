use std::process::exit;

use clap::Clap;
use std::path::PathBuf;
use std::env::current_dir;
use kvs::{Result, KvsError, KvStore};

#[derive(Clap)]
#[clap(name = env!("CARGO_PKG_NAME"),version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opts {
    #[clap(subcommand)]
    command: Command
}

#[derive(Clap)]
enum Command {
    #[clap()]
    Get(Get),

    #[clap()]
    Set(Set),

    #[clap()]
    Rm(Rm),
}

#[derive(Clap)]
struct Get {
    #[clap()]
    key: String,
}

#[derive(Clap)]
struct Rm {
    #[clap()]
    key: String,
}

#[derive(Clap)]
struct Set {
    #[clap()]
    key: String,
    #[clap()]
    value: String,
}


fn main() -> Result<()> {
    let opts: Opts = Opts::parse();
    let mut store = KvStore::open(current_dir()?)?;

    match opts.command {
        Command::Get(get) => {
            let value = store.get(get.key)?;
            match value {
                Some(value) => {
                    println!("{}", value)
                },
                None => {
                    println!("Key not found")
                }
            }
        }
        Command::Set(set) => {
            store.set(set.key, set.value)?
        }
        Command::Rm(rm) => {
            match store.remove(rm.key) {
                Ok(()) => {},
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                },
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}
