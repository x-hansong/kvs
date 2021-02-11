use std::process::exit;

use clap::Clap;

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


fn main() {
    let opts: Opts = Opts::parse();
    match opts.command {
        Command::Get(get) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Set(set) => {
            eprintln!("unimplemented");
            exit(1);
        }
        Command::Rm(rm) => {
            eprintln!("unimplemented");
            exit(1);
        }
    }
}
