use clap::Parser;

/// Something of the sort. Please honor the period.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    name: String,

    /// Number of times to greet
    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

fn main() {
    let args = Args::parse();

    for _ in 0..args.count {
        println!("Hello {}!", args.name);
    }
}
