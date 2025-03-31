use clap::Parser;

/// Something of the sort. Please honor the period.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// Node id (format should be host:port)
    #[arg(long)]
    id: String,

    /// Spanner database (projects/p/instances/i/databases/db)
    #[arg(long)]
    db: String,

    /// Spanner database for hedge-rs (same with `--db` if not set)
    #[arg(long)]
    db_hedge: String,

    /// Spanner table (for hedge-rs)
    #[arg(long)]
    table: String,

    /// Lock name (for hedge-rs)
    #[arg(short, long, default_value = "juno")]
    name: String,
}

fn main() {
    let args = Args::parse();

    println!("id: {}!", args.id);
    println!("db: {}!", args.db);
}
