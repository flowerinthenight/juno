mod spanner;

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::{Receiver, Sender, unbounded};
use ctrlc;
use hedge_rs::*;
use log::*;
use spanner::*;
use std::{
    collections::HashMap,
    fmt::Write as _,
    io::{BufReader, prelude::*},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Instant,
};

#[macro_use(defer)]
extern crate scopeguard;

/// Simple PubSub system using Cloud Spanner as backing storage.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[clap(verbatim_doc_comment)]
struct Args {
    /// Node id (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:8080")]
    id: String,

    /// Host:port for the API (format should be host:port)
    #[arg(long, long, default_value = "0.0.0.0:9090")]
    api: String,

    /// Spanner database URL (format: 'projects/p/instances/i/databases/db')
    #[arg(long)]
    db: String,

    /// Spanner database for hedge-rs (same with `--db` if not set)
    #[arg(long, long, default_value = "*")]
    db_hedge: String,

    /// Spanner table (for hedge-rs)
    #[arg(long, long, default_value = "juno")]
    table: String,

    /// Lock name (for hedge-rs)
    #[arg(short, long, default_value = "juno")]
    name: String,
}

#[derive(Debug)]
enum WorkerCtrl {
    HandleApi(TcpStream),
    PingMember(String),
    ToLeader {
        msg: Vec<u8>,
        tx: Sender<Vec<u8>>,
    },
    Broadcast {
        name: String,
        msg: Vec<u8>,
        tx: Sender<Vec<u8>>,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let (tx_ctrlc, rx_ctrlc) = mpsc::channel();
    ctrlc::set_handler(move || tx_ctrlc.send(()).unwrap())?;

    // We will use this channel for the 'send' and 'broadcast' features.
    // Use Sender as inputs, then we read replies through the Receiver.
    let (tx_op, rx_op): (mpsc::Sender<Comms>, mpsc::Receiver<Comms>) = mpsc::channel();

    let mut db_hedge = String::new();
    if args.db_hedge == "*" {
        db_hedge = args.db;
    }

    let op = Arc::new(Mutex::new(
        OpBuilder::new()
            .id(args.id.clone())
            .db(db_hedge)
            .table(args.table)
            .name(args.name)
            .lease_ms(3_000)
            .tx_comms(Some(tx_op.clone()))
            .build(),
    ));

    {
        op.lock().unwrap().run()?;
    }

    // Start a new thread that will serve as handlers for both send() and broadcast() APIs.
    let id_handler = args.id.clone();
    thread::spawn(move || {
        loop {
            match rx_op.recv() {
                Ok(v) => match v {
                    // This is our 'send' handler. When we are leader, we reply to all
                    // messages coming from other nodes using the send() API here.
                    Comms::ToLeader { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        info!("[send()] received: {msg_s}");

                        // Send our reply back using 'tx'.
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from leader:{}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    // This is our 'broadcast' handler. When a node broadcasts a message,
                    // through the broadcast() API, we reply here.
                    Comms::Broadcast { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        info!("[broadcast()] received: {msg_s}");

                        // Send our reply back using 'tx'.
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from {}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    Comms::OnLeaderChange(state) => {
                        info!("leader state change: {state}");
                    }
                },
                Err(e) => {
                    error!("{e}");
                    continue;
                }
            }
        }
    });

    let (tx_work, rx_work): (Sender<WorkerCtrl>, Receiver<WorkerCtrl>) = unbounded();
    let rxs: Arc<Mutex<HashMap<usize, Receiver<WorkerCtrl>>>> = Arc::new(Mutex::new(HashMap::new()));
    let cpus = num_cpus::get();

    for i in 0..cpus {
        let recv = rxs.clone();

        {
            let mut rv = recv.lock().unwrap();
            rv.insert(i, rx_work.clone());
        }
    }

    // Start our worker threads for our TCP server.
    for i in 0..cpus {
        let recv = rxs.clone();
        thread::spawn(move || {
            loop {
                let mut rx: Option<Receiver<WorkerCtrl>> = None;

                {
                    let rxval = match recv.lock() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("T{i}: lock failed: {e}");
                            break;
                        }
                    };

                    if let Some(v) = rxval.get(&i) {
                        rx = Some(v.clone());
                    }
                }

                match rx.unwrap().recv().unwrap() {
                    WorkerCtrl::HandleApi(stream) => {
                        let start = Instant::now();

                        defer! {
                            debug!("[T{i}]: tcp took {:?}", start.elapsed());
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    // Starts a new thread for the API.
    let tx_api = tx_work.clone();
    let hp_api = args.api.clone();
    thread::spawn(move || {
        let listen = TcpListener::bind(hp_api.to_string()).unwrap();
        for stream in listen.incoming() {
            let stream = match stream {
                Ok(v) => v,
                Err(e) => {
                    error!("stream failed: {e}");
                    continue;
                }
            };

            tx_api.send(WorkerCtrl::HandleApi(stream)).unwrap();
        }
    });

    rx_ctrlc.recv()?; // wait for Ctrl-C
    op.lock().unwrap().close();

    Ok(())
}
