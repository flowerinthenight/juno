mod spanner;

use anyhow::Result;
use clap::Parser;
use ctrlc;
use hedge_rs::*;
use log::*;
use spanner::*;
use std::{
    fmt::Write as _,
    io::{BufReader, prelude::*},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
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

    /// Host:port for test TCP server (tmp)
    #[arg(long, default_value = "0.0.0.0:9091")]
    test_tcp: String,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).unwrap())?;

    // We will use this channel for the 'send' and 'broadcast' features.
    // Use Sender as inputs, then we read replies through the Receiver.
    let (tx_comms, rx_comms): (Sender<Comms>, Receiver<Comms>) = channel();

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
            .tx_comms(Some(tx_comms.clone()))
            .build(),
    ));

    {
        op.lock().unwrap().run()?;
    }

    // Start a new thread that will serve as handlers for both send() and broadcast() APIs.
    let id_handler = args.id.clone();
    thread::spawn(move || {
        loop {
            match rx_comms.recv() {
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

    // Starts a new thread for the API.
    let op_api = op.clone();
    let hp_api = args.api.clone();
    thread::spawn(move || {
        let listen = TcpListener::bind(hp_api.to_string()).unwrap();
        for stream in listen.incoming() {
            match stream {
                Err(_) => break,
                Ok(v) => {
                    let mut reader = BufReader::new(&v);
                    let mut msg = String::new();
                    reader.read_line(&mut msg).unwrap();

                    if msg.starts_with("q") {
                        break;
                    }

                    if msg.starts_with("send") {
                        let send = msg[..msg.len() - 1].to_string();
                        match op_api.lock().unwrap().send(send.as_bytes().to_vec()) {
                            Ok(v) => info!("reply from leader: {}", String::from_utf8(v).unwrap()),
                            Err(e) => error!("send failed: {e}"),
                        }

                        continue;
                    }

                    if msg.starts_with("broadcast") {
                        let (tx_reply, rx_reply): (Sender<Broadcast>, Receiver<Broadcast>) = channel();
                        let send = msg[..msg.len() - 1].to_string();

                        {
                            op_api
                                .lock()
                                .unwrap()
                                .broadcast(send.as_bytes().to_vec(), tx_reply)
                                .unwrap();
                        }

                        // Read through all the replies from all nodes. An empty
                        // id or message marks the end of the streaming reply.
                        loop {
                            match rx_reply.recv().unwrap() {
                                Broadcast::ReplyStream { id, msg, error } => {
                                    if id == "" || msg.len() == 0 {
                                        break;
                                    }

                                    if error {
                                        error!("{:?}", String::from_utf8(msg).unwrap());
                                    } else {
                                        info!("{:?}", String::from_utf8(msg).unwrap());
                                    }
                                }
                            }
                        }

                        continue;
                    }

                    info!("{msg:?} not supported");
                }
            };
        }
    });

    // Starts a new thread for our test TCP server. Messages that start with 'q' will cause the
    // server thread to terminate. Messages that begin with 'send' will send that message to
    // the current leader. Finally, messages that begin with 'broadcast' will broadcast that
    // message to all nodes in the group.
    let op_tcp = op.clone();
    let hp_tcp = args.test_tcp.clone();
    thread::spawn(move || {
        let listen = TcpListener::bind(hp_tcp.to_string()).unwrap();
        for stream in listen.incoming() {
            match stream {
                Err(_) => break,
                Ok(v) => {
                    let mut reader = BufReader::new(&v);
                    let mut msg = String::new();
                    reader.read_line(&mut msg).unwrap();

                    if msg.starts_with("q") {
                        break;
                    }

                    if msg.starts_with("send") {
                        let send = msg[..msg.len() - 1].to_string();
                        match op_tcp.lock().unwrap().send(send.as_bytes().to_vec()) {
                            Ok(v) => info!("reply from leader: {}", String::from_utf8(v).unwrap()),
                            Err(e) => error!("send failed: {e}"),
                        }

                        continue;
                    }

                    if msg.starts_with("broadcast") {
                        let (tx_reply, rx_reply): (Sender<Broadcast>, Receiver<Broadcast>) = channel();
                        let send = msg[..msg.len() - 1].to_string();

                        {
                            op_tcp
                                .lock()
                                .unwrap()
                                .broadcast(send.as_bytes().to_vec(), tx_reply)
                                .unwrap();
                        }

                        // Read through all the replies from all nodes. An empty
                        // id or message marks the end of the streaming reply.
                        loop {
                            match rx_reply.recv().unwrap() {
                                Broadcast::ReplyStream { id, msg, error } => {
                                    if id == "" || msg.len() == 0 {
                                        break;
                                    }

                                    if error {
                                        error!("{:?}", String::from_utf8(msg).unwrap());
                                    } else {
                                        info!("{:?}", String::from_utf8(msg).unwrap());
                                    }
                                }
                            }
                        }

                        continue;
                    }

                    info!("{msg:?} not supported");
                }
            };
        }
    });

    rx.recv()?; // wait for Ctrl-C
    op.lock().unwrap().close();

    Ok(())
}
