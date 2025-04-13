use anyhow::Result;
use clap::Parser;
use crossbeam_channel::{Receiver, Sender, unbounded};
use ctrlc;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    statement::Statement,
};
use hedge_rs::*;
use log::*;
use regex::Regex;
use std::{
    collections::HashMap,
    fmt::Write as _,
    io::{BufReader, prelude::*},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};
use tokio::runtime::Runtime;
use uuid::Uuid;

#[macro_use(defer)]
extern crate scopeguard;

static TOPICS_TABLE: &'static str = "zzz_topics";
static SUBSCRIPTIONS_TABLE: &'static str = "zzz_subscriptions";
static MESSAGES_TABLE: &'static str = "zzz_messages";

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
        db_hedge = args.db.clone();
    }

    let leader = Arc::new(AtomicUsize::new(0)); // for leader state change callback

    // Setup hedge-rs.Op as our memberlist manager.
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
    let leader_clone = leader.clone();
    let id_handler = args.id.clone();
    thread::spawn(move || {
        loop {
            match rx_op.recv() {
                Err(_) => continue,
                Ok(v) => match v {
                    // This is our 'send' handler. When we are leader, we reply to all
                    // messages coming from other nodes using the send() API here.
                    Comms::ToLeader { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from leader:{}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    // This is our 'broadcast' handler. When a node broadcasts a message,
                    // through the broadcast() API, we reply here.
                    Comms::Broadcast { msg, tx } => {
                        let msg_s = String::from_utf8(msg).unwrap();
                        let mut reply = String::new();
                        write!(&mut reply, "echo '{msg_s}' from {}", id_handler.to_string()).unwrap();
                        tx.send(reply.as_bytes().to_vec()).unwrap();
                    }
                    Comms::OnLeaderChange(state) => leader_clone.store(state, Ordering::Relaxed),
                },
            }
        }
    });

    let (tx_work, rx_work): (Sender<WorkerCtrl>, Receiver<WorkerCtrl>) = unbounded();
    let rxh: Arc<Mutex<HashMap<usize, Receiver<WorkerCtrl>>>> = Arc::new(Mutex::new(HashMap::new()));
    let cpus = num_cpus::get();

    for i in 0..cpus {
        let recv = rxh.clone();

        {
            let mut rv = recv.lock().unwrap();
            rv.insert(i, rx_work.clone());
        }
    }

    // Start our API worker threads.
    for i in 0..cpus {
        let rxc = rxh.clone();
        let db_clone = args.db.clone();
        let op_clone = op.clone();
        thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            let (tx, rx): (Sender<Option<Client>>, Receiver<Option<Client>>) = unbounded();
            rt.block_on(async {
                let config = ClientConfig::default().with_auth().await;
                match config {
                    Err(_) => tx.send(None).unwrap(),
                    Ok(v) => {
                        let client = Client::new(db_clone, v).await;
                        match client {
                            Ok(v) => tx.send(Some(v)).unwrap(),
                            Err(e) => {
                                error!("client failed: {e}");
                                tx.send(None).unwrap();
                            }
                        }
                    }
                }
            });

            let read = rx.recv().unwrap();
            if read.is_none() {
                return;
            }

            let client = read.unwrap(); // shouldn't panic

            loop {
                let mut o_rx: Option<Receiver<WorkerCtrl>> = None;

                {
                    let rxv = match rxc.lock() {
                        Ok(v) => v,
                        Err(e) => {
                            error!("T{i}: lock failed: {e}");
                            break;
                        }
                    };

                    if let Some(v) = rxv.get(&i) {
                        o_rx = Some(v.clone());
                    }
                }

                if o_rx.is_none() {
                    continue;
                }

                let rx = o_rx.unwrap();
                match rx.recv().unwrap() {
                    WorkerCtrl::HandleApi(mut stream) => {
                        let mut reader = BufReader::new(&stream);
                        let mut data = String::new();
                        reader.read_line(&mut data).unwrap();
                        match data.get(..1).unwrap() {
                            //
                            // &<topic-name>\n
                            //
                            // Create a topic. Name should start/end with a letter.
                            // Hyphens are allowed in between.
                            //
                            "&" => {
                                let start = Instant::now();

                                defer! {
                                    info!("[T{i}]: create-topic took {:?}", start.elapsed());
                                }

                                let topic = &data[1..&data.len() - 1];
                                let re = Regex::new(r"^[a-zA-Z]+[a-zA-Z0-9-]+[a-zA-Z0-9]$").unwrap();
                                if !re.is_match(topic) {
                                    let mut err = String::new();
                                    write!(&mut err, "-Invalid topic name\n").unwrap();
                                    let _ = stream.write_all(err.as_bytes());
                                    return;
                                }

                                let (tx_rt, rx_rt): (Sender<String>, Receiver<String>) = unbounded();
                                rt.block_on(async {
                                    let mut q = String::new();
                                    write!(&mut q, "insert {} ", TOPICS_TABLE).unwrap();
                                    write!(&mut q, "(TopicName, Created, Updated) ").unwrap();
                                    write!(&mut q, "values ('{}', ", topic).unwrap();
                                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(), ").unwrap();
                                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP())").unwrap();
                                    let stmt = Statement::new(q);
                                    let rwt = client.begin_read_write_transaction().await;
                                    if let Err(e) = rwt {
                                        let mut err = String::new();
                                        write!(&mut err, "{e}").unwrap();
                                        tx_rt.send(err).unwrap();
                                        return;
                                    }

                                    let mut t = rwt.unwrap();
                                    let res = t.update(stmt).await;
                                    let res = t.end(res, None).await;
                                    match res {
                                        Ok(_) => tx_rt.send(String::new()).unwrap(),
                                        Err(e) => {
                                            let mut err = String::new();
                                            write!(&mut err, "{e}").unwrap();
                                            tx_rt.send(err).unwrap();
                                        }
                                    };
                                });

                                let res = rx_rt.recv().unwrap();
                                let mut ack = String::new();
                                if res.len() != 0 {
                                    write!(&mut ack, "-{res}\n").unwrap();
                                } else {
                                    write!(&mut ack, "+OK\n").unwrap();
                                }

                                let _ = stream.write_all(ack.as_bytes());
                            }
                            //
                            // %<topic-name>\n
                            //
                            // Delete a topic. Deleting a topic also deletes all associated
                            // subscriptions and messages.
                            //
                            "%" => {
                                let start = Instant::now();

                                defer! {
                                    info!("[T{i}]: delete-topic took {:?}", start.elapsed());
                                }

                                let topic = &data[1..&data.len() - 1];
                                let (tx_rt, rx_rt): (Sender<String>, Receiver<String>) = unbounded();
                                rt.block_on(async {
                                    let mut q = String::new();
                                    write!(&mut q, "delete from {} ", TOPICS_TABLE).unwrap();
                                    write!(&mut q, "where TopicName = '{}'", topic).unwrap();
                                    let stmt = Statement::new(q);
                                    let rwt = client.begin_read_write_transaction().await;
                                    if let Err(e) = rwt {
                                        let mut err = String::new();
                                        write!(&mut err, "{e}").unwrap();
                                        tx_rt.send(err).unwrap();
                                        return;
                                    }

                                    let mut t = rwt.unwrap();
                                    let res = t.update(stmt).await;
                                    let res = t.end(res, None).await;
                                    match res {
                                        Ok(_) => tx_rt.send(String::new()).unwrap(),
                                        Err(e) => {
                                            let mut err = String::new();
                                            write!(&mut err, "{e}").unwrap();
                                            tx_rt.send(err).unwrap();
                                        }
                                    };
                                });

                                let res = rx_rt.recv().unwrap();
                                let mut ack = String::new();
                                if res.len() != 0 {
                                    write!(&mut ack, "-{res}\n").unwrap();
                                } else {
                                    write!(&mut ack, "+OK\n").unwrap();
                                }

                                let _ = stream.write_all(ack.as_bytes());
                            }
                            //
                            // ^<topic-name> <subscription-name> <prop1=val1[ prop2=val2]...>\n
                            //
                            // Create a subscription. Name should start/end with a letter.
                            // Hyphens are allowed in between.
                            //
                            // Supported properties:
                            //
                            //   AcknowledgeTimeout=secs [default=60]
                            //   AutoExtend=bool [default=true]
                            //
                            "^" => {
                                let start = Instant::now();

                                defer! {
                                    info!("[T{i}]: create-subscription took {:?}", start.elapsed());
                                }

                                let line = &data[1..&data.len() - 1];
                                let vals: Vec<&str> = line.split(" ").collect();
                                if vals.len() < 2 {
                                    let mut err = String::new();
                                    write!(&mut err, "-Invalid payload format\n").unwrap();
                                    let _ = stream.write_all(err.as_bytes());
                                    return;
                                }

                                let re = Regex::new(r"^[a-zA-Z]+[a-zA-Z0-9-]+[a-zA-Z0-9]$").unwrap();
                                if !re.is_match(vals[0]) {
                                    let mut err = String::new();
                                    write!(&mut err, "-Invalid topic name\n").unwrap();
                                    let _ = stream.write_all(err.as_bytes());
                                    return;
                                }

                                if !re.is_match(vals[1]) {
                                    let mut err = String::new();
                                    write!(&mut err, "-Invalid subscription name\n").unwrap();
                                    let _ = stream.write_all(err.as_bytes());
                                    return;
                                }

                                let mut ack_timeout = 60;
                                let mut auto_extend = true;
                                for i in 2..vals.len() {
                                    let prop = vals[i].split("=").collect::<Vec<&str>>();
                                    if prop.len() != 2 {
                                        let mut err = String::new();
                                        write!(&mut err, "-Invalid property format\n").unwrap();
                                        let _ = stream.write_all(err.as_bytes());
                                        return;
                                    }

                                    match prop[0] {
                                        "AcknowledgeTimeout" => {
                                            let val = prop[1].parse::<i64>();
                                            if val.is_err() {
                                                let mut err = String::new();
                                                write!(&mut err, "-Invalid AcknowledgeTimeout value\n").unwrap();
                                                let _ = stream.write_all(err.as_bytes());
                                                return;
                                            }
                                            ack_timeout = val.unwrap() as i64;
                                        }
                                        "AutoExtend" => {
                                            let val = prop[1].parse::<bool>();
                                            if val.is_err() {
                                                let mut err = String::new();
                                                write!(&mut err, "-Invalid AutoExtend value\n").unwrap();
                                                let _ = stream.write_all(err.as_bytes());
                                                return;
                                            }
                                            auto_extend = val.unwrap() as bool;
                                        }
                                        _ => {
                                            let mut err = String::new();
                                            write!(&mut err, "-Unknown property\n").unwrap();
                                            let _ = stream.write_all(err.as_bytes());
                                            return;
                                        }
                                    }
                                }

                                let (tx_rt, rx_rt): (Sender<String>, Receiver<String>) = unbounded();
                                rt.block_on(async {
                                    let mut q = String::new();
                                    write!(&mut q, "insert {} ", SUBSCRIPTIONS_TABLE).unwrap();
                                    write!(&mut q, "(TopicName, SubscriptionName, ").unwrap();
                                    write!(&mut q, "AcknowledgeTimeout, AutoExtend, Created, Updated) ").unwrap();
                                    write!(&mut q, "values ('{}', ", vals[0]).unwrap();
                                    write!(&mut q, "'{}', ", vals[1]).unwrap();
                                    write!(&mut q, "{}, ", ack_timeout).unwrap();
                                    write!(&mut q, "{}, ", auto_extend).unwrap();
                                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(), ").unwrap();
                                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP())").unwrap();
                                    let stmt = Statement::new(q);
                                    let rwt = client.begin_read_write_transaction().await;
                                    if let Err(e) = rwt {
                                        let mut err = String::new();
                                        write!(&mut err, "{e}").unwrap();
                                        tx_rt.send(err).unwrap();
                                        return;
                                    }

                                    let mut t = rwt.unwrap();
                                    let res = t.update(stmt).await;
                                    let res = t.end(res, None).await;
                                    match res {
                                        Ok(_) => tx_rt.send(String::new()).unwrap(),
                                        Err(e) => {
                                            let mut err = String::new();
                                            write!(&mut err, "{e}").unwrap();
                                            tx_rt.send(err).unwrap();
                                        }
                                    };
                                });

                                let res = rx_rt.recv().unwrap();
                                let mut ack = String::new();
                                if res.len() != 0 {
                                    write!(&mut ack, "-{res}\n").unwrap();
                                } else {
                                    write!(&mut ack, "+OK\n").unwrap();
                                }

                                let _ = stream.write_all(ack.as_bytes());
                            }
                            //
                            // *<subscription-name>\n
                            //
                            // Receive messages from a subscription.
                            //
                            "*" => {
                                let start = Instant::now();

                                defer! {
                                    info!("[T{i}]: subscribe took {:?}", start.elapsed());
                                }
                            }
                            //
                            // @<message-id>\n
                            //
                            // Acknowledge a message.
                            //
                            "@" => {
                                let start = Instant::now();

                                defer! {
                                    info!("[T{i}]: ack-message took {:?}", start.elapsed());
                                }
                            }
                            //
                            // #<topic-name> <base64(msg) base64(attrs)>\n
                            //
                            // Publish a message to a topic.
                            //
                            "#" => {
                                let start = Instant::now();

                                defer! {
                                    info!("[T{i}]: publish-message took {:?}", start.elapsed());
                                }

                                let line = &data[1..&data.len() - 1];
                                let vals: Vec<&str> = line.split(" ").collect();
                                if vals.len() < 2 {
                                    let mut err = String::new();
                                    write!(&mut err, "-Invalid payload format\n").unwrap();
                                    let _ = stream.write_all(err.as_bytes());
                                    return;
                                }

                                let msg_id = Uuid::new_v4().to_string();
                                let mut attrs = String::new();
                                if vals.len() > 2 {
                                    write!(&mut attrs, "{}", vals[2]).unwrap();
                                }

                                let (tx_rt, rx_rt): (Sender<String>, Receiver<String>) = unbounded();
                                rt.block_on(async {
                                    let mut q = String::new();
                                    write!(&mut q, "insert {} ", MESSAGES_TABLE).unwrap();
                                    write!(&mut q, "(TopicName, Id, Payload, ").unwrap();
                                    write!(&mut q, "Attributes, Created, Updated) ").unwrap();
                                    write!(&mut q, "values ('{}', ", vals[0]).unwrap();
                                    write!(&mut q, "'{}', ", msg_id).unwrap();
                                    write!(&mut q, "'{}', ", vals[1]).unwrap();
                                    write!(&mut q, "'{}', ", attrs).unwrap();
                                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(), ").unwrap();
                                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP())").unwrap();
                                    let stmt = Statement::new(q);
                                    let rwt = client.begin_read_write_transaction().await;
                                    if let Err(e) = rwt {
                                        let mut err = String::new();
                                        write!(&mut err, "{e}").unwrap();
                                        tx_rt.send(err).unwrap();
                                        return;
                                    }

                                    let mut t = rwt.unwrap();
                                    let res = t.update(stmt).await;
                                    let res = t.end(res, None).await;
                                    match res {
                                        Ok(_) => tx_rt.send(String::new()).unwrap(),
                                        Err(e) => {
                                            let mut err = String::new();
                                            write!(&mut err, "{e}").unwrap();
                                            tx_rt.send(err).unwrap();
                                        }
                                    };
                                });

                                let mut broadcast = true;
                                let res = rx_rt.recv().unwrap();
                                let mut ack = String::new();
                                if res.len() != 0 {
                                    write!(&mut ack, "-{res}\n").unwrap();
                                    broadcast = false;
                                } else {
                                    write!(&mut ack, "+{msg_id}\n").unwrap();
                                }

                                let _ = stream.write_all(ack.as_bytes());
                                if !broadcast {
                                    return;
                                }

                                let _ = broadcast_publish_msg(&op_clone, line);
                            }
                            _ => {}
                        }
                    }
                    _ => {} // add here for tasks that need these workers
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

fn broadcast_publish_msg(op: &Arc<Mutex<Op>>, msg: &str) -> Result<()> {
    let (tx, rx): (mpsc::Sender<Broadcast>, mpsc::Receiver<Broadcast>) = mpsc::channel();

    {
        op.lock().unwrap().broadcast(msg.as_bytes().to_vec(), tx)?;
    }

    // Best-effort basis only; do nothing with the replies.
    loop {
        match rx.recv().unwrap() {
            Broadcast::ReplyStream { id, msg, error: _ } => {
                if id == "" || msg.len() == 0 {
                    break;
                }
            }
        }
    }

    Ok(())
}
