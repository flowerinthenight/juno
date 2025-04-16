mod api;
mod broadcast;
mod send;

use anyhow::Result;
use api::*;
use broadcast::*;
use clap::Parser;
use crossbeam_channel::{Receiver, Sender, unbounded};
use ctrlc;
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    statement::Statement,
};
use hedge_rs::*;
use log::*;
use send::*;
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
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;

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
    #[arg(long, long, default_value = "--db")]
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
    GetMeta(Sender<HashMap<String, Vec<Subscription>>>),
    Broadcast {
        name: String,
        msg: Vec<u8>,
        tx: Sender<Vec<u8>>,
    },
}

struct Subscription {
    name: String,
    ack_timeout: i64,
    auto_extend: bool,
}

struct Message {
    id: String,
    data: String,
    attrs: String,
}

struct Meta {
    subs: Vec<Subscription>,
    msgs: Vec<Message>,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let (tx_ctrlc, rx_ctrlc) = mpsc::channel();
    ctrlc::set_handler(move || tx_ctrlc.send(()).unwrap())?;

    let mut db_hedge = String::new();
    if args.db_hedge == "--db" {
        db_hedge = args.db.clone();
    }

    info!(
        "starting node:{}, api={}, db={}, db-hedge={}, table={}, lockname={}",
        &args.id, &args.api, &args.db, &db_hedge, &args.table, &args.name
    );

    // Key is topic name, value is metadata for the associated subscriptions and messages.
    let tm: Arc<Mutex<HashMap<String, Arc<Mutex<Meta>>>>> = Arc::new(Mutex::new(HashMap::new()));

    let leader = Arc::new(AtomicUsize::new(0)); // for leader state change callback

    // We will use this channel for the 'send' and 'broadcast' features.
    // Use Sender as inputs, then we read replies through the Receiver.
    let (tx_op, rx_op): (mpsc::Sender<Comms>, mpsc::Receiver<Comms>) = mpsc::channel();

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
    let leader_cb = leader.clone();
    thread::spawn(move || {
        loop {
            match rx_op.recv() {
                Err(_) => continue,
                Ok(v) => match v {
                    Comms::ToLeader { msg, tx } => {
                        let _ = handle_toleader(&args.id, msg, tx, &tm, &leader_cb);
                    }
                    Comms::Broadcast { msg, tx } => {
                        let _ = handle_broadcast(&args.id, msg, tx, &tm, &leader_cb);
                    }
                    Comms::OnLeaderChange(state) => {
                        leader_cb.store(state, Ordering::Relaxed);
                    }
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

    // We use a single Tokio runtime.
    let a_rt = Arc::new(Runtime::new()?);
    let mut v_rt = vec![];
    for _ in 0..cpus {
        v_rt.push(a_rt.clone());
    }

    // Start our API worker threads.
    for i in 0..v_rt.len() {
        let rxc = rxh.clone();
        let db_work = args.db.clone();
        let op_work = op.clone();
        let rt = v_rt[i].clone();
        thread::spawn(move || {
            let (tx, rx): (Sender<Option<Client>>, Receiver<Option<Client>>) = unbounded();
            rt.block_on(async {
                let config = ClientConfig::default().with_auth().await;
                match config {
                    Err(_) => tx.send(None).unwrap(),
                    Ok(v) => {
                        let client = Client::new(db_work, v).await;
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
                    WorkerCtrl::HandleApi(stream) => {
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
                                let topic = &data[1..&data.len() - 1];
                                let _ = api_create_topic(i, &rt, stream, &client, topic);
                            }
                            //
                            // %<topic-name>\n
                            //
                            // Delete a topic. Deleting a topic also deletes all associated
                            // subscriptions and messages.
                            //
                            "%" => {
                                let topic = &data[1..&data.len() - 1];
                                let _ = api_delete_topic(i, &rt, stream, &client, topic);
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
                                let line = &data[1..&data.len() - 1];
                                let _ = api_create_sub(i, &rt, stream, &client, line);
                            }
                            //
                            // !<subscription-name>\n
                            //
                            // Delete a subscription.
                            //
                            "!" => {
                                let sub = &data[1..&data.len() - 1];
                                let _ = api_delete_sub(i, &rt, stream, &client, sub);
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
                                let line = &data[1..&data.len() - 1];
                                if let Ok(bc) = api_publish_msg(i, &rt, stream, &client, line) {
                                    if bc {
                                        let _ = broadcast_publish_msg(&op_work, line);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    WorkerCtrl::GetMeta(tx) => {
                        let (tx_rt, rx_rt): (Sender<Subscription>, Receiver<Subscription>) = unbounded();
                        rt.block_on(async {
                            let mut q = String::new();
                            write!(&mut q, "select TopicName, SubscriptionName, ").unwrap();
                            write!(&mut q, "AcknowledgeTimeout, AutoExtend ").unwrap();
                            write!(&mut q, "from {}", SUBSCRIPTIONS_TABLE).unwrap();
                            let stmt = Statement::new(q);
                            let mut tx = client.single().await.unwrap();
                            let mut iter = tx.query(stmt).await.unwrap();
                            while let Some(row) = iter.next().await.unwrap() {
                                let t = row.column_by_name::<String>("TopicName").unwrap();
                                let s = row.column_by_name::<String>("SubscriptionName").unwrap();
                                let a = row.column_by_name::<i64>("AcknowledgeTimeout").unwrap();
                                let x = row.column_by_name::<bool>("AutoExtend").unwrap();
                                let ts = format!("{}/{}", t, s);
                                tx_rt
                                    .send(Subscription {
                                        name: ts,
                                        ack_timeout: a,
                                        auto_extend: x,
                                    })
                                    .unwrap();
                            }

                            tx_rt
                                .send(Subscription {
                                    name: "".to_string(),
                                    ack_timeout: 0,
                                    auto_extend: false,
                                })
                                .unwrap();
                        });

                        let mut hm: HashMap<String, Vec<Subscription>> = HashMap::new();

                        loop {
                            let sub = rx_rt.recv().unwrap();
                            if sub.name == "" {
                                break;
                            }

                            let ts: Vec<&str> = sub.name.split("/").collect();
                            if hm.contains_key(ts[0]) {
                                let v = hm.get_mut(ts[0]).unwrap();
                                v.push(Subscription {
                                    name: ts[1].to_string(),
                                    ack_timeout: sub.ack_timeout,
                                    auto_extend: sub.auto_extend,
                                });
                            } else {
                                hm.insert(
                                    ts[0].to_string(),
                                    vec![Subscription {
                                        name: ts[1].to_string(),
                                        ack_timeout: sub.ack_timeout,
                                        auto_extend: sub.auto_extend,
                                    }],
                                );
                            }
                        }

                        tx.send(hm).unwrap();
                    }
                    _ => {} // add here for tasks that need these workers
                }
            }
        });
    }

    // Starts a new thread for the API.
    let tx_api = tx_work.clone();
    thread::spawn(move || {
        let listen = TcpListener::bind(&args.api).unwrap();
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

    // Start a new thread for leader to broadcast changes to topic/subs and messages.
    let tx_ldr = tx_work.clone();
    thread::spawn(move || {
        loop {
            defer! {
                thread::sleep(Duration::from_secs(5));
            }

            if leader.load(Ordering::Acquire) == 0 {
                continue;
            }

            let (tx, rx): (
                Sender<HashMap<String, Vec<Subscription>>>,
                Receiver<HashMap<String, Vec<Subscription>>>,
            ) = unbounded();

            tx_ldr.send(WorkerCtrl::GetMeta(tx)).unwrap();
            let hm = rx.recv().unwrap();
            for (topic, meta) in hm.iter() {
                info!("topic: {}", topic);
                for sub in meta {
                    info!("  meta.sub.name: {}", sub.name);
                    info!("  meta.sub.ack_timeout: {}", sub.ack_timeout);
                    info!("  meta.sub.auto_extend: {}", sub.auto_extend);
                    info!("  ---");
                }
            }
        }
    });

    rx_ctrlc.recv()?; // wait for Ctrl-C

    {
        op.lock().unwrap().close();
    }

    Ok(())
}
