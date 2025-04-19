use std::{
    collections::HashMap,
    fmt::Write as _,
    io::prelude::*,
    net::TcpStream,
    ptr::write_bytes,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicBool},
        mpsc,
    },
    time::Instant,
};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, unbounded};
use google_cloud_spanner::{client::Client, statement::Statement};
use hedge_rs::*;
use log::*;
use regex::Regex;
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::{Message, MessageMeta, Subscription, utils::get_all_subs_for_topic};

pub static TOPICS_TABLE: &'static str = "juno_topics";
pub static SUBSCRIPTIONS_TABLE: &'static str = "juno_subscriptions";
pub static MESSAGES_TABLE: &'static str = "juno_messages";

pub fn api_create_topic(i: usize, rt: &Runtime, mut stream: TcpStream, client: &Client, topic: &str) -> Result<()> {
    let start = Instant::now();

    defer! {
        info!("[T{i}]: api_create_topic took {:?}", start.elapsed());
    }

    let re = Regex::new(r"^[a-zA-Z]+[a-zA-Z0-9-]+[a-zA-Z0-9]$")?;
    if !re.is_match(topic) {
        let mut err = String::new();
        write!(&mut err, "-Invalid topic name\n")?;
        let _ = stream.write_all(err.as_bytes());
        return Ok(());
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

    let res = rx_rt.recv()?;
    let mut ack = String::new();
    if res.len() != 0 {
        write!(&mut ack, "-{res}\n")?;
    } else {
        write!(&mut ack, "+OK\n")?;
    }

    let _ = stream.write_all(ack.as_bytes());

    Ok(())
}

pub fn api_delete_topic(i: usize, rt: &Runtime, mut stream: TcpStream, client: &Client, topic: &str) -> Result<()> {
    let start = Instant::now();

    defer! {
        info!("[T{i}]: api_delete_topic took {:?}", start.elapsed());
    }

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

    let res = rx_rt.recv()?;
    let mut ack = String::new();
    if res.len() != 0 {
        write!(&mut ack, "-{res}\n")?;
    } else {
        write!(&mut ack, "+OK\n")?;
    }

    let _ = stream.write_all(ack.as_bytes());

    Ok(())
}

pub fn api_create_sub(i: usize, rt: &Runtime, mut stream: TcpStream, client: &Client, line: &str) -> Result<()> {
    let start = Instant::now();

    defer! {
        info!("[T{i}]: api_create_sub took {:?}", start.elapsed());
    }

    let vals: Vec<&str> = line.split(" ").collect();
    if vals.len() < 2 {
        let mut err = String::new();
        write!(&mut err, "-Invalid payload format\n")?;
        let _ = stream.write_all(err.as_bytes());
        return Ok(());
    }

    let re = Regex::new(r"^[a-zA-Z]+[a-zA-Z0-9-]+[a-zA-Z0-9]$")?;
    if !re.is_match(vals[0]) {
        let mut err = String::new();
        write!(&mut err, "-Invalid topic name\n")?;
        let _ = stream.write_all(err.as_bytes());
        return Ok(());
    }

    if !re.is_match(vals[1]) {
        let mut err = String::new();
        write!(&mut err, "-Invalid subscription name\n")?;
        let _ = stream.write_all(err.as_bytes());
        return Ok(());
    }

    let mut ack_timeout = 60;
    let mut auto_extend = true;
    for i in 2..vals.len() {
        let prop = vals[i].split("=").collect::<Vec<&str>>();
        if prop.len() != 2 {
            let mut err = String::new();
            write!(&mut err, "-Invalid property format\n")?;
            let _ = stream.write_all(err.as_bytes());
            return Ok(());
        }

        match prop[0] {
            "AcknowledgeTimeout" => {
                let val = prop[1].parse::<i64>();
                if val.is_err() {
                    let mut err = String::new();
                    write!(&mut err, "-Invalid AcknowledgeTimeout value\n")?;
                    let _ = stream.write_all(err.as_bytes());
                    return Ok(());
                }
                ack_timeout = val? as i64;
            }
            "AutoExtend" => {
                let val = prop[1].parse::<bool>();
                if val.is_err() {
                    let mut err = String::new();
                    write!(&mut err, "-Invalid AutoExtend value\n")?;
                    let _ = stream.write_all(err.as_bytes());
                    return Ok(());
                }
                auto_extend = val? as bool;
            }
            _ => {
                let mut err = String::new();
                write!(&mut err, "-Unknown property\n")?;
                let _ = stream.write_all(err.as_bytes());
                return Ok(());
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

    let res = rx_rt.recv()?;
    let mut ack = String::new();
    if res.len() != 0 {
        write!(&mut ack, "-{res}\n")?;
    } else {
        write!(&mut ack, "+OK\n")?;
    }

    let _ = stream.write_all(ack.as_bytes());

    Ok(())
}

pub fn api_delete_sub(i: usize, rt: &Runtime, mut stream: TcpStream, client: &Client, sub: &str) -> Result<()> {
    let start = Instant::now();

    defer! {
        info!("[T{i}]: api_delete_sub took {:?}", start.elapsed());
    }

    let (tx_rt, rx_rt): (Sender<String>, Receiver<String>) = unbounded();
    rt.block_on(async {
        let mut q = String::new();
        write!(&mut q, "delete from {} ", SUBSCRIPTIONS_TABLE).unwrap();
        write!(&mut q, "where SubscriptionName = '{}'", sub).unwrap();
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

    let res = rx_rt.recv()?;
    let mut ack = String::new();
    if res.len() != 0 {
        write!(&mut ack, "-{res}\n")?;
    } else {
        write!(&mut ack, "+OK\n")?;
    }

    let _ = stream.write_all(ack.as_bytes());

    Ok(())
}

pub fn api_publish_msg(i: usize, rt: &Runtime, mut stream: TcpStream, client: &Client, line: &str) -> Result<String> {
    let start = Instant::now();

    defer! {
        info!("[T{i}]: api_publish_msg took {:?}", start.elapsed());
    }

    let vals: Vec<&str> = line.split(" ").collect();
    if vals.len() < 2 {
        let mut err = String::new();
        write!(&mut err, "-Invalid payload format\n")?;
        let _ = stream.write_all(err.as_bytes());
        return Ok(String::new());
    }

    let msg_id = Uuid::new_v4().to_string();
    let mut attrs = String::new();
    if vals.len() > 2 {
        write!(&mut attrs, "{}", vals[2])?;
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
    let res = rx_rt.recv()?;
    let mut ack = String::new();
    if res.len() != 0 {
        write!(&mut ack, "-{res}\n")?;
        broadcast = false;
    } else {
        write!(&mut ack, "+{msg_id}\n")?;
    }

    let _ = stream.write_all(ack.as_bytes());
    if !broadcast {
        return Ok(String::new());
    }

    Ok(msg_id)
}

pub fn broadcast_publish_msg(op: &Arc<Mutex<Op>>, msg: &str) -> Result<()> {
    info!("Broadcasting message: {}", msg);
    let (tx, rx): (mpsc::Sender<Broadcast>, mpsc::Receiver<Broadcast>) = mpsc::channel();

    {
        op.lock().unwrap().broadcast(msg.as_bytes().to_vec(), tx)?;
    }

    // Best-effort basis only; do nothing with the replies.
    loop {
        match rx.recv()? {
            Broadcast::ReplyStream { id, msg, error: _ } => {
                if id == "" || msg.len() == 0 {
                    break;
                }
                println!("Broadcast reply: {}", String::from_utf8(msg)?);
            }
        }
    }

    Ok(())
}

pub fn fetch_all_msgs_then_broadcast(
    op: &Arc<Mutex<Op>>,
    client: &Client,
    rt: &Runtime,
    ts: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Subscription>>>>>>,
) -> Result<()> {
    rt.block_on(async {
        let mut q = String::new();
        write!(&mut q, "select TopicName, Id, ").unwrap();
        write!(&mut q, "Payload, Attributes ").unwrap();
        write!(&mut q, "from {} where Acknowledged = False", MESSAGES_TABLE).unwrap();
        let stmt = Statement::new(q);
        let mut tx = client.single().await.unwrap();
        let mut iter = tx.query(stmt).await.unwrap();
        while let Some(row) = iter.next().await.unwrap() {
            let tn = row.column_by_name::<String>("TopicName").unwrap();
            let id = row.column_by_name::<String>("Id").unwrap();
            let p = row.column_by_name::<i64>("Payload").unwrap();
            let at = row.column_by_name::<bool>("Attributes").unwrap();

            let mut fsubs: Vec<MessageMeta> = vec![];
            let subs = get_all_subs_for_topic(&tn, ts);
            for s in subs.iter() {
                let to_append = MessageMeta {
                    acknowledged: AtomicBool::new(false),
                    locked: AtomicBool::new(false),
                    subscription: s.name.clone(),
                };
                fsubs.push(to_append);
            }
            let d = Message {
                id,
                data: p.to_string(),
                attrs: at.to_string(),
                meta: fsubs,
                final_deleted: atomic::AtomicBool::new(false),
            };
        }
    });
    Ok(())
}
