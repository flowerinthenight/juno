use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, unbounded};
use google_cloud_spanner::{client::Client, statement::Statement};
use hedge_rs::*;
use regex::Regex;
use std::{
    fmt::Write as _,
    io::prelude::*,
    net::TcpStream,
    sync::{Arc, Mutex, mpsc},
};
use tokio::runtime::Runtime;
use uuid::Uuid;

static TOPICS_TABLE: &'static str = "juno_topics";
static SUBSCRIPTIONS_TABLE: &'static str = "juno_subscriptions";
static MESSAGES_TABLE: &'static str = "juno_messages";

pub fn api_create_topic(rt: &Runtime, mut stream: TcpStream, client: &Client, topic: &str) -> Result<()> {
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

pub fn api_delete_topic(rt: &Runtime, mut stream: TcpStream, client: &Client, topic: &str) -> Result<()> {
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

pub fn api_create_sub(rt: &Runtime, mut stream: TcpStream, client: &Client, line: &str) -> Result<()> {
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

pub fn api_delete_sub(rt: &Runtime, mut stream: TcpStream, client: &Client, sub: &str) -> Result<()> {
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

pub fn api_publish_msg(rt: &Runtime, mut stream: TcpStream, client: &Client, line: &str) -> Result<bool> {
    let vals: Vec<&str> = line.split(" ").collect();
    if vals.len() < 2 {
        let mut err = String::new();
        write!(&mut err, "-Invalid payload format\n")?;
        let _ = stream.write_all(err.as_bytes());
        return Ok(false);
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
        return Ok(false);
    }

    Ok(true)
}

pub fn broadcast_publish_msg(op: &Arc<Mutex<Op>>, msg: &str) -> Result<()> {
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
            }
        }
    }

    Ok(())
}
