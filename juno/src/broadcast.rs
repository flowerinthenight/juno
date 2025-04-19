use crate::utils::get_all_subs_for_topic;
use crate::utils::insert_message;
use crate::{Message, MessageMeta, Subscription};

use std::sync::atomic::AtomicBool;
use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{Arc, Mutex, atomic::AtomicUsize, mpsc},
};

use anyhow::Result;

static NEWMSG: &str = "NM"; // New message published

// This is our 'broadcast' handler. When a node broadcasts a
// message, through the broadcast() API, we reply here.
pub async fn handle_broadcast(
    node_id: &str,
    msg: Vec<u8>,
    tx: mpsc::Sender<Vec<u8>>,
    ts: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Subscription>>>>>>,
    tm: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Message>>>>>>,
    _leader: &Arc<AtomicUsize>,
) -> Result<()> {
    let tm = tm.clone();

    let msg_s = String::from_utf8(msg)?;
    let mut reply = String::new();
    write!(&mut reply, "echo '{msg_s}' from {}", node_id)?;

    let parts: Vec<&str> = msg_s.split(" ").collect();

    match parts.first() {
        // Store message in memory
        Some(&cmd) if cmd == NEWMSG => {
            let mut parts = msg_s.splitn(3, ' ');
            let msg_id = match parts.next() {
                Some(id) => id.to_string(),
                None => return Ok(()),
            };

            let topic = match parts.next() {
                Some(t) => t.to_string(),
                None => return Ok(()),
            };
            let data = match parts.next() {
                Some(d) => d.to_string(),
                None => return Ok(()),
            };
            let attrs = parts.next().unwrap_or("").to_string();
            let mut fmeta: Vec<MessageMeta> = vec![];
            let subs = get_all_subs_for_topic(&topic, ts);
            for s in subs.iter() {
                let to_append = MessageMeta {
                    acknowledged: AtomicBool::new(false),
                    locked: AtomicBool::new(false),
                    subscription: s.name.clone(),
                };
                fmeta.push(to_append);
            }
            insert_message(&tm, msg_id, topic, data, attrs, fmeta);
        }
        Some(_) => {}
        None => {
            // No command found, do nothing
            print!("No Command {}: {}", node_id, msg_s);
        }
    }

    tx.send(reply.as_bytes().to_vec())?;
    Ok(())
}
