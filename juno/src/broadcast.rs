use crate::Meta;
use anyhow::Result;
use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{Arc, Mutex, atomic::AtomicUsize, mpsc},
};

// This is our 'broadcast' handler. When a node broadcasts a
// message, through the broadcast() API, we reply here.
pub fn handle_broadcast(
    node_id: &str,
    msg: Vec<u8>,
    tx: mpsc::Sender<Vec<u8>>,
    _tm: &Arc<Mutex<HashMap<String, Arc<Mutex<Meta>>>>>,
    _leader: &Arc<AtomicUsize>,
) -> Result<()> {
    let msg_s = String::from_utf8(msg)?;
    let mut reply = String::new();
    write!(&mut reply, "echo '{msg_s}' from {}", node_id)?;
    tx.send(reply.as_bytes().to_vec())?;

    Ok(())
}
