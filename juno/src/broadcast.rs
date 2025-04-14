use anyhow::Result;
use std::{fmt::Write as _, sync::mpsc};

// This is our 'broadcast' handler. When a node broadcasts a message, through the broadcast()
// API, we reply here.
pub fn handle_broadcast(node_id: &str, msg: Vec<u8>, tx: mpsc::Sender<Vec<u8>>) -> Result<()> {
    let msg_s = String::from_utf8(msg).unwrap();
    let mut reply = String::new();
    write!(&mut reply, "echo '{msg_s}' from {}", node_id).unwrap();
    tx.send(reply.as_bytes().to_vec()).unwrap();

    Ok(())
}
