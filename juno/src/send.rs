use anyhow::Result;
use std::{fmt::Write as _, sync::mpsc};

// This is our 'send' handler. When we are leader, we reply to all messages coming from other
// nodes using the send() API here.
pub fn handle_toleader(node_id: &str, msg: Vec<u8>, tx: mpsc::Sender<Vec<u8>>) -> Result<()> {
    let msg_s = String::from_utf8(msg).unwrap();
    let mut reply = String::new();
    write!(&mut reply, "echo '{msg_s}' from leader:{}", node_id).unwrap();
    tx.send(reply.as_bytes().to_vec()).unwrap();

    Ok(())
}
