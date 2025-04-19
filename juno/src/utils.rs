use crate::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn insert_message(
    tm: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Message>>>>>>,
    topic: &str,
    data: String,
    attrs: String,
) {
    let mut tm_guard = tm.lock().unwrap();
    let meta = tm_guard
        .entry(topic.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(vec![])));
    let mut meta_guard = meta.lock().unwrap();
    meta_guard.push(Message {
        id: uuid::Uuid::new_v4().to_string(),
        data,
        attrs,
    });
}

pub fn delete_messages(tm: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Message>>>>>>, topic: &str) {
    let mut tm_guard = tm.lock().unwrap();
    tm_guard.remove(topic);
}
