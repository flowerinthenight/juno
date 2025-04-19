use crate::{Message, MessageMeta, Subscription};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

pub fn insert_message(
    tm: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Message>>>>>>,
    id: String,
    topic: String,
    data: String,
    attrs: String,
    meta: Vec<MessageMeta>,
) {
    let mut tm_guard = tm.lock().unwrap();
    let m = tm_guard
        .entry(topic.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(vec![])));
    let mut meta_guard = m.lock().unwrap();
    meta_guard.push(Message {
        id,
        data,
        attrs,
        meta: meta,
        final_deleted: AtomicBool::new(false),
    });
}

pub fn delete_messages(tm: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Message>>>>>>, topic: &str) {
    let mut tm_guard = tm.lock().unwrap();
    tm_guard.remove(topic);
}

pub fn get_all_subs_for_topic(
    topic: &String,
    ts: &Arc<Mutex<HashMap<String, Arc<Mutex<Vec<Subscription>>>>>>,
) -> Vec<Subscription> {
    let t = ts.lock().unwrap();
    let subs = t.get(topic).unwrap();
    let tt = subs.lock().unwrap();
    tt.to_vec()
}
