use anyhow::{Result, anyhow};
use crossbeam_channel::{Receiver, Sender, unbounded};
use google_cloud_spanner::{
    client::{Client, ClientConfig},
    statement::Statement,
    value::CommitTimestamp,
};
use log::*;
use std::{fmt::Write as _, time::Instant};
use time::OffsetDateTime;
use tokio::runtime::Runtime;

#[derive(Debug)]
struct DiffToken {
    diff: i64,
    token: i128,
}

#[derive(Debug)]
struct Record {
    name: String,
    heartbeat: i128,
    token: i128,
    writer: String,
}

#[derive(Debug)]
pub enum Ctrl {
    Exit,
    Dummy(Sender<bool>),
    InitialLock(Sender<i128>),
    NextLockInsert { name: String, tx: Sender<i128> },
    NextLockUpdate { token: i128, tx: Sender<i128> },
    CheckLock(Sender<DiffToken>),
    CurrentToken(Sender<Record>),
    Heartbeat(Sender<i128>),
}

// This will be running on a separate thread. Caller thread will be requesting Spanner async calls
// here through ProtoCtrl commands using channels for exchanging information. This is an easier
// approach for allowing our main threads to have access to async function calls. Here, a single
// tokio runtime is being used to block on these async calls.
pub fn caller(db: String, table: String, name: String, id: String, rx_ctrl: Receiver<Ctrl>, tx_ok: Sender<Result<()>>) {
    let rt = Runtime::new().unwrap();
    let (tx, rx): (Sender<Option<Client>>, Receiver<Option<Client>>) = unbounded();
    rt.block_on(async {
        let config = ClientConfig::default().with_auth().await;
        match config {
            Err(_) => tx.send(None).unwrap(),
            Ok(v) => {
                let client = Client::new(db, v).await;
                match client {
                    Err(_) => tx.send(None).unwrap(),
                    Ok(v) => tx.send(Some(v)).unwrap(),
                }
            }
        }
    });

    let read = rx.recv().unwrap();
    if read.is_none() {
        tx_ok.send(Err(anyhow!("client failed"))).unwrap();
        return;
    }

    let client = read.unwrap(); // shouldn't panic
    tx_ok.send(Ok(())).unwrap(); // inform main we're okay

    for code in rx_ctrl {
        let start = Instant::now();

        defer! {
            info!("spanner_caller took {:?}", start.elapsed());
        }

        match code {
            Ctrl::Exit => {
                rt.block_on(async { client.close().await });
                return;
            }
            Ctrl::Dummy(tx) => {
                tx.send(true).unwrap();
            }
            Ctrl::InitialLock(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "insert {} ", table).unwrap();
                    write!(&mut q, "(name, heartbeat, token, writer) ").unwrap();
                    write!(&mut q, "values (").unwrap();
                    write!(&mut q, "'{}',", name).unwrap();
                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(),").unwrap();
                    write!(&mut q, "PENDING_COMMIT_TIMESTAMP(),").unwrap();
                    write!(&mut q, "'{}')", id).unwrap();
                    let stmt = Statement::new(q);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt).await;
                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            debug!("InitialLock commit timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("InitialLock DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("InitialLock reply failed: {e}")
                }

                debug!("InitialLock took {:?}", start.elapsed());
            }
            Ctrl::NextLockInsert { name, tx } => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "insert {} ", table).unwrap();
                    write!(&mut q, "(name) ").unwrap();
                    write!(&mut q, "values ('{}')", name).unwrap();
                    let stmt = Statement::new(q);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt).await;
                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("NextLockInsert DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("NextLockInsert reply failed: {e}")
                }

                debug!("NextLockInsert took {:?}", start.elapsed());
            }
            Ctrl::NextLockUpdate { token, tx } => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "update {} set ", table).unwrap();
                    write!(&mut q, "heartbeat = PENDING_COMMIT_TIMESTAMP(), ").unwrap();
                    write!(&mut q, "token = @token, ").unwrap();
                    write!(&mut q, "writer = @writer ").unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt1 = Statement::new(q);
                    let odt = OffsetDateTime::from_unix_timestamp_nanos(token).unwrap();
                    stmt1.add_param("token", &odt);
                    stmt1.add_param("writer", &id);
                    stmt1.add_param("name", &name);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt1).await;

                    // Best-effort cleanup.
                    let mut q = String::new();
                    write!(&mut q, "delete from {} ", table).unwrap();
                    write!(&mut q, "where starts_with(name, '{}_')", name).unwrap();
                    let stmt2 = Statement::new(q);
                    let _ = t.update(stmt2).await;

                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(e) => {
                            error!("NextLockUpdate DML failed: {e}");
                            tx_in.send(-1).unwrap();
                        }
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("NextLockUpdate reply failed: {e}")
                }

                debug!("NextLockUpdate took {:?}", start.elapsed());
            }
            Ctrl::CheckLock(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<DiffToken>, Receiver<DiffToken>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select ",).unwrap();
                    write!(&mut q, "timestamp_diff(current_timestamp(), ",).unwrap();
                    write!(&mut q, "heartbeat, millisecond) as diff, ",).unwrap();
                    write!(&mut q, "token from {} ", table).unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    let mut empty = true;
                    while let Some(row) = iter.next().await.unwrap() {
                        let d = row.column_by_name::<i64>("diff").unwrap();
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        tx_in
                            .send(DiffToken {
                                diff: d,
                                token: t.unix_timestamp_nanos(),
                            })
                            .unwrap();

                        empty = false;
                        break; // ensure single line
                    }

                    if empty {
                        tx_in.send(DiffToken { diff: 0, token: -1 }).unwrap();
                    }
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("CheckLock reply failed: {e}")
                }

                debug!("CheckLock took {:?}", start.elapsed());
            }
            Ctrl::CurrentToken(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<Record>, Receiver<Record>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "select token, writer, ").unwrap();
                    write!(&mut q, "timestamp_diff(current_timestamp(), ",).unwrap();
                    write!(&mut q, "heartbeat, millisecond) as diff ",).unwrap();
                    write!(&mut q, "from {} where name = @name", table).unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let mut tx = client.single().await.unwrap();
                    let mut iter = tx.query(stmt).await.unwrap();
                    let mut empty = true;
                    while let Some(row) = iter.next().await.unwrap() {
                        let t = row.column_by_name::<CommitTimestamp>("token").unwrap();
                        let w = row.column_by_name::<String>("writer").unwrap();
                        let d = row.column_by_name::<i64>("diff").unwrap();
                        tx_in
                            .send(Record {
                                name: String::from(""),
                                heartbeat: d as i128,
                                token: t.unix_timestamp_nanos(),
                                writer: w,
                            })
                            .unwrap();

                        empty = false;
                        break; // ensure single line
                    }

                    if empty {
                        tx_in
                            .send(Record {
                                name: String::from(""),
                                heartbeat: -1,
                                token: -1,
                                writer: String::from(""),
                            })
                            .unwrap();
                    }
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("CurrentToken reply failed: {e}")
                }

                debug!("CurrentToken took {:?}", start.elapsed());
            }
            Ctrl::Heartbeat(tx) => {
                let start = Instant::now();
                let (tx_in, rx_in): (Sender<i128>, Receiver<i128>) = unbounded();
                rt.block_on(async {
                    let mut q = String::new();
                    write!(&mut q, "update {} ", table).unwrap();
                    write!(&mut q, "set heartbeat = PENDING_COMMIT_TIMESTAMP() ").unwrap();
                    write!(&mut q, "where name = @name").unwrap();
                    let mut stmt = Statement::new(q);
                    stmt.add_param("name", &name);
                    let rwt = client.begin_read_write_transaction().await;
                    if rwt.is_err() {
                        tx.send(-1).unwrap();
                        return;
                    }

                    let mut t = rwt.unwrap();
                    let res = t.update(stmt).await;
                    let res = t.end(res, None).await;
                    match res {
                        Ok(s) => {
                            let ts = s.0.unwrap();
                            let dt = OffsetDateTime::from_unix_timestamp(ts.seconds)
                                .unwrap()
                                .replace_nanosecond(ts.nanos as u32)
                                .unwrap();
                            debug!("Heartbeat commit timestamp: {dt}");
                            tx_in.send(dt.unix_timestamp_nanos()).unwrap();
                        }
                        Err(_) => tx_in.send(-1).unwrap(),
                    };
                });

                if let Err(e) = tx.send(rx_in.recv().unwrap()) {
                    error!("Heartbeat reply failed: {e}")
                }

                debug!("Heartbeat took {:?}", start.elapsed());
            }
        }
    }
}
