use rustler::resource::ResourceArc;
use rustler::{Encoder, Env, Error, Term};
use std::path::Path;
use std::sync::mpsc;
use std::thread;

mod atoms {
    rustler::rustler_atoms! {
        atom ok;
        atom error;
        atom done;
        atom not_found;
        //atom __true__ = "true";
        //atom __false__ = "false";
    }
}

mod wrapper;
use wrapper::Wrapper;

rustler::rustler_export_nifs! {
    "Elixir.Anchor",
    [
        // ("test", 1, test),
        // ("open_ctx", 1, open_ctx),
        ("open_env", 1, open_env),
        ("txn_write_new", 1, txn_write_new),
        ("txn_write_commit", 1, txn_write_commit),
        ("txn_read_new", 1, txn_read_new),
        ("txn_read_abort", 1, txn_read_abort),
        ("get", 2, get),
        ("put", 3, put),
        ("range", 3, range),
        ("range_next", 1, range_next),
        ("range_take", 2, range_take),
        ("range_abort", 1, range_abort),
    ],
    Some(on_init)
}

enum Reply {
    Ok {},
    Value { value: String },
    KeyValue { key: String, value: String },
    KeyValueBatch { results: Vec<(String, String)> },
    Done {},
    NotFound {},
}

impl Reply {
    pub fn encode<'a>(&self, env: Env<'a>) -> Result<Term<'a>, Error> {
        match self {
            Reply::Ok {} => Ok(atoms::ok().encode(env)),
            Reply::Value { value } => Ok((atoms::ok(), value).encode(env)),
            Reply::KeyValue { key, value } => Ok((atoms::ok(), (key, value)).encode(env)),
            Reply::KeyValueBatch { results } => Ok((atoms::ok(), results).encode(env)),
            Reply::Done {} => Ok(atoms::done().encode(env)),
            Reply::NotFound {} => Ok((atoms::error(), atoms::not_found()).encode(env)),
        }
    }
}

struct ReplySender(mpsc::SyncSender<Reply>);
struct ReplyReceiver(mpsc::Receiver<Reply>);
struct ReplyChannel();
impl ReplyChannel {
    pub fn new() -> (ReplySender, ReplyReceiver) {
        let (tx, rx) = mpsc::sync_channel::<Reply>(0);
        (ReplySender(tx), ReplyReceiver(rx))
    }
}

enum Command {
    Range {
        start: String,
        end: String,
        receiver: CommandReceiver,
    },
    Put {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Next {},
    Take {
        count: usize,
    },
    Done {},
}

struct CommandSender(mpsc::SyncSender<(Command, ReplySender)>);
struct CommandReceiver(mpsc::Receiver<(Command, ReplySender)>);
struct CommandChannel();
impl CommandChannel {
    pub fn new() -> (CommandSender, CommandReceiver) {
        let (tx, rx) = mpsc::sync_channel::<(Command, ReplySender)>(0);
        (CommandSender(tx), CommandReceiver(rx))
    }
}

fn on_init<'a>(env: Env<'a>, _load_info: Term<'a>) -> bool {
    rustler::resource_struct_init!(Wrapper<lmdb_rs::Environment>, env);
    rustler::resource_struct_init!(CommandSender, env);
    true
}

fn open_env<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let path: &str = args[0].decode()?;
    let environment = lmdb_rs::Environment::new()
        .autocreate_dir(true)
        .map_size(60000000000)
        .open(Path::new(path), 0o600)
        .unwrap();
    environment
        .get_default_db(lmdb_rs::DbFlags::empty())
        .unwrap();
    Ok((atoms::ok(), ResourceArc::new(Wrapper::new(environment))).encode(env))
}

fn txn_write_new<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapper: ResourceArc<Wrapper<lmdb_rs::Environment>> = args[0].decode()?;
    let (tx, rx) = CommandChannel::new();
    let (rtx, rrx) = ReplyChannel::new();
    thread::spawn(move || {
        let txn = wrapper.value.new_transaction().unwrap();
        rtx.0.send(Reply::Ok {}).unwrap();
        let handle = wrapper
            .value
            .get_default_db(lmdb_rs::DbFlags::empty())
            .unwrap();
        let db = txn.bind(&handle);
        let reply = process(db, rx).unwrap();
        txn.commit().unwrap();
        reply.0.send(Reply::Ok {}).unwrap();
    });
    rrx.0.recv().unwrap();
    Ok((atoms::ok(), ResourceArc::new(tx)).encode(env))
}

fn txn_write_commit<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    let cmd = Command::Done {};
    call(tx, cmd).encode(env)
}

fn txn_read_new<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let wrapper: ResourceArc<Wrapper<lmdb_rs::Environment>> = args[0].decode()?;
    let (tx, rx) = CommandChannel::new();
    thread::spawn(move || {
        let mut txn = wrapper.value.get_reader().unwrap();
        let handle = wrapper
            .value
            .get_default_db(lmdb_rs::DbFlags::empty())
            .unwrap();
        let db = txn.bind(&handle);
        let reply = process(db, rx).unwrap();
        txn.abort();
        reply.0.send(Reply::Ok {}).unwrap();
    });
    Ok((atoms::ok(), ResourceArc::new(tx)).encode(env))
}

fn txn_read_abort<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    let cmd = Command::Done {};
    call(tx, cmd).encode(env)
}

fn range<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    let start: &str = args[1].decode()?;
    let end: &str = args[2].decode()?;
    let (rtx, rrx) = CommandChannel::new();
    let cmd = Command::Range {
        start: start.to_string(),
        end: end.to_string(),
        receiver: rrx,
    };
    call(tx, cmd);
    Ok((atoms::ok(), ResourceArc::new(rtx)).encode(env))
}
fn range_next<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    call(tx, Command::Next {}).encode(env)
}

fn range_take<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    let number: usize = args[1].decode()?;
    call(tx, Command::Take { count: number }).encode(env)
}

fn range_abort<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    call(tx, Command::Done {}).encode(env)
}

fn put<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    let key: &str = args[1].decode()?;
    let value: &str = args[2].decode()?;
    let cmd = Command::Put {
        key: key.to_string(),
        value: value.to_string(),
    };
    call(tx, cmd).encode(env)
}

fn get<'a>(env: Env<'a>, args: &[Term<'a>]) -> Result<Term<'a>, Error> {
    let tx: ResourceArc<CommandSender> = args[0].decode()?;
    let key: &str = args[1].decode()?;
    call(
        tx,
        Command::Get {
            key: key.to_string(),
        },
    )
    .encode(env)
}

fn call(sender: ResourceArc<CommandSender>, cmd: Command) -> Reply {
    let (reply, reply_wait) = ReplyChannel::new();
    sender.0.send((cmd, reply)).unwrap();
    reply_wait.0.recv().unwrap()
}

fn process(db: lmdb_rs::Database, rx: CommandReceiver) -> Option<ReplySender> {
    for cmd in rx.0 {
        match cmd {
            (Command::Get { key }, reply) => match db.get::<&str>(&key) {
                Ok(value) => {
                    reply
                        .0
                        .send(Reply::Value {
                            value: value.to_string(),
                        })
                        .unwrap();
                }
                _ => {
                    reply.0.send(Reply::NotFound {}).unwrap();
                }
            },
            (Command::Put { key, value }, reply) => {
                db.set(&key, &value).unwrap();
                reply.0.send(Reply::Ok {}).unwrap();
            }
            (
                Command::Range {
                    start,
                    end,
                    receiver,
                },
                reply,
            ) => {
                let mut cursor = db.keyrange_from_to(&start, &end).unwrap();
                reply.0.send(Reply::Ok {}).unwrap();
                for cursor_cmd in receiver.0 {
                    match cursor_cmd {
                        (Command::Next {}, cursor_reply) => match cursor.next() {
                            Some(next) => {
                                cursor_reply
                                    .0
                                    .send(Reply::KeyValue {
                                        key: next.get_key::<&str>().to_string(),
                                        value: next.get_value::<&str>().to_string(),
                                    })
                                    .unwrap();
                            }
                            _ => {
                                cursor_reply.0.send(Reply::Done {}).unwrap();
                                break;
                            }
                        },
                        (Command::Take { count }, cursor_reply) => {
                            let mut results = vec![];
                            for _n in 0..count {
                                match cursor.next() {
                                    Some(next) => {
                                        results.push((
                                            next.get_key::<&str>().to_string(),
                                            next.get_value::<&str>().to_string(),
                                        ));
                                    }
                                    _ => {
                                        break;
                                    }
                                }
                            }
                            if results.len() == 0 {
                                cursor_reply.0.send(Reply::Done {}).unwrap();
                                break;
                            }
                            cursor_reply
                                .0
                                .send(Reply::KeyValueBatch { results: results })
                                .unwrap();
                        }
                        (Command::Done {}, cursor_reply) => {
                            cursor_reply.0.send(Reply::Ok {}).unwrap();
                            break;
                        }
                        _ => continue,
                    }
                }
            }
            (Command::Done {}, reply) => {
                return Some(reply);
            }
            _ => {
                continue;
            }
        }
    }
    return None;
}
