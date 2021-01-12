use futures::{
    future::{select, Either, FutureExt},
    pin_mut,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::process::Stdio;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::process::{Child, Command};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net,
    runtime::Runtime,
};

struct WaitUntil<F: Future> {
    pred: fn(F::Output) -> bool,
    fut: Vec<Pin<Box<F>>>,
}

impl<F: Future> WaitUntil<F> {
    #[allow(dead_code)]
    fn new(pred: fn(F::Output) -> bool) -> WaitUntil<F> {
        WaitUntil {
            pred,
            fut: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn push(&mut self, f: F) {
        self.fut.push(Box::pin(f))
    }
}

impl<F: Future> Future for WaitUntil<F> {
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut found = false;
        let mut idx = 0;
        let mut evaluated = Vec::new();
        let pred = self.pred; // cannot access self.pred in the iterator

        // Find all futures that have been evaluated
        for f in self.fut.iter_mut() {
            if let Poll::Ready(e) = f.poll_unpin(cx) {
                if !found {
                    found = (pred)(e)
                };
                evaluated.push(idx);
            }
            idx += 1;
        }

        // Book-keeping: Remove fully evaluated futures
        for e in evaluated.into_iter().rev() {
            self.fut.remove(e); // not e*O(1)
        }

        // Return result
        match found {
            true => Poll::Ready(found),
            false => Poll::Pending,
        }
    }
}

struct Process {
    name: String,
    _rank: usize,
    val: Option<Child>,
}

/// Read the environment variable RSRUN_RANK as an integer, or return
/// 0 if such a value does not exist.
fn rank() -> usize {
    if let Ok(rankstr) = std::env::var("RSRUN_RANK") {
        match rankstr.parse::<usize>() {
            Ok(rank) => rank,
            _ => 0,
        }
    } else {
        0
    }
}

/// Return a unique process identifier for the current executable,
/// consisting of a host name and the system process identifier.
fn pid() -> String {
    format!(
        "{}:{}",
        dns_lookup::get_hostname().unwrap_or("(unknown)".to_string()),
        std::process::id()
    )
}

/// Spawn this very application exactly once on the given remote host.
/// On the remote host the environment variable RSRUN_RANK will be set
/// to the given integer.
fn spawn_self(host: &str, rank: usize) -> Option<Process> {
    let exe = std::env::current_exe().ok()?;
    let fd = std::fs::File::open(exe).ok()?;
    let cmd = format!(
        "{} && RSRUN_RANK={} {}",
        "exe=$(/bin/mktemp) && /bin/cat > $exe",
        rank,
        "/lib64/ld-linux-x86-64.so.2 $exe && /bin/rm $exe"
    );
    let fwd = "-R 3636:127.0.0.1:3636";
    let val = Command::new("ssh")
        .arg(fwd)
        .arg(host)
        .arg(cmd)
        .stdin(fd)
        .stdout(Stdio::piped())
        .spawn()
        .ok();
    Some(Process {
        name: host.to_string(),
        _rank: rank,
        val,
    })
}

#[allow(dead_code)]
/// Spawn this very application on the given hosts. The binary is
/// first transferred via SSH to the remote host and then started.
fn run_on(hosts: &[&str]) -> Vec<Process> {
    hosts
        .iter()
        .enumerate()
        .filter_map(|(rank, host)| spawn_self(host, rank + 1))
        .collect()
}

#[allow(dead_code)]
/// Spawn this very application several times as separate processes on
/// the local host. The application binary is being determined by
/// looking at the file descriptors of the running program.
fn run_local(nproc: usize) -> Vec<Process> {
    (1..=nproc)
        .map(|rank| {
            let val = std::env::current_exe().ok().and_then(|exe| {
                Command::new(&exe)
                    .env("RSRUN_RANK", rank.to_string())
                    .stdout(Stdio::piped())
                    .spawn()
                    .ok()
            });
            Process {
                name: format!("child {}", rank),
                _rank: rank,
                val,
            }
        })
        .collect()
}

#[derive(PartialEq, Deserialize, Serialize, Debug)]
enum Proceed {
    Quit,
    Resume,
}

#[derive(Deserialize, Serialize, Debug)]
struct Message {
    sender: String,
    rank: usize,
    proceed: Proceed,
    payload: String,
}

async fn rsrun_quit(stream: &mut net::TcpStream) {
    let (_, mut tx) = stream.split();
    let msg = Message {
        sender: pid(),
        rank: rank(),
        proceed: Proceed::Quit,
        payload: String::new(),
    };
    if let Ok(encoded) = bincode::serialize(&msg) {
        tx.write_all(&encoded).await.unwrap_or(())
    }
}

async fn rsrun_send(stream: &mut net::TcpStream, s: &str) {
    let (_, mut tx) = stream.split();
    let msg = Message {
        sender: pid(),
        rank: rank(),
        proceed: Proceed::Resume,
        payload: s.to_string(),
    };
    if let Ok(encoded) = bincode::serialize(&msg) {
        tx.write_all(&encoded).await.unwrap_or(())
    }
}

async fn rsrun_recv(stream: &mut net::TcpStream) -> Option<Message> {
    let mut buffer = Vec::with_capacity(16);
    loop {
        let mut peak_buffer = [0; 16];
        let peaked_bytes = stream.peek(&mut peak_buffer).await.ok()?;
        if peaked_bytes == 0 {
            return None;
        }
        buffer.extend_from_slice(&peak_buffer[..peaked_bytes]);
        match bincode::deserialize::<Message>(&buffer) {
            Ok(message) => {
                let message_size = bincode::serialized_size(&message).ok()? as usize;
                let bytes_to_leave = buffer.len() - message_size;
                let bytes_to_discard = peaked_bytes - bytes_to_leave;
                let mut void = vec![0; bytes_to_discard];
                stream.read_exact(&mut void[..]).await.ok()?;
                return Some(message);
            }
            Err(kind) => match *kind {
                bincode::ErrorKind::Io(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        let bytes_to_discard = peaked_bytes;
                        let mut void = vec![0; bytes_to_discard];
                        stream.read_exact(&mut void[..]).await.ok()?;
                    }
                    _ => {
                        return None;
                    }
                },
                _ => {
                    return None;
                }
            },
        };
    }
}

async fn handle_slave(stream: &mut net::TcpStream) {
    match rank() {
        2 => {
            rsrun_send(stream, "need some computing to do...").await;
            rsrun_send(stream, "got my message?").await;
            std::thread::sleep(std::time::Duration::from_millis(2000));
            rsrun_send(stream, "computation done!").await;
            rsrun_quit(stream).await;
        }
        3 => {
            std::thread::sleep(std::time::Duration::from_millis(3000));
            rsrun_quit(stream).await;
        }
        _ => rsrun_send(stream, "hello").await,
    };
}

async fn slave() {
    let master = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 3636);
    if let Ok(mut stream) = net::TcpStream::connect(master).await {
        handle_slave(&mut stream).await;
    }
}

async fn bind_socket(port: u16) -> Option<net::TcpListener> {
    let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);
    net::TcpListener::bind(addr).await.ok()
}

/// How the master process reacts to client's messages is determined
/// here. The return value of the function should reflect whether the
/// computation needs to resume.
async fn handle_master(mut stream: net::TcpStream) -> Proceed {
    let mut rcv = rsrun_recv(&mut stream).await;
    let mut res = Proceed::Resume;
    while let Some(msg) = rcv {
        match msg {
            Message {
                proceed: Proceed::Resume,
                ..
            } => {
                println!(
                    "Message from {} with rank {}: \"{}\"",
                    msg.sender, msg.rank, msg.payload
                );
                rcv = rsrun_recv(&mut stream).await;
            }
            Message {
                proceed: Proceed::Quit,
                ..
            } => {
                println!("Sender {} with rank {} wants to quit", msg.sender, msg.rank);
                res = Proceed::Quit;
                rcv = None;
            }
        }
    }
    res
}

/// Accept connections and create futures handling these connections.
/// Be aware that new connections are only accepted if the master
/// process confirms that slaves want to continue execution.
async fn accept_conn(listener: net::TcpListener) {
    let mut p = Proceed::Resume;
    while Proceed::Resume == p {
        if let Ok((stream, _)) = listener.accept().await {
            p = handle_master(stream).await;
        }
    }
}

/// Accept connections and create futures handling these connections.
/// Handling connections does not interfere with accepting new
/// connections.
async fn _accept_conn(listener: net::TcpListener) {
    unimplemented!()
}

async fn master<S: Fn() -> Vec<Process>>(spawner: S) {
    if let Some(socket) = bind_socket(3636).await {
        let listener = accept_conn(socket); // accept connections
        let slaves = spawner(); // spawn children
        listener.await; // handle connections

        for s in slaves {
            // wait for children to terminate
            if let Some(mut child) = s.val {
                println!("Waiting for {} to terminate.", s.name);
                let _ = child.wait().await;
            }
        }
    }
}

fn main() {
    // all hosts execute these statements
    let my_rank = rank();
    println!("Master with rank {} is {}", my_rank, pid());

    if let Ok(rt) = Runtime::new() {
        // master process spawns and waits for slaves
        if my_rank == 0 {
            let _remote = || run_on(&["rust1", "rust2"]);
            let local = || run_local(4);
            rt.block_on(master(local));
        }
        // all other processes run the slave function
        else {
            rt.block_on(slave())
        }
    }
}
