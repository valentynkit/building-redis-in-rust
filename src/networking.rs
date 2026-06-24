use std::collections::HashMap;
use std::io::{self};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::client::{Client, Disposition};
use crate::db::Db;
use crate::poll::Poller;
const ADDR: &str = "127.0.0.1:6379";

pub struct Server {
    listener: TcpListener,
    clients: HashMap<RawFd, Client>,
    poller: Poller,
    db: Db,
    start_ms: Instant,
}

impl Server {
    pub fn new() -> Result<Self> {
        let listener = server_start().context("starting listener")?;
        let poller = Poller::new().context("creating poller")?;
        poller
            .register(listener.as_raw_fd())
            .context("registering listener fd")?;
        // register the listener for "readable" = incoming connection
        let monotonic_ms = Instant::now();
        let start_ms = monotonic_ms;
        let realtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("reading wall clock")?;

        let db = Db::create(start_ms, realtime_ms);
        Ok(Self {
            listener,
            clients: HashMap::new(),
            poller,
            db,
            start_ms,
        })
    }
    fn set_current_time(&mut self) -> Result<()> {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH)?;

        self.db.update_time(realtime_ms);
        Ok(())
    }
    pub fn run(mut self) -> Result<()> {
        let lfd = self.listener.as_raw_fd();

        loop {
            self.set_current_time()?;
            let events = self.poller.wait()?;
            for event in events {
                if event.readable {
                    if event.fd == lfd {
                        self.accept_client();
                    } else {
                        self.service_client(event.fd);
                    }
                }
            }
        }
    }

    fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    // Accepted socket does NOT inherit the listener's nonblocking
                    if let Err(e) = stream.set_nonblocking(true) {
                        eprintln!("set_nonblocking({addr}) failed: {e}");
                        continue;
                    }
                    let cfd = stream.as_raw_fd();

                    if let Err(e) = self.poller.register(cfd) {
                        eprintln!("register (fd {cfd}): {e}");
                        continue;
                    }
                    println!("connected: {addr} (fd {})", stream.as_raw_fd());
                    let client = Client::new(stream);
                    self.clients.insert(cfd, client);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => {
                    eprintln!("accept error: {e}");
                    break;
                }
            }
        }
    }

    fn service_client(&mut self, fd: RawFd) {
        if let Some(client) = self.clients.get_mut(&fd)
            && matches!(client.on_readable(&mut self.db), Disposition::Drop)
        {
            self.clients.remove(&fd);
        }
    }
}

fn server_start() -> Result<TcpListener, anyhow::Error> {
    let listener = TcpListener::bind(ADDR)?;
    listener.set_nonblocking(true)?;
    println!(
        "listening on {} (fd {})",
        listener.local_addr()?,
        listener.as_raw_fd()
    );
    Ok(listener)
}
