use core::error;
use std::collections::HashMap;
use std::io::{self};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tracing::{debug, debug_span, error, info, instrument, warn};

use crate::client::{Client, Disposition};
use crate::db::Db;
use crate::poll::Poller;
const ADDR: &str = "127.0.0.1:6379";
pub struct StartTime {
    start_ms_mono: Instant,
}

impl StartTime {
    pub fn new(start_ms_mono: Instant) -> Self {
        Self { start_ms_mono }
    }
}

pub struct Server {
    listener: TcpListener,
    clients: HashMap<RawFd, Client>,
    poller: Poller,
    db: Db,
    cronloops: u64,
    start_time: StartTime,
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
        let realtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("reading wall clock")?;
        let start_time = StartTime::new(monotonic_ms);
        let db = Db::create(monotonic_ms, realtime_ms);
        Ok(Self {
            listener,
            clients: HashMap::new(),
            poller,
            db,
            cronloops: 0,
            start_time,
        })
    }
    fn set_current_time(&mut self) -> Result<()> {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let uptime = self.start_time.start_ms_mono.elapsed();
        debug!(?uptime);

        self.db.update_time(realtime_ms);
        Ok(())
    }
    // HouseKeeping
    fn before_sleep(&mut self) -> Result<()> {
        self.set_current_time()?;
        self.cronloops += 1;
        let waiters = self.db.handle_waiters();
        for (client_fd, value) in waiters {
            if let Some(client) = self.clients.get_mut(&client_fd) {
                let client_fd = client.get_raw_fd();
                info!(?client_fd, ?value, "writing to waiting clients");
                let value_bytes: Vec<u8> = (&value).into();
                client.write_response(&value_bytes);
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(lfd = self.listener.as_raw_fd()))]
    pub fn run(mut self) -> Result<()> {
        let lfd = self.listener.as_raw_fd();
        // Span::current().record("lfd", lfd);

        loop {
            let _span = debug_span!("server loop", loop = self.cronloops + 1).entered();

            self.before_sleep()?;
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
    #[instrument(skip(self))]
    fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    // Accepted socket does NOT inherit the listener's nonblocking
                    if let Err(error) = stream.set_nonblocking(true) {
                        error!(?addr, ?error, "set_nonblocking failed");
                        continue;
                    }
                    let client_fd = stream.as_raw_fd();

                    if let Err(error) = self.poller.register(client_fd) {
                        error!(?client_fd, ?error, "registration failed");
                        continue;
                    }
                    info!(?addr, ?client_fd, "connected client");
                    let client = Client::new(stream);
                    self.clients.insert(client_fd, client);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    error!(?error, "accept failed");
                    break;
                }
            }
        }
    }

    #[instrument(skip(self))]
    fn service_client(&mut self, fd: RawFd) {
        if let Some(client) = self.clients.get_mut(&fd)
            && matches!(client.on_readable(&mut self.db), Disposition::Drop)
        {
            warn!("removing client");
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
