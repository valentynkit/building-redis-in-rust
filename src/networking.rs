use std::collections::HashMap;
use std::io::{self};
use std::os::fd::AsRawFd;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use mio::Token;
use mio::net::TcpListener;
use tracing::{debug, debug_span, error, info, instrument, warn};

use crate::client::{Client, ClientId, Disposition};
use crate::db::Db;
use crate::poll::Poller;
use crate::resp::Resp;
const ADDR: &str = "127.0.0.1:6379";
const LISTENER: Token = Token(0);

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
    clients: HashMap<Token, Client>,
    next_client_id: usize,
    poller: Poller,
    db: Db,
    cronloops: u64,
    start_time: StartTime,
}

impl Server {
    fn get_increased_id(&mut self) -> usize {
        self.next_client_id += 1;
        self.next_client_id
    }
    pub fn new() -> Result<Self> {
        let mut listener = server_start().context("starting listener")?;
        let mut poller = Poller::new().context("creating poller")?;
        poller
            .register(&mut listener, LISTENER)
            .context("registering listener")?;

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
            next_client_id: 0,
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
    fn before_sleep(&mut self) {
        self.cronloops += 1;
        // HM<ClientId, Option<(Key, Value)>> getting None for some client_id, means that it timeout, and have
        // to receive response
        let waiters = self.db.handle_waiters();
        for (client_id, kv) in waiters {
            let client_id = Token(client_id.get());
            if let Some(client) = self.clients.get_mut(&client_id) {
                let resp = match kv {
                    Some((key, value)) => {
                        info!(?client_id, ?key, ?value, "writing to waiting clients");
                        Resp::Array(Some(vec![
                            Resp::Bulk(Some(key.into())),
                            Resp::Bulk(Some(value.into())),
                        ]))
                    }
                    None => {
                        // TODO: Array none? or Bulk none
                        Resp::Array(None)
                    }
                };

                client.write_out(&resp);

                if matches!(client.flush(), Disposition::Drop) {
                    warn!("removing client");
                    self.clients.remove(&client_id);
                }
            }
        }
    }

    #[instrument(skip(self), fields(lfd = self.listener.as_raw_fd()))]
    pub fn run(mut self) -> Result<()> {
        loop {
            let _span = debug_span!("server loop", loop = self.cronloops + 1).entered();

            self.before_sleep();
            let events = self.poller.wait()?;
            self.set_current_time()?;
            for event in events {
                if event.is_readable() {
                    if event.token() == LISTENER {
                        self.accept_client();
                    } else {
                        self.service_client(event.token());
                    }
                }
            }
        }
    }
    #[instrument(skip(self))]
    fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, addr)) => {
                    let c_token = Token(self.get_increased_id());
                    if let Err(error) = self.poller.register(&mut stream, c_token) {
                        error!(?c_token, ?error, "registration failed");
                        continue;
                    }
                    info!(?addr, ?c_token, "connected client");
                    let client = Client::new(stream, ClientId::new(c_token.0));
                    self.clients.insert(c_token, client);
                }
                Err(e) if would_block(&e) => break,
                Err(e) if interrupted(&e) => {}
                Err(error) => {
                    error!(?error, "accept failed");
                    break;
                }
            }
        }
    }

    #[instrument(skip(self))]
    fn service_client(&mut self, token: Token) {
        if let Some(client) = self.clients.get_mut(&token)
            && matches!(client.on_readable(&mut self.db), Disposition::Drop)
        {
            warn!("removing client");
            self.clients.remove(&token);
        }
    }
}

fn server_start() -> Result<TcpListener, anyhow::Error> {
    let addr = ADDR.parse().expect("should be valide IPv4 or IPv6");
    let listener = mio::net::TcpListener::bind(addr)?;

    println!(
        "listening on {} (fd {})",
        listener.local_addr()?,
        listener.as_raw_fd()
    );
    Ok(listener)
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
