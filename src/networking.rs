use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self};
use std::os::fd::AsRawFd;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use tracing::{debug, debug_span, error, info, instrument, warn};

use crate::Cli;
use crate::client::{Client, ClientId, Disposition};
use crate::db::{Db, HandleWaitersResult};
const ADDR: &str = "127.0.0.1";
const LISTENER: Token = Token(0);
const MAX_EVENTS: usize = 128;

pub struct StartTime {
    start_ms_mono: Instant,
}

impl StartTime {
    pub const fn new(start_ms_mono: Instant) -> Self {
        Self { start_ms_mono }
    }
}

pub enum ServerRole {
    master,
    slave,
}
impl ServerRole {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::master => "master",
            Self::slave => "slave",
        }
    }
}
pub struct ServerInfo {
    pub role: ServerRole,
    pub connected_slaves: u32,
    pub master_replid: String,
    pub master_repl_offset: i64,
}

impl ServerInfo {
    fn new(
        role: ServerRole,
        connected_slaves: u32,
        master_replid: String,
        master_repl_offset: i64,
    ) -> Self {
        Self {
            role,
            connected_slaves,
            master_replid,
            master_repl_offset,
        }
    }
}
pub struct Server {
    listener: TcpListener,
    clients: HashMap<Token, Client>,
    next_client_id: usize,
    poll: Poll,
    db: Db,
    cronloops: u64,
    start_time: StartTime,
    server_info: Rc<RefCell<ServerInfo>>,
}

impl Server {
    const fn get_increased_id(&mut self) -> usize {
        self.next_client_id += 1;
        self.next_client_id
    }

    pub fn new(cli: &Cli) -> Result<Self> {
        let replicaof = cli.parse_replicaof()?;
        let port = cli.get_port();
        let mut listener = server_start(port).context("starting listener")?;
        let poll = Poll::new().context("creating poller")?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)
            .context("registering listener")?;

        // register the listener for "readable" = incoming connection
        let monotonic_ms = Instant::now();
        let realtime_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("reading wall clock")?;
        let start_time = StartTime::new(monotonic_ms);
        let db = Db::create(monotonic_ms, realtime_ms);

        let role = match replicaof {
            Some(_) => ServerRole::slave,
            None => ServerRole::master,
        };

        let server_info = Rc::new(RefCell::new(ServerInfo::new(
            role,
            0,
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
            0,
        )));

        Ok(Self {
            listener,
            clients: HashMap::new(),
            next_client_id: 0,
            poll,
            db,
            cronloops: 0,
            start_time,
            server_info,
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
    fn before_sleep(&mut self) -> Option<Duration> {
        self.cronloops += 1;
        let HandleWaitersResult(list_replies, list_deadline) = self.db.handle_waiters();
        let HandleWaitersResult(stream_replies, stream_deadline) = self.db.handle_stream_waiters();

        for (client_id, resp) in list_replies.into_iter().chain(stream_replies) {
            let client_id = Token(client_id.get());
            if let Some(client) = self.clients.get_mut(&client_id) {
                info!(?client_id, "writing to waiting client");
                client.write_out(&resp);

                if matches!(client.flush(), Disposition::Drop) {
                    warn!("removing client");
                    self.clients.remove(&client_id);
                }
            }
        }

        match (list_deadline, stream_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    #[instrument(skip(self), fields(lfd = self.listener.as_raw_fd()))]
    pub fn run(mut self) -> Result<()> {
        let mut events = Events::with_capacity(MAX_EVENTS);
        loop {
            let _span = debug_span!("server loop", loop = self.cronloops + 1).entered();

            let timeout = self.before_sleep();
            self.poll.poll(&mut events, timeout)?;
            self.set_current_time()?;
            for event in &events {
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

                    if let Err(error) =
                        self.poll
                            .registry()
                            .register(&mut stream, c_token, Interest::READABLE)
                    {
                        error!(?c_token, ?error, "registration failed");
                        continue;
                    }

                    info!(?addr, ?c_token, "connected client");
                    let server_info = Rc::clone(&self.server_info);
                    let client = Client::new(stream, ClientId::new(c_token.0), server_info);
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
            self.db.remove_watcher(ClientId::new(token.0));
            warn!("removing client");
            self.clients.remove(&token);
        }
    }
}

fn server_start(port: u16) -> Result<TcpListener, anyhow::Error> {
    let addr = format!("{ADDR}:{port}")
        .parse()
        .expect("should be valide IPv4 or IPv6");

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
