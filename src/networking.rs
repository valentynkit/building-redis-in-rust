use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::iter;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::error;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use thiserror::Error;
use tracing::{debug, debug_span, error, info, instrument, warn};

use crate::client::{Client, ClientId, ClientRole, Disposition};
use crate::db::{Db, HandleWaitersResult};
use crate::resp::RespBody;
use crate::{Cli, client};
const ADDR: &str = "127.0.0.1";
const LISTENER: Token = Token(0);
const MASTER: Token = Token(1);
const MAX_EVENTS: usize = 128;

pub struct StartTime {
    start_ms_mono: Instant,
}

impl StartTime {
    pub const fn new(start_ms_mono: Instant) -> Self {
        Self { start_ms_mono }
    }
}

#[derive(Debug, Error, Clone)]
pub enum NetworkingError {
    #[error("{0} - should be valid IPv4 or IPv6")]
    NotValidAddr(String),
    #[error("slave has invalid state")]
    InvalidSlave,
    #[error("master disconnected")]
    MasterDisconnected,
    #[error("Handshake unfinished")]
    HandshakeUnfinished,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum ServerRole {
    Master,
    Slave,
}
impl ServerRole {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Slave => "slave",
        }
    }
}
pub struct ServerInfo {
    pub role: ServerRole,
    pub connected_slaves: u32,
    pub master_replid: String,
    pub master_repl_offset: i64,
    pub replica_of: Option<String>,
    dir: String,
    dbfilename: String,
}

impl ServerInfo {
    pub(crate) fn new(
        role: ServerRole,
        connected_slaves: u32,
        master_replid: String,
        master_repl_offset: i64,
        replica_of: Option<String>,
        dir: String,
        dbfilename: String,
    ) -> Self {
        Self {
            role,
            connected_slaves,
            master_replid,
            master_repl_offset,
            replica_of,
            dir,
            dbfilename,
        }
    }
    pub fn rdb_path(&self) -> PathBuf {
        Path::new(&self.dir).join(&self.dbfilename)
    }
}

pub struct Server {
    listener: TcpListener,
    clients: HashMap<Token, Client>,
    // TODO hashset?
    slaves: HashSet<Token>,
    master_link: Option<Client>,
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
        let port = cli.port();
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
        let db = Db::create(realtime_ms);
        let mut role = ServerRole::Master;
        let mut replica_of_parsed: Option<String> = None;

        if let Some((host, port)) = replicaof {
            role = ServerRole::Slave;
            replica_of_parsed = Some(format!("{host}:{port}"));
        }

        warn!(?role, ?replica_of_parsed);

        //TODO:  make optional master_replid, offset. and have it None for slaves and configured for master.
        let server_info = Rc::new(RefCell::new(ServerInfo::new(
            role,
            0,
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
            0,
            replica_of_parsed,
            cli.dir().into(),
            cli.dbfilename().into(),
        )));
        Ok(Self {
            listener,
            clients: HashMap::new(),
            slaves: HashSet::new(),
            next_client_id: 1,
            poll,
            db,
            cronloops: 0,
            start_time,
            server_info,
            master_link: None,
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
        let HandleWaitersResult {
            replies: list_replies,
            deadline: list_deadline,
        } = self.db.handle_list_waiters();
        let HandleWaitersResult {
            replies: stream_replies,
            deadline: stream_deadline,
        } = self.db.handle_stream_waiters();

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

    fn slave_handshake(&mut self, port: u16) -> Result<(), anyhow::Error> {
        {
            let master_addr = {
                let server_info = self.server_info.borrow();

                if server_info.role == ServerRole::Master {
                    return Ok(());
                }

                warn!("strarting the slave handshake");
                let Some(master_addr) = &server_info.replica_of else {
                    return Err(NetworkingError::InvalidSlave.into());
                };

                info!(master_addr = %master_addr, "connecting slave to master");
                master_addr.clone()
            };

            let stream = std::net::TcpStream::connect(master_addr)?;
            // Long lived replication link for slave -> master
            let stream = mio::net::TcpStream::from_std(stream);

            let c_token = Token(self.get_increased_id());
            let client = self
                .register_client(stream, c_token, ClientRole::Master)
                .expect("client initialization should succedd");

            info!(?c_token, "connected master_client");
            self.master_link = Some(client);
        }

        self.slave_ping()?;
        self.slave_replconf(port)?;
        self.slave_psync()?;
        info!("handshake successfully finished, client is ready");
        Ok(())
    }

    #[instrument(skip(self), fields(lfd = self.listener.as_raw_fd()))]
    pub fn run(mut self, port: u16) -> Result<()> {
        let mut events = Events::with_capacity(MAX_EVENTS);
        self.slave_handshake(port)?;
        loop {
            let _span = debug_span!("server loop", loop = self.cronloops + 1).entered();

            let timeout = self.before_sleep();
            self.poll.poll(&mut events, timeout)?;
            self.set_current_time()?;
            for event in &events {
                if event.is_readable() {
                    let token = event.token();
                    match token {
                        LISTENER => self.accept_client(),
                        MASTER => self.service_master(),
                        _ => self.service_client(token),
                    }
                }
            }
        }
    }

    fn register_client(
        &self,
        mut stream: TcpStream,
        c_token: Token,
        role: ClientRole,
    ) -> Option<Client> {
        if let Err(error) = self
            .poll
            .registry()
            .register(&mut stream, c_token, Interest::READABLE)
        {
            error!(?c_token, ?error, "registration failed");
            return None;
        }

        let server_info = Rc::clone(&self.server_info);
        let client = Client::new(stream, ClientId::new(c_token.0), role, server_info);
        Some(client)
    }

    #[instrument(skip(self))]
    fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    let c_token = MASTER;
                    info!(?addr, ?c_token, "connected client");
                    let client = self
                        .register_client(stream, c_token, ClientRole::Normal)
                        .expect("client initialization should succedd");

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
    fn service_master(&mut self) {
        if let Some(master) = &mut self.master_link {
            let (disposition, _) = master.on_readable(&mut self.db);

            if matches!(disposition, Disposition::Drop) {
                error!("master link was dropped");
            }
        } else {
            error!("master_link empty");
        }
    }
    #[instrument(skip(self, token), fields(client_id = token.0))]
    fn service_client(&mut self, token: Token) {
        if let Some(client) = self.clients.get_mut(&token) {
            let (disposition, to_propogate) = client.on_readable(&mut self.db);
            if matches!(disposition, Disposition::Drop) {
                let client_role = client.role();

                self.db.remove_watcher(ClientId::new(token.0));
                warn!("removing client");
                self.clients.remove(&token);

                if client_role == ClientRole::Slave && self.slaves.contains(&token) {
                    self.slaves.remove(&token);

                    let mut server_info = self.server_info.borrow_mut();
                    server_info.connected_slaves -= 1;
                }
                return;
            }
            if client.role() == ClientRole::Slave && !self.slaves.contains(&token) {
                let mut server_info = self.server_info.borrow_mut();
                server_info.connected_slaves += 1;
                self.slaves.insert(token);
            }

            for cmd in &to_propogate {
                for token in &mut self.slaves.iter() {
                    if let Some(slave) = self.clients.get_mut(token) {
                        slave.write_out(cmd);
                        slave.flush();
                    }
                }
            }
        }
    }

    fn slave_psync(&mut self) -> Result<(), anyhow::Error> {
        info!("starting replconf for master-slave");

        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        // 1/2
        let resp_body = ["PSYNC", "?", "-1"].into_iter().collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();
        let out = master_client.read_line()?;

        if !out.starts_with("+FULLRESYNC ") {
            error!(?out, "master-slave psync: expected +FULLRESYNC\r\n");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }

    fn slave_replconf(&mut self, port: u16) -> Result<(), anyhow::Error> {
        info!("starting replconf for master-slave");

        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        // 1/2
        let resp_body = ["REPLCONF", "listening-port", &port.to_string()]
            .into_iter()
            .collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();

        let out = master_client.read_line()?;
        if out != "+OK\r\n" {
            error!(?out, "master-slave repl_conf 1/2: expected +OK\r\n");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        // 2/2
        let resp_body = ["REPLCONF", "capa", "psync2"]
            .into_iter()
            .collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();
        let out = master_client.read_line()?;
        if out != "+OK\r\n" {
            error!(?out, "master-slave repl_conf 2/2: expected +OK\r\n");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }
    fn slave_ping(&mut self) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };

        master_client.write_out(&iter::once("PING").collect::<RespBody>());
        master_client.flush();
        let out = master_client.read_line()?;
        if out != "+PONG\r\n" {
            error!(?out, "master-slave: expected +PONG\r\n");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }
}

fn server_start(port: u16) -> Result<TcpListener, anyhow::Error> {
    let addr_str = format!("{ADDR}:{port}");
    let addr = addr_str
        .parse()
        .map_err(|_| NetworkingError::NotValidAddr(addr_str))?;

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
