use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
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
use tracing::{Span, debug, debug_span, error, field, info, info_span, trace};

use crate::client::{Client, ClientId, Disposition, PeerRole};
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

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone)]
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

struct OffsetAckWaiters {
    // key: ofsset, value: (ClientId, expected ack)
    client_id: ClientId,
    expected_ack: u32,
    deadline: Option<Duration>,
}

struct OffsetAck {
    // key: offset, value: number of ack for it, all the higher
    inner: BTreeMap<i64, u32>,
    // latest offset with full ack for connected_slaves
    latest_full_ack: i64,
}

impl OffsetAck {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            latest_full_ack: 0,
        }
    }

    pub fn increase_ack_count(&mut self, offset: i64, connected_slaves: u32) {
        // TODO: go through the btree map and find the first one where value = connected_slaves. and
        // we could remove all the values lower than this, Compaction
        let entry = self.inner.entry(offset).or_insert_with(|| 0);
        *entry += 1;

        // COMPACTION
        for (&key, &value) in self.inner.iter().rev() {
            if value == connected_slaves {
                self.latest_full_ack = key;
                break;
            }
        }
        self.inner = self.inner.split_off(&self.latest_full_ack);
    }
    pub fn get_ack_count(&self, offset: i64, connected_slaves: u32) -> u32 {
        if offset <= self.latest_full_ack {
            return connected_slaves;
        }

        if let Some(&received_ack) = self.inner.get(&offset) {
            received_ack
        } else {
            0
        }
    }
}

pub struct ServerInfo {
    role: ServerRole,
    connected_slaves: u32,
    master_replid: String,
    master_repl_offset: i64,
    replica_of: Option<String>,
    dir: String,
    dbfilename: String,
    // key: offset, value: number of ack for it, all the higher
    offset_ack: OffsetAck,
    offset_ack_waiters: BTreeMap<i64, OffsetAckWaiters>,
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
        let offset_ack = OffsetAck::new();
        let offset_ack_waiters = BTreeMap::new();

        Self {
            role,
            connected_slaves,
            master_replid,
            master_repl_offset,
            replica_of,
            dir,
            dbfilename,
            offset_ack,
            offset_ack_waiters,
        }
    }
    pub(crate) const fn role(&self) -> ServerRole {
        self.role
    }

    pub(crate) const fn connected_slaves(&self) -> u32 {
        self.connected_slaves
    }

    pub(crate) fn master_replid(&self) -> String {
        self.master_replid.clone()
    }

    pub(crate) const fn master_repl_offset(&self) -> i64 {
        self.master_repl_offset
    }

    pub(crate) const fn incr_repl_offset(&mut self, consumed: usize) {
        self.master_repl_offset += consumed as i64;
    }

    pub fn rdb_path(&self) -> PathBuf {
        Path::new(&self.dir).join(&self.dbfilename)
    }
}

pub struct ServerConfig {
    hz: u32,
    active_expire_enabled: bool,
    active_expire_percent: u32,
    active_expire_keys_per_round: usize,
    client_timeout_s: u32,
    client_timeout_check_hz: u32,
}

impl ServerConfig {
    fn init() -> ServerConfig {
        Self {
            // Server tick rate
            hz: 10,
            active_expire_enabled: true,
            active_expire_percent: 25,
            active_expire_keys_per_round: 20,
            client_timeout_s: 300,
            client_timeout_check_hz: 10,
        }
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
    info: Rc<RefCell<ServerInfo>>,
    config: ServerConfig,
    span: Span,
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
        let db = Db::create(monotonic_ms.elapsed(), realtime_ms);
        let mut role = ServerRole::Master;
        let mut replica_of_parsed: Option<String> = None;

        if let Some((host, port)) = replicaof {
            role = ServerRole::Slave;
            replica_of_parsed = Some(format!("{host}:{port}"));
        }

        info!(?role, ?replica_of_parsed, "server role resolved");

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

        let server_span = info_span!(
            "server",
            role = server_info.borrow().role.as_str(),
            port,
            replid = &server_info.borrow().master_replid[..7],
            run_id = std::process::id(),
            lfd = listener.as_raw_fd(),
            slaves = field::Empty,
        );

        Ok(Self {
            listener,
            clients: HashMap::new(),
            slaves: HashSet::new(),
            next_client_id: 1,
            poll,
            db,
            cronloops: 0,
            start_time,
            info: server_info,
            config: ServerConfig::init(),
            master_link: None,
            span: server_span,
        })
    }

    fn set_current_time(&mut self) -> Result<()> {
        let realtime_ms = SystemTime::now().duration_since(UNIX_EPOCH)?;
        let monotonic_ms = Instant::now().elapsed();

        let uptime = self.start_time.start_ms_mono.elapsed();
        trace!(?uptime, "clock tick");

        self.db.update_time(monotonic_ms, realtime_ms);
        Ok(())
    }

    fn active_expire_cron(&mut self) {
        let config = &self.config;
        if !config.active_expire_enabled {
            return;
        }

        let start: u128 = self.db.monotonic_ms().as_millis();
        let mut current: u128 = start;
        let budget_ms: u128 = u128::from((1000 / config.hz) / 4);
        while (current - start < budget_ms) {
            let expired: u32 = self.db.active_sweep(config.active_expire_keys_per_round);
            let is_below_threshold = (expired * 100)
                .saturating_div(config.active_expire_keys_per_round as u32)
                < config.active_expire_percent;

            if is_below_threshold {
                break;
            }
            current = Instant::now().elapsed().as_millis();
        }
    }
    fn clients_key_waiters_cron(&mut self) -> Option<Duration> {
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
                let _g = client.span().entered();
                debug!("delivering blocked reply to waiter");
                client.write_out(&resp);

                if matches!(client.flush(), Disposition::Drop) {
                    info!("waiter disconnected mid-flush; removed");
                    self.clients.remove(&client_id);
                }
            }
        }

        match (list_deadline, stream_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn client_ack_waiters_cron(&mut self) -> Option<Duration> {
        todo!()
    }
    // HouseKeeping
    fn before_sleep(&mut self) -> Option<Duration> {
        self.cronloops += 1;
        self.active_expire_cron();
        let key_waiters_deadline = self.clients_key_waiters_cron();
        let ack_waiters_deadline = self.client_ack_waiters_cron();
        key_waiters_deadline.min(ack_waiters_deadline)
    }

    fn slave_handshake(&mut self, port: u16) -> Result<(), anyhow::Error> {
        {
            let master_addr = {
                let server_info = self.info.borrow();

                if server_info.role == ServerRole::Master {
                    return Ok(());
                }

                let Some(master_addr) = &server_info.replica_of else {
                    return Err(NetworkingError::InvalidSlave.into());
                };

                info!(master_addr = %master_addr, "starting slave handshake");
                master_addr.clone()
            };

            let stream = std::net::TcpStream::connect(master_addr)?;
            // Long lived replication link for slave -> master
            let stream = mio::net::TcpStream::from_std(stream);

            let client = self
                .register_client(stream, MASTER, PeerRole::Master)
                .expect("master link registration should succeed");

            self.master_link = Some(client);
        }

        // Every handshake step logs under the master link's connection span.
        let span = self.master_link.as_ref().expect("master link set").span();
        let _g = span.entered();

        self.slave_ping()?;
        self.slave_replconf(port)?;
        self.slave_psync()?;
        info!("handshake finished; replica ready");
        Ok(())
    }

    pub fn run(mut self, port: u16) -> Result<()> {
        let _server_guard = self.span.clone().entered();
        info!("server starting");
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
        role: PeerRole,
    ) -> Option<Client> {
        if let Err(error) = self
            .poll
            .registry()
            .register(&mut stream, c_token, Interest::READABLE)
        {
            error!(?c_token, ?error, "registration failed");
            return None;
        }

        let server_info = Rc::clone(&self.info);
        let client = Client::new(stream, ClientId::new(c_token.0), role, server_info);
        Some(client)
    }

    fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    let c_token = Token(self.get_increased_id());
                    let client = self
                        .register_client(stream, c_token, PeerRole::Normal)
                        .expect("client registration should succeed");

                    let _g = client.span().entered();
                    info!(?addr, "client connected");
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

    fn service_master(&mut self) {
        let Some(master) = &mut self.master_link else {
            error!("master link empty");
            return;
        };
        let _g = master.span().entered();
        debug!("servicing master link");
        let (disposition, _) = master.on_readable(&mut self.db);
        if matches!(disposition, Disposition::Drop) {
            error!("master link dropped");
        }
    }

    fn service_client(&mut self, token: Token) {
        let Some(client) = self.clients.get_mut(&token) else {
            return;
        };
        let _g = client.span().entered();
        debug!("servicing client");
        let (disposition, to_propagate) = client.on_readable(&mut self.db);
        if matches!(disposition, Disposition::Drop) {
            let client_role = client.peer_role();

            self.db.remove_watcher(ClientId::new(token.0));
            info!("client disconnected; removed");
            self.clients.remove(&token);

            if client_role == PeerRole::Slave && self.slaves.contains(&token) {
                self.slaves.remove(&token);

                let slaves = {
                    let mut server_info = self.info.borrow_mut();
                    server_info.connected_slaves -= 1;
                    server_info.connected_slaves
                };
                self.span.record("slaves", slaves as u64);
                info!(slaves, "replica detached");
            }
            return;
        }
        if client.peer_role() == PeerRole::Slave && !self.slaves.contains(&token) {
            let slaves = {
                let mut server_info = self.info.borrow_mut();
                server_info.connected_slaves += 1;
                server_info.connected_slaves
            };
            self.slaves.insert(token);
            self.span.record("slaves", slaves as u64);
            info!(slaves, "replica attached");
        }

        let mut server_info = self.info.borrow_mut();

        let mut cmd_buffer: Vec<u8> = vec![];
        for cmd in &to_propagate {
            let mut delivered = 0u64;

            cmd_buffer.clear();
            cmd.encode(&mut cmd_buffer);

            let offset = cmd_buffer.len();
            server_info.incr_repl_offset(offset);

            for token in &mut self.slaves.iter() {
                if let Some(slave) = self.clients.get_mut(token) {
                    slave.write_out_buffer(&cmd_buffer);
                    slave.flush();
                    delivered += 1;
                }
            }
            debug!(slaves = delivered, "propagated write");
        }
    }

    fn slave_psync(&mut self) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        debug!("handshake step: PSYNC");
        let resp_body = ["PSYNC", "?", "-1"].into_iter().collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();
        let out = master_client.read_fullresync()?;

        if !out.starts_with("+FULLRESYNC ") {
            error!(?out, "handshake PSYNC: expected +FULLRESYNC");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }
        master_client.consume(&mut self.db);
        if matches!(master_client.flush(), Disposition::Drop) {
            error!(?out, "handshake PSYNC: couldn't flush to master");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }

    fn slave_replconf(&mut self, port: u16) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        debug!("handshake step: REPLCONF listening-port");
        let resp_body = ["REPLCONF", "listening-port", &port.to_string()]
            .into_iter()
            .collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();

        let out = master_client.read_line()?;
        if out != "+OK\r\n" {
            error!(?out, "handshake REPLCONF 1/2: expected +OK");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        debug!("handshake step: REPLCONF capa psync2");
        let resp_body = ["REPLCONF", "capa", "psync2"]
            .into_iter()
            .collect::<RespBody>();

        master_client.write_out(&resp_body);
        master_client.flush();
        let out = master_client.read_line()?;
        if out != "+OK\r\n" {
            error!(?out, "handshake REPLCONF 2/2: expected +OK");
            return Err(NetworkingError::HandshakeUnfinished.into());
        }

        Ok(())
    }
    fn slave_ping(&mut self) -> Result<(), anyhow::Error> {
        let Some(master_client) = &mut self.master_link else {
            return Err(NetworkingError::HandshakeUnfinished.into());
        };
        debug!("handshake step: PING");
        master_client.write_out(&iter::once("PING").collect::<RespBody>());
        master_client.flush();
        let out = master_client.read_line()?;
        if out != "+PONG\r\n" {
            error!(?out, "handshake PING: expected +PONG");
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
