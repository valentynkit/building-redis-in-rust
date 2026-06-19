use std::collections::HashMap;
use std::io::{self, Read};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::{io::Write, net::TcpListener};

use crate::client::{Client, Disposition};
use crate::command;
use crate::resp;
#[macro_export]
macro_rules! syscall {
    ($fn: ident ( $($arg: expr),* $(,)* ) ) => {{
        #[allow(unused_unsafe)]
        let res = unsafe { libc::$fn($($arg, )*) };
        if res < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

pub const ADDR: &str = "127.0.0.1:6379";
pub const MAX_EVENTS: usize = 64;
pub const MSG_MAX: usize = 4096;

pub struct Server {
    pub listener: TcpListener,
    pub clients: HashMap<RawFd, Client>,
    pub kq: RawFd,
}

fn register(kq: RawFd, fd: RawFd) -> io::Result<()> {
    let change = read_event(fd, libc::EV_ADD);
    syscall!(kevent(
        kq,
        &raw const change,
        1,
        std::ptr::null_mut(),
        0,
        std::ptr::null()
    ))?;
    Ok(())
}

impl Server {
    pub fn new() -> io::Result<Self> {
        let listener = server_start()?;
        let kq = syscall!(kqueue())?;
        // register the listener for "readable" = incoming connection

        register(kq, listener.as_raw_fd())?;

        Ok(Self {
            listener,
            clients: HashMap::new(),
            kq,
            // exit; fine for a forever-server
        })
    }

    pub fn accept_client(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    // Accepted socket does NOT inherit the listener's nonblocking
                    if let Err(e) = stream.set_nonblocking(true) {
                        eprintln!("set_nonblocking({addr}) failed: {e}");
                        continue;
                    }
                    let cfd = stream.as_raw_fd();

                    if let Err(e) = register(self.kq, cfd) {
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

    pub fn service_client(&mut self, fd: RawFd) {
        if let Some(client) = self.clients.get_mut(&fd)
            && matches!(client.handle(), Disposition::Drop)
        {
            self.clients.remove(&fd);
        }
    }
}

// one EVFILT_READ filter for fd
pub fn read_event(fd: RawFd, flags: u16) -> libc::kevent {
    libc::kevent {
        ident: fd as usize,
        filter: libc::EVFILT_READ,
        flags,
        fflags: 0,
        data: 0,
        udata: std::ptr::null_mut(),
    }
}

fn server_start() -> Result<TcpListener, io::Error> {
    let listener = TcpListener::bind(ADDR)?;
    listener.set_nonblocking(true)?;
    println!(
        "listening on {} (fd {})",
        listener.local_addr()?,
        listener.as_raw_fd()
    );
    Ok(listener)
}
