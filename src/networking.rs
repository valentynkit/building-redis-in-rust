use std::collections::HashMap;
use std::io::{self};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};

use crate::client::{Client, Disposition};

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

const ADDR: &str = "127.0.0.1:6379";
const MAX_EVENTS: usize = 64;

pub struct Server {
    listener: TcpListener,
    clients: HashMap<RawFd, Client>,
    kq: RawFd,
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

    pub fn run(mut self) -> io::Result<()> {
        let mut events = [read_event(0, 0); MAX_EVENTS];
        // Busy-poll
        // TODO: mio or switch to tokio or any other abstractions.
        let lfd = self.listener.as_raw_fd();
        loop {
            // SAFETY: events is a valide [kevent; 64]; NULL timeout = block until ready (0% idle CPU).
            let n = syscall!(kevent(
                self.kq,
                std::ptr::null_mut(),
                0,
                events.as_mut_ptr(),
                events.len() as i32,
                std::ptr::null()
            ))?;
            for ev in &events[..n as usize] {
                let fd = ev.ident as RawFd;
                // new connection to server
                if fd == lfd {
                    self.accept_client();
                } else {
                    self.service_client(fd);
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

    fn service_client(&mut self, fd: RawFd) {
        if let Some(client) = self.clients.get_mut(&fd)
            && matches!(client.on_readable(), Disposition::Drop)
        {
            self.clients.remove(&fd);
        }
    }
}

// one EVFILT_READ filter for fd
fn read_event(fd: RawFd, flags: u16) -> libc::kevent {
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
