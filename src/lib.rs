use std::io::{self, Read};
use std::net::TcpStream;
use std::{io::Write, net::TcpListener, os::fd::AsRawFd};

fn server_start() -> Result<TcpListener, io::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    println!(
        "Server listening on: {} - fd: {}",
        listener.local_addr()?,
        listener.as_raw_fd()
    );
    listener.set_nonblocking(true)?;
    println!(
        "listening on {} (fd {})",
        listener.local_addr()?,
        listener.as_raw_fd()
    );
    Ok(listener)
}

struct Client {
    stream: TcpStream,
    inbuf: Vec<u8>,
}

struct Server {
    listener: TcpListener,
    clients: Vec<Client>,
}
/// Does this client survive the poll, or get dropped?
enum Disposition {
    Keep,
    Drop,
}

impl Client {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            inbuf: Vec::new(),
        }
    }
}

impl Server {
    fn new(listener: TcpListener) -> Self {
        Self {
            listener,
            clients: Vec::new(),
        }
    }

    fn accept_client(&mut self) {
        match self.listener.accept() {
            Ok((stream, addr)) => {
                // Accepted socket does NOT inherit the listener's nonblocking
                if let Err(e) = stream.set_nonblocking(true) {
                    eprintln!("set_nonblocking({addr}) failed: {e}");
                    return; // client dropped -> fd closed
                }
                println!("connected: {addr} (fd {})", stream.as_raw_fd());
                let client = Client::new(stream);
                self.clients.push(client);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => eprintln!("accept error: {e}"),
        }
    }

    fn poll_clients(&mut self) {
        self.clients
            .retain_mut(|c| matches!(handle_client(c), Disposition::Keep));
    }
}

fn handle_client(client: &mut Client) -> Disposition {
    let mut buf = [0u8; 512];
    match client.stream.read(&mut buf) {
        // EOF: peer closed cleanly
        Ok(0) => {
            println!("disconnected (fd{})", client.stream.as_raw_fd());
            Disposition::Drop
        }
        // TODO extract logic
        Ok(_n) => match client.stream.write_all(b"+PONG\r\n") {
            Ok(()) => Disposition::Keep,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep,
            Err(e) => {
                eprintln!("write (fd {}): {e}", client.stream.as_raw_fd());
                Disposition::Drop
            }
        },
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep, // nothing yet
        Err(e) if e.kind() == io::ErrorKind::Interrupted => Disposition::Keep, // EINTR
        Err(e) => {
            eprintln!("read (fd {}): {e}", client.stream.as_raw_fd());
            Disposition::Drop
        }
    }
}

pub fn run() -> Result<(), io::Error> {
    let mut server = Server::new(server_start()?);

    // Busy-poll
    // TODO: mio/epoll or switch to tokio
    loop {
        server.accept_client();
        server.poll_clients();
    }
}

#[cfg(test)]
mod test {
    use crate::run;

    #[test]
    fn run_test() {
        run();
    }
}
