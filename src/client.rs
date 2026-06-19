use crate::command;
use crate::resp;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};

pub const READ_BUF: usize = 512;
/// Does this client survive the poll, or get dropped?
pub enum Disposition {
    Keep,
    Drop,
}

pub struct Client {
    stream: TcpStream,
    inbuf: Vec<u8>,
    outbuf: Vec<u8>, // replies waiting to go out
}

impl Client {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            inbuf: Vec::with_capacity(READ_BUF),
            outbuf: Vec::new(),
        }
    }
    fn process(&mut self) -> Disposition {
        while let Some((args, consumed)) = resp::parse(&self.inbuf) {
            self.inbuf.drain(..consumed);
            command::dispatch(&args, &mut self.outbuf); // pure args in reply bytes out
        }
        self.flush()
    }

    fn flush(&mut self) -> Disposition {
        if let Err(e) = self.stream.write_all(&self.outbuf) {
            eprintln!("flush (fd{}): {e}", self.stream.as_raw_fd());
            return Disposition::Drop;
        }
        Disposition::Keep
    }

    pub fn handle(&mut self) -> Disposition {
        let mut stream = &self.stream;
        let mut buf = [0u8; READ_BUF];

        match stream.read(&mut buf) {
            // EOF: peer closed cleanly
            Ok(0) => {
                println!("disconnected (fd{})", stream.as_raw_fd());
                Disposition::Drop
            }
            // TODO extract logic
            Ok(n) => {
                self.inbuf.extend_from_slice(&buf[..n]);
                self.process()
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Disposition::Keep, // nothing yet
            Err(e) if e.kind() == io::ErrorKind::Interrupted => Disposition::Keep, // EINTR
            Err(e) => {
                eprintln!("read (fd {}): {e}", stream.as_raw_fd());
                Disposition::Drop
            }
        }
    }
}
