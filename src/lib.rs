mod client;
mod command;
mod networking;
mod resp;

use networking::{Server, read_event};
use resp::parse;
use std::io::{self};
use std::os::fd::{AsRawFd, RawFd};

pub const MAX_EVENTS: usize = 64;
pub fn run() -> Result<(), io::Error> {
    let mut server = Server::new()?;

    let mut events = [read_event(0, 0); MAX_EVENTS];
    // Busy-poll
    // TODO: mio or switch to tokio or any other abstractions.
    let lfd = server.listener.as_raw_fd();
    loop {
        // SAFETY: events is a valide [kevent; 64]; NULL timeout = block until ready (0% idle CPU).
        let n = syscall!(kevent(
            server.kq,
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
                server.accept_client();
            } else {
                server.service_client(fd);
            }
        }
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
