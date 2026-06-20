//! kqueue readiness poller - the one place that touches the OS event API.
//! Swap this module for mio/tokio later; 'Server' won't change.
use std::io;
use std::os::fd::RawFd;

const MAX_EVENTS: usize = 64;

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

pub struct Poller {
    kq: RawFd,
    events: [libc::kevent; MAX_EVENTS],
}

impl Poller {
    pub fn new() -> io::Result<Self> {
        let kq = syscall!(kqueue())?;
        Ok(Self {
            kq,
            events: [read_event(0, 0); MAX_EVENTS],
        })
    }

    /// Start watching 'fd' for readability
    pub fn register(&self, fd: RawFd) -> io::Result<()> {
        let change = read_event(fd, libc::EV_ADD);
        syscall!(kevent(
            self.kq,
            &raw const change,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null()
        ))?;
        Ok(())
    }

    /// Block until one or more fds are readable; return them.
    // TODO: use iterator instead?
    pub fn wait(&mut self) -> io::Result<Vec<RawFd>> {
        // SAFETY: events is a valid [kevent; MAX_EVENTS]; NULL timeout = block until ready (0%
        // idle CPU).
        let n = syscall!(kevent(
            self.kq,
            std::ptr::null_mut(),
            0,
            self.events.as_mut_ptr(),
            self.events.len() as i32,
            std::ptr::null()
        ))?;

        Ok(self.events[..n as usize]
            .iter()
            .map(|ev| ev.ident as RawFd)
            .collect())
        // pontail: one Vec per poll cycle; reuse a buffer if it ever shows in a profile
    }
}
