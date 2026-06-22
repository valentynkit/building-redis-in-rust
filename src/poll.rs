//! Readiness poller. One interface, two OS backends.
//! Callers see `Poller` + `Ready` only — never epoll/kqueue types.
//! Swap this module for mio/tokio later; `Server` won't change.
use std::io;
use std::os::fd::RawFd;

const MAX_EVENTS: usize = 64;

/// Platform-neutral readiness event. Each backend maps its native event into this,
/// so the rest of the server never touches `EventFilter`/`epoll` types.
#[derive(Debug, Clone, Copy)]
pub struct Ready {
    pub fd: RawFd,
    pub readable: bool,
    pub writable: bool,
}

#[cfg(target_os = "linux")]
mod imp {
    use std::os::fd::{BorrowedFd, OwnedFd};

    use super::*;
    use rustix::{
        buffer::spare_capacity,
        event::epoll::{self, CreateFlags, Event, EventData, EventFlags},
    };

    pub struct Poller {
        epoll_fd: OwnedFd,
        events: Vec<Event>, // reusable kernel buffer, refilled each wait()
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            let epoll_fd = epoll::jreate(CreateFlags::CLOEXEC)?;
            Ok(Self {
                epoll_fd,
                events: Vec::with_capacity(MAX_EVENTS),
            })
        }

        pub fn register(&self, fd: RawFd) -> io::Result<()> {
            // Stash the fd in EventData so wait() recovers it (epoll's analog of kqueue's udata).
            // borrow_raw: caller keeps `fd` open while it's registered (Server owns the socket).
            let source = unsafe { BorrowedFd::borrow_raw(fd) };
            // `?` converts rustix's Errno into std io::Error.
            epoll::add(
                &self.epoll_fd,
                source,
                EventData::new_u64(fd as u64),
                EventFlags::IN,
            )?;
            Ok(())
        }

        pub fn wait(&mut self) -> io::Result<Vec<Ready>> {
            // clear() -> len 0 so spare_capacity reuses the whole allocation each call.
            self.events.clear();
            epoll::wait(&self.epoll_fd, spare_capacity(&mut self.events), None)?;

            Ok(self
                .events
                .iter()
                .map(|ev| {
                    // Event is #[repr(packed)] on x86_64 — copy fields out before use;
                    // referencing a packed field directly is a hard error (E0793).
                    let (data, flags) = (ev.data, ev.flags);
                    Ready {
                        fd: data.u64() as RawFd,
                        readable: flags.contains(EventFlags::IN),
                        writable: flags.contains(EventFlags::OUT),
                    }
                })
                .collect())
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::os::fd::OwnedFd;

    use super::*;
    use rustix::{
        buffer::spare_capacity,
        event::kqueue::{Event, EventFilter, EventFlags, kevent, kqueue},
    };

    pub struct Poller {
        kq: OwnedFd,
        events: Vec<Event>, // reusable kernel buffer, refilled each wait()
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            let kq = kqueue()?;
            Ok(Self {
                kq,
                events: Vec::with_capacity(MAX_EVENTS),
            })
        }

        // Consider fastloops later: register + collect in one kevent syscall.
        pub fn register(&self, fd: RawFd) -> io::Result<()> {
            let change = Event::new(EventFilter::Read(fd), EventFlags::ADD, std::ptr::null_mut());
            let mut empty: [Event; 0] = [];
            // SAFETY: caller keeps `fd` open while it's registered in this kqueue.
            unsafe {
                kevent(&self.kq, &[change], &mut empty, None)?;
            }
            Ok(())
        }

        pub fn wait(&mut self) -> io::Result<Vec<Ready>> {
            // clear() -> len 0 so spare_capacity reuses the whole allocation each call
            // (without it the Vec appends every wait and grows unbounded).
            self.events.clear();
            // SAFETY: registered fds are kept alive by the caller (Server owns the sockets).
            unsafe { kevent(&self.kq, &[], spare_capacity(&mut self.events), None)? };

            // spare_capacity set len == #events written, so iterate the whole Vec.
            Ok(self
                .events
                .iter()
                .filter_map(|ev| match ev.filter() {
                    EventFilter::Read(fd) => Some(Ready {
                        fd,
                        readable: true,
                        writable: false,
                    }),
                    EventFilter::Write(fd) => Some(Ready {
                        fd,
                        readable: false,
                        writable: true,
                    }),
                    _ => None,
                })
                .collect())
        }
    }
}

pub use imp::Poller;
