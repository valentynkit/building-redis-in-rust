//! Readiness poller. One interface, two OS backends.
//! Callers see `Poller` + `Ready` only — never epoll/kqueue types.
//! Swap this module for mio/tokio later; `Server` won't change.
use std::io;

const MAX_EVENTS: usize = 128;

mod imp {

    use mio::{Events, Interest, Poll, Token, event::Source};

    use super::*;

    pub struct Poller {
        poll: Poll,
        events: Events, // reusable kernel buffer, refilled each wait()
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            let poll = Poll::new()?;
            Ok(Self {
                poll,
                events: Events::with_capacity(MAX_EVENTS),
            })
        }

        // Consider fastloops later: register + collect in one kevent syscall.
        pub fn register<S: Source>(&mut self, source: &mut S, token: Token) -> io::Result<()> {
            self.poll
                .registry()
                .register(source, token, Interest::READABLE)?;
            Ok(())
        }

        pub fn wait(&mut self) -> io::Result<&Events> {
            // clear() -> len 0 so spare_capacity reuses the whole allocation each call
            // (without it the Vec appends every wait and grows unbounded).
            self.events.clear();

            self.poll.poll(&mut self.events, None)?;

            // spare_capacity set len == #events written, so iterate the whole Vec.
            Ok(&self.events)
        }
    }
}

pub use imp::Poller;
