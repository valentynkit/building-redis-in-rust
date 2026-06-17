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
    Ok(listener)
}
fn process_client(client: &mut TcpStream) -> Result<(), io::Error> {
    println!("accepted new connection: {}", client.as_raw_fd());
    let mut buf = [0u8; 512];
    loop {
        let n = client.read(&mut buf)?;
        if n == 0 {
            break;
        }
        client.write_all(b"+PONG\r\n")?;
    }

    Ok(())
}
pub fn run() -> Result<(), io::Error> {
    let listener = server_start()?;
    loop {
        let (mut client, __peer) = listener.accept()?;
        process_client(&mut client)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use crate::run;

    #[test]
    fn run_test() {
        run();
    }
}
