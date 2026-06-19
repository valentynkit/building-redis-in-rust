pub enum Command {
    Ping,
    Echo,
}
impl Command {
    pub fn from_name(name: &[u8]) -> Option<Self> {
        match name {
            b"PING" => Some(Self::Ping),
            b"ECHO" => Some(Self::Echo),
            _ => None,
        }
    }

    pub fn arity(&self) -> usize {
        match self {
            Command::Ping => 1,
            Command::Echo => 1,
        }
    }
}
/// All command handling lives here. This is the seam that grows into a Command enum.
pub fn dispatch(_args: &[Vec<u8>], out: &mut Vec<u8>) {
    out.extend_from_slice(b"+PONG\r\n"); // stub
}
