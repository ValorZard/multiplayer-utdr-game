use std::{cell::OnceCell, io::Error as IoError, net::SocketAddr};

pub struct Lobby {
    // when a lobby is created, we initialize the left side first, and wait for the right side to be initialized before starting.
    left_side: SocketAddr,
    right_side: OnceCell<SocketAddr>,
    winner: Option<SocketAddr>,
}

impl Lobby {
    pub fn new(left_side: SocketAddr) -> Self {
        Self {
            left_side,
            right_side: OnceCell::new(),
            winner: None,
        }
    }

    pub fn start_game(&mut self, right_side: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        if self.left_side == right_side {
            return Err(IoError::new(
                std::io::ErrorKind::InvalidInput,
                "Error! We can't have two players who come from the same addr",
            )
            .into());
        }
        Ok(())
    }
}
