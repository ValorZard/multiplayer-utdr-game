use std::{cell::OnceCell, io::Error as IoError, net::SocketAddr};

pub struct Lobby {
    left_side: Option<SocketAddr>,
    right_side: Option<SocketAddr>,
    winner: Option<SocketAddr>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LobbyState {
    Empty,
    Waiting,
    Full
}

#[derive(Debug)]
pub enum LobbyError {
    SameAddr,
    AlreadyFull,
    NeverExisted
}

impl std::fmt::Display for LobbyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LobbyError::SameAddr => write!(f, "Error! We can't have two players who come from the same addr"),
            LobbyError::AlreadyFull => write!(f, "Error! This lobby is already full!"),
            LobbyError::NeverExisted => write!(f, "Error! This player never existed here!"),
        }
    }
}

impl std::error::Error for LobbyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl Lobby {
    // we initialize with the left side first, but the left side can leave the match, which can be annoying
    pub fn new(left_side: SocketAddr) -> Self {
        Self {
            left_side: Some(left_side),
            right_side: None,
            winner: None,
        }
    }

    pub fn insert_player(&mut self, new_player: SocketAddr) -> Result<LobbyState, LobbyError> {
        let new_player = Some(new_player);
        if self.left_side == new_player || self.right_side == new_player {
            return Err(LobbyError::SameAddr);
        }
        // depending on if a player left the lobby, either left or right side can be free
        if self.left_side.is_none() {
            self.left_side = new_player;
        } else if self.right_side.is_none() {
            self.right_side = new_player;
        } else {
            // both sides are filled, which means that this lobby is full
            return Err(LobbyError::AlreadyFull);
        }
        // return state of lobby
        if self.left_side.is_some() && self.right_side.is_some() {
            Ok(LobbyState::Full)
        } else {
            Ok(LobbyState::Waiting)
        }
    }

    pub fn remove_player(&mut self, leaving_player: SocketAddr) -> Result<LobbyState, LobbyError> {
        if let Some(addr) = self.left_side && addr == leaving_player {
            let _ = self.left_side.take();
            if self.right_side.is_some() {
                return Ok(LobbyState::Waiting);
            } else {
                 return Ok(LobbyState::Empty);
            }
        } else if let Some(addr) = self.right_side && addr == leaving_player {
            let _ = self.right_side.take();
            if self.left_side.is_some() {
                return Ok(LobbyState::Waiting);
            } else {
                return Ok(LobbyState::Empty);
            }
        } 
        Err(LobbyError::NeverExisted)
    }

    pub fn get_current_state(&self) -> LobbyState {
        if self.left_side.is_some() && self.right_side.is_some() {
            LobbyState::Full
        } else if self.left_side.is_none() && self.right_side.is_none() {
            LobbyState::Empty
        } else {
            LobbyState::Waiting
        }
    }
}

enum LobbyMessage {
    Heartbeat,
}

pub fn run_lobby_actor() {}
