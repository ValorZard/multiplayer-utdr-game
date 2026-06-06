use rpc::PlayerSide;
use rpc::PlayerSideResolver;
use std::net::SocketAddr;

pub struct LobbyData {
    pub left_side: Option<SocketAddr>,
    pub right_side: Option<SocketAddr>,
    winner: Option<SocketAddr>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LobbyState {
    Empty,
    Waiting,
    Full,
}

#[derive(Debug)]
pub enum LobbyError {
    SameAddr,
    AlreadyFull,
    NeverExisted,
}

impl std::fmt::Display for LobbyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LobbyError::SameAddr => write!(
                f,
                "Error! We can't have two players who come from the same addr"
            ),
            LobbyError::AlreadyFull => write!(f, "Error! This lobby is already full!"),
            LobbyError::NeverExisted => write!(f, "Error! This player never existed here!"),
        }
    }
}

impl std::error::Error for LobbyError {}

impl LobbyData {
    pub fn new(left_side: SocketAddr) -> Self {
        Self {
            left_side: Some(left_side),
            right_side: None,
            winner: None,
        }
    }

    pub fn insert_player(
        &mut self,
        new_player: SocketAddr,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        let new_player = Some(new_player);

        if self.left_side == new_player || self.right_side == new_player {
            return Err(LobbyError::SameAddr);
        }

        if self.left_side.is_none() {
            self.left_side = new_player;
            return Ok((PlayerSide::Left, self.get_current_state()));
        } else if self.right_side.is_none() {
            self.right_side = new_player;
            return Ok((PlayerSide::Right, self.get_current_state()));
        } else {
            return Err(LobbyError::AlreadyFull);
        }
    }

    pub fn remove_player(
        &mut self,
        leaving_player: SocketAddr,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        if let Some(addr) = self.left_side
            && addr == leaving_player
        {
            let _ = self.left_side.take();
            if self.right_side.is_some() {
                return Ok((PlayerSide::Left, LobbyState::Waiting));
            } else {
                return Ok((PlayerSide::Left, LobbyState::Empty));
            }
        } else if let Some(addr) = self.right_side
            && addr == leaving_player
        {
            let _ = self.right_side.take();
            if self.left_side.is_some() {
                return Ok((PlayerSide::Right, LobbyState::Waiting));
            } else {
                return Ok((PlayerSide::Right, LobbyState::Empty));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn lobby_tests() {
        let dummy_left = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let dummy_right = SocketAddr::from_str("127.0.0.1:12342").unwrap();

        let mut lobby = LobbyData::new(dummy_left);
        assert_eq!(lobby.get_current_state(), LobbyState::Waiting);

        lobby.insert_player(dummy_right).unwrap();
        assert_eq!(lobby.get_current_state(), LobbyState::Full);

        lobby.remove_player(dummy_left).unwrap();
        assert_eq!(lobby.get_current_state(), LobbyState::Waiting);

        lobby.remove_player(dummy_right).unwrap();
        assert_eq!(lobby.get_current_state(), LobbyState::Empty);
    }
}
