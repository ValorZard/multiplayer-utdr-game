use crate::rps::{GameError, GameSession};
use anyhow::{anyhow, bail};
use rpc::{LobbyId, PlayerSideResolver, RpcServerMessage, encode_server_message};
use rpc::{LobbyState, PlayerSide, RPSGameState, RPSWinState, UserId};
use tokio::sync::mpsc::UnboundedSender;

pub type UserSender = UnboundedSender<Vec<u8>>;

#[derive(Debug)]
pub struct LobbySession {
    left_side: Option<(UserId, UserSender)>,
    right_side: Option<(UserId, UserSender)>,
    current_round: GameSession,
    winner: Option<RPSWinState>,
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

impl LobbySession {
    pub fn new(left_side: (UserId, UserSender)) -> Self {
        Self {
            left_side: Some(left_side),
            right_side: None,
            winner: None,
            current_round: GameSession::new(),
        }
    }

    pub fn insert_player(
        &mut self,
        new_player: (UserId, UserSender),
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        if (self.left_side.is_some() && self.left_side.as_ref().unwrap().0 == new_player.0)
            || (self.right_side.is_some() && self.right_side.as_ref().unwrap().0 == new_player.0)
        {
            return Err(LobbyError::SameAddr);
        }

        let new_player = Some(new_player);

        return if self.left_side.is_none() {
            self.left_side = new_player;
            Ok((PlayerSide::Left, self.get_current_lobby_state()))
        } else if self.right_side.is_none() {
            self.right_side = new_player;
            Ok((PlayerSide::Right, self.get_current_lobby_state()))
        } else {
            Err(LobbyError::AlreadyFull)
        };
    }

    pub fn remove_player(
        &mut self,
        leaving_player: UserId,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        // clear lobby state if we're removing players
        self.reset_lobby();
        if let Some((addr, _)) = self.left_side
            && addr == leaving_player
        {
            let _ = self.left_side.take();
            return Ok((PlayerSide::Left, self.get_current_lobby_state()));
        } else if let Some((addr, _)) = self.right_side
            && addr == leaving_player
        {
            let _ = self.right_side.take();
            return Ok((PlayerSide::Right, self.get_current_lobby_state()));
        }

        Err(LobbyError::NeverExisted)
    }

    pub fn get_winner(&self) -> Option<RPSWinState> {
        self.winner.clone()
    }

    pub fn get_left(&self) -> Option<UserId> {
        self.left_side
            .as_ref()
            .map_or(None, |(user, _)| Some(*user))
    }

    pub fn get_right(&self) -> Option<UserId> {
        self.right_side
            .as_ref()
            .map_or(None, |(user, _)| Some(*user))
    }

    pub fn set_left_input(&mut self, input: rpc::GameInput) -> Result<RPSGameState, GameError> {
        let state = self.current_round.set_left_input(input)?;
        if let RPSGameState::Win { state, .. } = state.clone() {
            self.winner = Some(state);
        }
        Ok(state)
    }

    pub fn set_right_input(&mut self, input: rpc::GameInput) -> Result<RPSGameState, GameError> {
        let state = self.current_round.set_right_input(input)?;
        if let RPSGameState::Win { state, .. } = state.clone() {
            self.winner = Some(state);
        }
        Ok(state)
    }

    pub fn reset_lobby(&mut self) {
        self.winner = None;
        self.current_round = GameSession::new();
    }

    pub fn get_current_lobby_state(&self) -> LobbyState {
        if self.winner.is_some() {
            LobbyState::Finished
        } else if self.left_side.is_some() && self.right_side.is_some() {
            LobbyState::Running
        } else if self.left_side.is_none() && self.right_side.is_none() {
            LobbyState::Empty
        } else {
            LobbyState::Waiting
        }
    }

    pub fn get_current_game_state(&self) -> RPSGameState {
        self.current_round.compute_state()
    }

    pub fn get_player_side(&self, addr: UserId) -> Option<PlayerSide> {
        if self.get_left() == Some(addr) {
            Some(PlayerSide::Left)
        } else if self.get_right() == Some(addr) {
            Some(PlayerSide::Right)
        } else {
            None
        }
    }

    pub(crate) fn send_message_to_user(
        &self,
        message: &RpcServerMessage,
        user_addr: &UserId,
    ) -> anyhow::Result<()> {
        let message_as_bytes = encode_server_message(message)?;
        if let Some((user, sender)) = self.left_side.as_ref()
            && *user == *user_addr
        {
            sender
                .send(message_as_bytes)
                .map_err(|e| anyhow!("failed to send message to left side {user_addr}: {e}"))
        } else if let Some((user, sender)) = self.right_side.as_ref()
            && *user == *user_addr
        {
            sender
                .send(message_as_bytes)
                .map_err(|e| anyhow!("failed to send message to right side {user_addr}: {e}"))
        } else {
            Err(anyhow!("User {user_addr} does not exist!"))
        }
    }

    fn send_message_to_lobby(&self, message: &RpcServerMessage) -> anyhow::Result<()> {
        let message_as_bytes = encode_server_message(message)?;
        if let Some((addr, sender)) = self.left_side.as_ref() {
            sender
                .send(message_as_bytes.clone())
                .map_err(|e| anyhow!("failed to send message to {addr}: {e}"))?;
        }

        if let Some((addr, sender)) = self.right_side.as_ref() {
            sender
                .send(message_as_bytes)
                .map_err(|e| anyhow!("failed to send message to {addr}: {e}"))?;
        }

        Ok(())
    }

    pub(crate) fn broadcast_lobby_state(&self) -> anyhow::Result<()> {
        self.send_message_to_lobby(&RpcServerMessage::LobbyState(
            self.get_current_lobby_state(),
        ))
    }

    pub(crate) fn broadcast_game_state(&self) -> anyhow::Result<()> {
        let state = self.get_current_game_state();
        let left_side_score = 0;
        let right_side_score = 0;
        self.send_message_to_lobby(&RpcServerMessage::GameState {
            state,
            left_side_score,
            right_side_score,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /*
    #[test]
    fn lobby_tests() {
        let dummy_left = UserId::from_str("127.0.0.1:1234").unwrap();
        let dummy_right = UserId::from_str("127.0.0.1:12342").unwrap();

        let mut lobby = LobbySession::new(dummy_left);
        assert_eq!(lobby.get_current_lobby_state(), LobbyState::Waiting);

        lobby.insert_player(dummy_right).unwrap();
        assert_eq!(lobby.get_current_lobby_state(), LobbyState::Running);

        lobby.remove_player(dummy_left).unwrap();
        assert_eq!(lobby.get_current_lobby_state(), LobbyState::Waiting);

        lobby.remove_player(dummy_right).unwrap();
        assert_eq!(lobby.get_current_lobby_state(), LobbyState::Empty);
    }
    */
}
