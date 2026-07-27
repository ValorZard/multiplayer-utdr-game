use crate::actor::define_actor;
use crate::battle::{GameError, GameSession};
use shared::game::{BattleGameState, MoveGameState, MoveInputState, PlayerSide, TurnAction};
use shared::rpc::{
    InputSequence, LobbyId, LobbyState, ReliableRpcServerMessage, UnreliableRpcServerMessage,
    UserId,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Duration;
use tracing::warn;

pub type UserReliableSender = UnboundedSender<ReliableRpcServerMessage>;
pub type UserUnreliableSender = UnboundedSender<UnreliableRpcServerMessage>;

type PlayerDataTuple = (UserId, UserReliableSender, UserUnreliableSender);

/// How often the lobby pushes state to its players, ~1/30s.
const TICK_PERIOD: Duration = Duration::from_millis(33);

/// How many messages may queue up for a lobby before senders start waiting.
const MESSAGE_CAPACITY: usize = 32;

define_actor! {
    actor LobbySession;

    message LobbySessionMessage;

    /// Cloneable handle to a running [`LobbySession`].
    pub handle LobbySessionHandle;

    /// Opens a lobby with `left_side` already seated in it.
    spawn with MESSAGE_CAPACITY => fn new(id: LobbyId, left_side: PlayerDataTuple);

    tick every TICK_PERIOD => fn tick();

    ask {
        InsertPlayer => fn insert_player(
            new_player: PlayerDataTuple,
        ) -> Result<(PlayerSide, LobbyState), LobbyError>;

        RemovePlayer => fn remove_player(
            leaving_player: UserId,
        ) -> Result<(PlayerSide, LobbyState), LobbyError>;

        GetPlayerSide => fn get_player_side(user_addr: UserId) -> Result<PlayerSide, LobbyError>;

        GetLeft => fn get_left() -> Result<UserId, LobbyError>;

        GetRight => fn get_right() -> Result<UserId, LobbyError>;

        GetCurrentLobbyState => fn get_current_lobby_state() -> LobbyState;

        SendTurnAction => fn send_turn_action(
            user_addr: UserId,
            action: TurnAction,
        ) -> Result<BattleGameState, LobbyError>;

        #[allow(dead_code)]
        SendReliableMessageToUser => fn send_reliable_message_to_user(
            message: ReliableRpcServerMessage,
            user_addr: UserId,
        ) -> Result<(), LobbyError>;

        #[allow(dead_code)]
        SendUnreliableMessageToUser => fn send_unreliable_message_to_user(
            message: UnreliableRpcServerMessage,
            user_addr: UserId,
        ) -> Result<(), LobbyError>;

        #[allow(dead_code)]
        SendReliableMessageToLobby => fn send_reliable_message_to_lobby(
            message: ReliableRpcServerMessage,
        ) -> Result<(), LobbyError>;

        #[allow(dead_code)]
        SendUnreliableMessageToLobby => fn send_unreliable_message_to_lobby(
            message: UnreliableRpcServerMessage,
        ) -> Result<(), LobbyError>;
    }

    tell {
        SendMoveInput => fn send_move_input(
            user_addr: UserId,
            input: MoveInputState,
            sequence: InputSequence,
            client_send_time_ms: i64,
        );

        ResetLobby => fn reset_lobby();
    }
}

struct LobbySession {
    id: LobbyId,
    left_side: Option<PlayerDataTuple>,
    right_side: Option<PlayerDataTuple>,
    current_round: GameSession,
    receiver: mpsc::Receiver<LobbySessionMessage>,
}

#[derive(Debug, PartialEq)]
pub enum LobbyError {
    SameAddr(UserId),
    AlreadyFull,
    NeverExisted(UserId),
    MessageSendFailed(UserId),
    GameError(GameError),
    SideNotFound(PlayerSide),
    NotRunning,
}

impl std::fmt::Display for LobbyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LobbyError::SameAddr(addr) => write!(
                f,
                "Error! We can't have two players who come from the same addr {addr:?}"
            ),
            LobbyError::AlreadyFull => write!(f, "Error! This lobby is already full!"),
            LobbyError::NeverExisted(addr) => {
                write!(f, "Error! This player {addr:?} never existed here!")
            }
            LobbyError::MessageSendFailed(addr) => write!(
                f,
                "Error! Sending a message to this player {addr:?} failed!"
            ),
            LobbyError::GameError(err) => write!(f, "Game Error: {}", err),
            LobbyError::SideNotFound(side) => {
                write!(f, "UserId for player side not found: {side:?}")
            }
            LobbyError::NotRunning => {
                write!(f, "Error! This lobby is not running!")
            }
        }
    }
}

impl std::error::Error for LobbyError {}

impl LobbySession {
    fn new(
        id: LobbyId,
        left_side: PlayerDataTuple,
        receiver: mpsc::Receiver<LobbySessionMessage>,
    ) -> Self {
        let _ = left_side
            .1
            .send(ReliableRpcServerMessage::LobbyInit(PlayerSide::Left, id));
        Self {
            id,
            left_side: Some(left_side),
            right_side: None,
            current_round: GameSession::new(),
            receiver,
        }
    }

    fn insert_player(
        &mut self,
        new_player: PlayerDataTuple,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        if let Some((player, _, _)) = self.left_side.as_ref()
            && *player == new_player.0
        {
            return Err(LobbyError::SameAddr(*player));
        } else if let Some((player, _, _)) = self.right_side.as_ref()
            && *player == new_player.0
        {
            return Err(LobbyError::SameAddr(*player));
        }

        if self.left_side.is_none() {
            let _ = new_player.1.send(ReliableRpcServerMessage::LobbyInit(
                PlayerSide::Left,
                self.id,
            ));
            self.left_side = Some(new_player);
            let _ = self.send_reliable_message_to_lobby(ReliableRpcServerMessage::GameState(
                self.current_round.compute_state(),
            ));
            Ok((PlayerSide::Left, self.get_current_lobby_state()))
        } else if self.right_side.is_none() {
            let _ = new_player.1.send(ReliableRpcServerMessage::LobbyInit(
                PlayerSide::Right,
                self.id,
            ));
            self.right_side = Some(new_player);
            let _ = self.send_reliable_message_to_lobby(ReliableRpcServerMessage::GameState(
                self.current_round.compute_state(),
            ));
            Ok((PlayerSide::Right, self.get_current_lobby_state()))
        } else {
            Err(LobbyError::AlreadyFull)
        }
    }

    fn remove_player(
        &mut self,
        leaving_player: UserId,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        // clear lobby state if we're removing players
        self.reset_lobby();
        if let Some((addr, _, _)) = self.left_side
            && addr == leaving_player
        {
            let _ = self.left_side.take();
            return Ok((PlayerSide::Left, self.get_current_lobby_state()));
        } else if let Some((addr, _, _)) = self.right_side
            && addr == leaving_player
        {
            let _ = self.right_side.take();
            return Ok((PlayerSide::Right, self.get_current_lobby_state()));
        }

        Err(LobbyError::NeverExisted(leaving_player))
    }

    #[allow(dead_code)]
    fn get_left(&self) -> Result<UserId, LobbyError> {
        if let Some((player, _, _)) = self.left_side.as_ref() {
            return Ok(*player);
        }
        Err(LobbyError::SideNotFound(PlayerSide::Left))
    }

    fn get_right(&self) -> Result<UserId, LobbyError> {
        if let Some((player, _, _)) = self.right_side.as_ref() {
            return Ok(*player);
        }
        Err(LobbyError::SideNotFound(PlayerSide::Right))
    }

    fn reset_lobby(&mut self) {
        self.current_round = GameSession::new();
    }

    fn get_current_lobby_state(&self) -> LobbyState {
        if matches!(
            self.current_round.compute_state(),
            BattleGameState::Win { .. }
        ) {
            LobbyState::Finished
        } else if self.left_side.is_some() && self.right_side.is_some() {
            LobbyState::Running
        } else if self.left_side.is_none() && self.right_side.is_none() {
            LobbyState::Empty
        } else {
            LobbyState::Waiting
        }
    }

    pub fn get_current_game_state(&self) -> BattleGameState {
        self.current_round.compute_state()
    }

    pub fn get_player_side(&self, addr: UserId) -> Result<PlayerSide, LobbyError> {
        if self.get_left() == Ok(addr) {
            Ok(PlayerSide::Left)
        } else if self.get_right() == Ok(addr) {
            Ok(PlayerSide::Right)
        } else {
            Err(LobbyError::NeverExisted(addr))
        }
    }

    fn send_reliable_message_to_user(
        &self,
        message: ReliableRpcServerMessage,
        user_addr: UserId,
    ) -> Result<(), LobbyError> {
        if let Some((user, sender, _)) = self.left_side.as_ref()
            && *user == user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(user_addr))
        } else if let Some((user, sender, _)) = self.right_side.as_ref()
            && *user == user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(user_addr))
        } else {
            Err(LobbyError::NeverExisted(user_addr))
        }
    }

    fn send_unreliable_message_to_user(
        &self,
        message: UnreliableRpcServerMessage,
        user_addr: UserId,
    ) -> Result<(), LobbyError> {
        if let Some((user, _, sender)) = self.left_side.as_ref()
            && *user == user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(user_addr))
        } else if let Some((user, _, sender)) = self.right_side.as_ref()
            && *user == user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(user_addr))
        } else {
            Err(LobbyError::NeverExisted(user_addr))
        }
    }

    #[allow(dead_code)]
    fn send_reliable_message_to_side(
        &self,
        message: ReliableRpcServerMessage,
        player_side: PlayerSide,
    ) -> Result<(), LobbyError> {
        match player_side {
            PlayerSide::Left => {
                if let Some((user, sender, _)) = self.left_side.as_ref() {
                    return sender
                        .send(message)
                        .map_err(|_e| LobbyError::MessageSendFailed(*user));
                }
            }
            PlayerSide::Right => {
                if let Some((user, sender, _)) = self.right_side.as_ref() {
                    return sender
                        .send(message)
                        .map_err(|_e| LobbyError::MessageSendFailed(*user));
                }
            }
        }
        Err(LobbyError::SideNotFound(player_side))
    }

    fn send_unreliable_message_to_side(
        &mut self,
        message: UnreliableRpcServerMessage,
        player_side: PlayerSide,
    ) -> Result<(), LobbyError> {
        match player_side {
            PlayerSide::Left => {
                if let Some((user, _, sender)) = self.left_side.as_ref() {
                    return sender
                        .send(message)
                        .map_err(|_e| LobbyError::MessageSendFailed(*user));
                }
            }
            PlayerSide::Right => {
                if let Some((user, _, sender)) = self.right_side.as_ref() {
                    return sender
                        .send(message)
                        .map_err(|_e| LobbyError::MessageSendFailed(*user));
                }
            }
        }
        Err(LobbyError::SideNotFound(player_side))
    }

    fn send_reliable_message_to_lobby(
        &self,
        message: ReliableRpcServerMessage,
    ) -> Result<(), LobbyError> {
        if let Some((addr, sender, _)) = self.left_side.as_ref() {
            sender
                .send(message.clone())
                .map_err(|_e| LobbyError::MessageSendFailed(*addr))?;
        }

        if let Some((addr, sender, _)) = self.right_side.as_ref() {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(*addr))?;
        }

        Ok(())
    }

    fn send_unreliable_message_to_lobby(
        &self,
        message: UnreliableRpcServerMessage,
    ) -> Result<(), LobbyError> {
        if let Some((addr, _, sender)) = self.left_side.as_ref() {
            sender
                .send(message.clone())
                .map_err(|_e| LobbyError::MessageSendFailed(*addr))?;
        }

        if let Some((addr, _, sender)) = self.right_side.as_ref() {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(*addr))?;
        }

        Ok(())
    }

    fn broadcast_lobby_state(&self) -> Result<(), LobbyError> {
        self.send_reliable_message_to_lobby(ReliableRpcServerMessage::LobbyState(
            self.get_current_lobby_state(),
        ))
    }

    fn broadcast_game_state(&mut self) -> Result<(), LobbyError> {
        let state = self.get_current_game_state();
        self.send_reliable_message_to_lobby(ReliableRpcServerMessage::GameState(state))
    }

    fn step_game(&mut self) -> Result<MoveGameState, LobbyError> {
        // don't step if the lobby is not running
        if self.get_current_lobby_state() == LobbyState::Running {
            self.current_round.step();
            Ok(self.current_round.get_move_state())
        } else {
            Err(LobbyError::NotRunning)
        }
    }

    fn send_turn_action(
        &mut self,
        player_addr: UserId,
        action: TurnAction,
    ) -> Result<BattleGameState, LobbyError> {
        // Rejections (unknown player, dead player, wrong phase) go back to the
        // caller instead of erroring here, so a stray message can't kill the
        // whole lobby actor.
        let player_side = self.get_player_side(player_addr)?;
        self.current_round
            .set_turn_action(player_side, action)
            .map_err(LobbyError::GameError)
    }

    fn send_move_input(
        &mut self,
        player_addr: UserId,
        input: MoveInputState,
        sequence: InputSequence,
        client_send_time_ms: i64,
    ) {
        // Nobody is waiting on a reply here, so an input from someone who isn't
        // in this lobby is dropped rather than taking the actor down with it.
        let Ok(player_side) = self.get_player_side(player_addr) else {
            warn!("dropping move input from player {player_addr:?} who isn't in this lobby");
            return;
        };

        match player_side {
            PlayerSide::Left => {
                self.current_round
                    .set_left_move_input(input, sequence, client_send_time_ms)
            }
            PlayerSide::Right => {
                self.current_round
                    .set_right_move_input(input, sequence, client_send_time_ms)
            }
        }
    }

    /// Sends out gamestate updates and steps the simulation. Called by the run
    /// loop every [`TICK_PERIOD`].
    fn tick(&mut self) {
        // A failed send means that player's connection is already gone. The
        // disconnect path takes them out of the lobby, so drop the message
        // rather than taking the whole session down over it.
        let _ = self.broadcast_lobby_state();
        let _ = self.broadcast_game_state();

        let Ok(move_state) = self.step_game() else {
            return;
        };

        // Each side gets its own acknowledgement bookkeeping back, so the two
        // messages differ by more than just the recipient.
        let current_tick = self.current_round.get_tick();
        let left_ack = self.current_round.get_left_remote_clock_ack();
        let left_client_time_ms = self.current_round.get_left_last_client_time_ms();
        let right_ack = self.current_round.get_right_remote_clock_ack();
        let right_client_time_ms = self.current_round.get_right_last_client_time_ms();

        // broadcast state to both right and left (it's okay if these fail)
        let _ = self.send_unreliable_message_to_side(
            UnreliableRpcServerMessage::GameState {
                state: move_state.clone(),
                acknowledged_sequence: left_ack,
                tick: current_tick,
                echo_client_time_ms: left_client_time_ms,
            },
            PlayerSide::Left,
        );
        let _ = self.send_unreliable_message_to_side(
            UnreliableRpcServerMessage::GameState {
                state: move_state,
                acknowledged_sequence: right_ack,
                tick: current_tick,
                echo_client_time_ms: right_client_time_ms,
            },
            PlayerSide::Right,
        );
    }
}
