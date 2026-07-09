use crate::rps::{GameError, GameSession};
use rpc::{
    InputSequence, LobbyId, LobbyState, MoveGameState, MoveInputState, PlayerSide, RPSGameState,
    RPSWinState, ReliableRpcServerMessage, TurnInput, UnreliableRpcServerMessage, UserId,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

pub type UserReliableSender = UnboundedSender<ReliableRpcServerMessage>;
pub type UserUnreliableSender = UnboundedSender<UnreliableRpcServerMessage>;

pub enum LobbySessionMessage {
    InsertPlayer(
        PlayerDataTuple,
        oneshot::Sender<Result<(PlayerSide, LobbyState), LobbyError>>,
    ),
    RemovePlayer(
        UserId,
        oneshot::Sender<Result<(PlayerSide, LobbyState), LobbyError>>,
    ),
    RPSInput(UserId, TurnInput, oneshot::Sender<RPSGameState>),
    MoveInput(UserId, MoveInputState, InputSequence),
    SendReliableMessageToUser(
        ReliableRpcServerMessage,
        UserId,
        oneshot::Sender<Result<(), LobbyError>>,
    ),
    SendUnreliableMessageToUser(
        UnreliableRpcServerMessage,
        UserId,
        oneshot::Sender<Result<(), LobbyError>>,
    ),
    SendReliableMessageToLobby(
        ReliableRpcServerMessage,
        oneshot::Sender<Result<(), LobbyError>>,
    ),
    SendUnreliableMessageToLobby(
        UnreliableRpcServerMessage,
        oneshot::Sender<Result<(), LobbyError>>,
    ),
    LobbyState(oneshot::Sender<LobbyState>),
    GetPlayerSide(UserId, oneshot::Sender<Result<PlayerSide, LobbyError>>),
    GetUserId(PlayerSide, oneshot::Sender<Result<UserId, LobbyError>>),
    ResetLobby,
}

type PlayerDataTuple = (UserId, UserReliableSender, UserUnreliableSender);

struct LobbySession {
    id: LobbyId,
    left_side: Option<PlayerDataTuple>,
    right_side: Option<PlayerDataTuple>,
    current_round: GameSession,
    winner: Option<RPSWinState>,
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
            winner: None,
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
            let _ = self.send_reliable_message_to_lobby(ReliableRpcServerMessage::GameState {
                state: self.current_round.compute_state(),
                left_side_score: 0,
                right_side_score: 0,
            });
            Ok((PlayerSide::Left, self.get_current_lobby_state()))
        } else if self.right_side.is_none() {
            let _ = new_player.1.send(ReliableRpcServerMessage::LobbyInit(
                PlayerSide::Right,
                self.id,
            ));
            self.right_side = Some(new_player);
            let _ = self.send_reliable_message_to_lobby(ReliableRpcServerMessage::GameState {
                state: self.current_round.compute_state(),
                left_side_score: 0,
                right_side_score: 0,
            });
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

    pub fn get_winner(&self) -> Option<RPSWinState> {
        self.winner.clone()
    }

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

    fn set_left_turn_input(&mut self, input: rpc::TurnInput) -> Result<RPSGameState, GameError> {
        let state = self.current_round.set_left_turn_input(input)?;
        if let RPSGameState::Win { state, .. } = state.clone() {
            self.winner = Some(state);
        }
        Ok(state)
    }

    fn set_right_turn_input(&mut self, input: rpc::TurnInput) -> Result<RPSGameState, GameError> {
        let state = self.current_round.set_right_turn_input(input)?;
        if let RPSGameState::Win { state, .. } = state.clone() {
            self.winner = Some(state);
        }
        Ok(state)
    }

    fn reset_lobby(&mut self) {
        self.winner = None;
        self.current_round = GameSession::new();
    }

    fn get_current_lobby_state(&self) -> LobbyState {
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
        user_addr: &UserId,
    ) -> Result<(), LobbyError> {
        if let Some((user, sender, _)) = self.left_side.as_ref()
            && *user == *user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(*user_addr))
        } else if let Some((user, sender, _)) = self.right_side.as_ref()
            && *user == *user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(*user_addr))
        } else {
            Err(LobbyError::NeverExisted(*user_addr))
        }
    }

    fn send_unreliable_message_to_user(
        &self,
        message: UnreliableRpcServerMessage,
        user_addr: &UserId,
    ) -> Result<(), LobbyError> {
        if let Some((user, _, sender)) = self.left_side.as_ref()
            && *user == *user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(*user_addr))
        } else if let Some((user, _, sender)) = self.right_side.as_ref()
            && *user == *user_addr
        {
            sender
                .send(message)
                .map_err(|_e| LobbyError::MessageSendFailed(*user_addr))
        } else {
            Err(LobbyError::NeverExisted(*user_addr))
        }
    }

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
        &self,
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
        let left_side_score = 0;
        let right_side_score = 0;
        self.send_reliable_message_to_lobby(ReliableRpcServerMessage::GameState {
            state,
            left_side_score,
            right_side_score,
        })
    }

    fn step_game(&mut self) -> MoveGameState {
        self.current_round.step();
        self.current_round.get_move_state()
    }

    fn handle_message(&mut self, message: LobbySessionMessage) -> Result<(), LobbyError> {
        match message {
            LobbySessionMessage::InsertPlayer(new_player, oneshot) => {
                let result = self.insert_player(new_player);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::RemovePlayer(addr, oneshot) => {
                let result = self.remove_player(addr);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::GetPlayerSide(addr, oneshot) => {
                let result = self.get_player_side(addr);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::GetUserId(side, oneshot) => match side {
                PlayerSide::Left => {
                    let _ = oneshot.send(self.get_left());
                }
                PlayerSide::Right => {
                    let _ = oneshot.send(self.get_right());
                }
            },
            LobbySessionMessage::RPSInput(player_addr, input, oneshot) => {
                let player_side = self.get_player_side(player_addr)?;
                let current_state = match player_side {
                    PlayerSide::Left => self
                        .set_left_turn_input(input)
                        .map_err(|e| LobbyError::GameError(e))?,
                    PlayerSide::Right => self
                        .set_right_turn_input(input)
                        .map_err(|e| LobbyError::GameError(e))?,
                };
                let _ = oneshot.send(current_state);
            }
            LobbySessionMessage::MoveInput(player_addr, input, sequence) => {
                let player_side = self.get_player_side(player_addr)?;
                match player_side {
                    PlayerSide::Left => self.current_round.set_left_move_input(input, sequence),
                    PlayerSide::Right => self.current_round.set_right_move_input(input, sequence),
                }
            }
            LobbySessionMessage::SendReliableMessageToUser(message, addr, oneshot) => {
                let result = self.send_reliable_message_to_user(message, &addr);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::SendUnreliableMessageToUser(message, addr, oneshot) => {
                let result = self.send_unreliable_message_to_user(message, &addr);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::SendReliableMessageToLobby(message, oneshot) => {
                let result = self.send_reliable_message_to_lobby(message);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::SendUnreliableMessageToLobby(message, oneshot) => {
                let result = self.send_unreliable_message_to_lobby(message);
                let _ = oneshot.send(result);
            }
            LobbySessionMessage::LobbyState(oneshot) => {
                let _ = oneshot.send(self.get_current_lobby_state());
            }
            LobbySessionMessage::ResetLobby => {
                self.reset_lobby();
            }
        }
        Ok(())
    }
}

async fn run_lobby_session(mut lobby: LobbySession) -> Result<(), LobbyError> {
    // loop every 1/30 seconds or deal with incoming messages
    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(33)); // ~1/30s

    'actor_loop: loop {
        tokio::select! {
            msg = lobby.receiver.recv() => {
                match msg {
                    Some(msg) => {
                        if let Err(e) = lobby.handle_message(msg) {
                            warn!("error handling message: {e:?}");
                            // TODO: For now we just break
                            break 'actor_loop;
                        }
                    }
                    None => break, // sender dropped, end the session
                }
            }
            _ = tick.tick() => {
                // send out gamestate updates
                lobby.broadcast_lobby_state()?;
                lobby.broadcast_game_state()?;
                let move_state = lobby.step_game();
                // broadcast state to both right and left (it's okay if these fail)
                let _ = lobby.send_unreliable_message_to_side(UnreliableRpcServerMessage::MoveGameState {state:move_state.clone(), acknowledged_sequence: lobby.current_round.get_left_processed_input()}, PlayerSide::Left);
                let _ = lobby.send_unreliable_message_to_side(UnreliableRpcServerMessage::MoveGameState {state:move_state, acknowledged_sequence: lobby.current_round.get_right_processed_input()}, PlayerSide::Right);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct LobbySessionHandle {
    sender: mpsc::Sender<LobbySessionMessage>,
}

impl LobbySessionHandle {
    pub fn new(id: LobbyId, left_side: PlayerDataTuple) -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let actor = LobbySession::new(id, left_side, receiver);
        tokio::spawn(async move {
            if let Err(e) = run_lobby_session(actor).await {
                warn!("error handling lobby session: {e:?}");
            }
        });

        Self { sender }
    }

    pub async fn insert_player(
        &self,
        new_player: PlayerDataTuple,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::InsertPlayer(new_player, send);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn remove_player(
        &self,
        leaving_player: UserId,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::RemovePlayer(leaving_player, send);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_player_side(&self, user_id: UserId) -> Result<PlayerSide, LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::GetPlayerSide(user_id, send);
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn send_reliable_message_to_user(
        &self,
        message: ReliableRpcServerMessage,
        user_addr: UserId,
    ) -> Result<(), LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::SendReliableMessageToUser(message, user_addr, send);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn send_reliable_message_to_lobby(
        &self,
        message: ReliableRpcServerMessage,
    ) -> Result<(), LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::SendReliableMessageToLobby(message, send);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn send_unreliable_message_to_user(
        &self,
        message: UnreliableRpcServerMessage,
        user_addr: UserId,
    ) -> Result<(), LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::SendUnreliableMessageToUser(message, user_addr, send);

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn send_rps_input(&self, user_addr: UserId, input: TurnInput) -> RPSGameState {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::RPSInput(user_addr, input, send);
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn send_move_input(
        &self,
        user_addr: UserId,
        input: MoveInputState,
        sequence: InputSequence,
    ) {
        let msg = LobbySessionMessage::MoveInput(user_addr, input, sequence);
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_current_lobby_state(&self) -> LobbyState {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::LobbyState(send);
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn reset_lobby(&self) {
        let msg = LobbySessionMessage::ResetLobby;
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_left(&self) -> Result<UserId, LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::GetUserId(PlayerSide::Left, send);
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_right(&self) -> Result<UserId, LobbyError> {
        let (send, recv) = oneshot::channel();
        let msg = LobbySessionMessage::GetUserId(PlayerSide::Right, send);
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}
