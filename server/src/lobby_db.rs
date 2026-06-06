use anyhow::{anyhow, bail};
use rpc::{LobbyId, PlayerSide, RPSGameState, RPSWinState, RpcClientMessage, RpcServerMessage, ScoreSize, YesOrNo};
use std::error::Error;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use tokio_tungstenite::tungstenite::{Message as WsMessage, Message};
use uuid::Uuid;

use crate::{
    encode_server_message,
    lobby::{LobbyError, LobbySession},
    rps::GameSession,
};

use rpc::LobbyState;

type UserSender = UnboundedSender<WsMessage>;

#[derive(Debug, Clone)]
pub struct UserRPCMessage {
    pub message: RpcClientMessage,
    pub send_addr: SocketAddr,
}

struct UserData {
    lobby_id: LobbyId,
    player_side: PlayerSide,
    sender: UserSender,
    score: ScoreSize,
}

struct FinishedLobbySession {
    lobby_session: LobbySession,
    left_side_continue: Option<YesOrNo>,
    right_side_continue: Option<YesOrNo>,
}

enum LobbyEntry {
    Waiting(LobbySession),
    Running(LobbySession),
    Finished(FinishedLobbySession),
}

impl LobbyEntry {
    fn session(&self) -> &LobbySession {
        match self {
            LobbyEntry::Waiting(s) => s,
            LobbyEntry::Running(s) => s,
            LobbyEntry::Finished(f) => &f.lobby_session,
        }
    }

    fn lobby_state(&self) -> LobbyState {
        match self {
            LobbyEntry::Waiting(_) => LobbyState::Waiting,
            LobbyEntry::Running(_) => LobbyState::Running,
            LobbyEntry::Finished(_) => LobbyState::Finished,
        }
    }
}

struct ServerStateInner {
    disconnected_user_list: HashMap<SocketAddr, ScoreSize>,
    connected_user_list: HashMap<SocketAddr, UserData>,
    lobby_list: HashMap<LobbyId, LobbyEntry>,
}

impl ServerStateInner {
    fn new() -> Self {
        Self {
            disconnected_user_list: HashMap::new(),
            connected_user_list: HashMap::new(),
            lobby_list: HashMap::new(),
        }
    }

    fn send_message_to_user(
        &self,
        message: &RpcServerMessage,
        user_addr: &SocketAddr,
    ) -> anyhow::Result<()> {
        let bytes = encode_server_message(message)?;
        let user = self
            .connected_user_list
            .get(user_addr)
            .ok_or_else(|| anyhow!("user {user_addr} not found"))?;

        user.sender
            .send(WsMessage::Binary(bytes.to_vec().into()))
            .map_err(|e| anyhow!("failed to send message to {user_addr}: {e}"))?;

        Ok(())
    }

    fn send_message_to_lobby(
        &self,
        message: &RpcServerMessage,
        lobby_id: &LobbyId,
    ) -> anyhow::Result<()> {
        let lobby = self
            .lobby_list
            .get(lobby_id)
            .ok_or_else(|| anyhow!("lobby {lobby_id} not found"))?;

        if let Some(left) = lobby.session().get_left() {
            self.send_message_to_user(message, &left)?;
        }

        if let Some(right) = lobby.session().get_right() {
            self.send_message_to_user(message, &right)?;
        }

        Ok(())
    }

    fn broadcast_lobby_state(&self, lobby_id: LobbyId) -> anyhow::Result<()> {
        let lobby = self
            .lobby_list
            .get(&lobby_id)
            .ok_or_else(|| anyhow!("lobby {lobby_id} not found"))?;

        self.send_message_to_lobby(
            &RpcServerMessage::LobbyState(lobby.lobby_state()),
            &lobby_id,
        )
    }

    fn broadcast_game_state(&self, lobby_id: LobbyId) -> anyhow::Result<()> {
        let lobby = self
            .lobby_list
            .get(&lobby_id)
            .ok_or_else(|| anyhow!("lobby {lobby_id} not found"))?;

        let state = lobby.session().get_current_game_state();
        let left_side_score = if let Some(left_addr) = lobby.session().get_left() {
            self.connected_user_list.get(&left_addr).unwrap().score
        } else {
            0
        };
        let right_side_score = if let Some(right_addr) = lobby.session().get_right() {
            self.connected_user_list.get(&right_addr).unwrap().score
        } else {
            0
        };
        self.send_message_to_lobby(&RpcServerMessage::GameState{state, left_side_score, right_side_score}, &lobby_id)
    }

    fn insert_user(
        &mut self,
        addr: SocketAddr,
        sender: UserSender,
    ) -> anyhow::Result<(PlayerSide, LobbyId)> {
        if let Some(existing) = self.connected_user_list.get(&addr) {
            return Ok((existing.player_side.clone(), existing.lobby_id));
        }

        // TODO: This is O(n), not O(log n)
        let waiting_lobby_id =
            self.lobby_list
                .iter()
                .find_map(|(lobby_id, lobby_entry)| match lobby_entry {
                    LobbyEntry::Waiting(_) => Some(*lobby_id),
                    _ => None,
                });

        let (player_side, lobby_id) = if let Some(lobby_id) = waiting_lobby_id {
            let lobby_entry = self.lobby_list.remove(&lobby_id).unwrap();
            let LobbyEntry::Waiting(mut lobby) = lobby_entry else {
                unreachable!(
                    "This should be waiting since we used find map to find something that matched what we want."
                );
            };
            lobby.reset_lobby();
            let (player_side, state) = lobby.insert_player(addr)?;
            self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
            println!("Lobby {lobby_id} should now be running: {state:?}");
            assert_eq!(state, LobbyState::Running);
            (player_side, lobby_id)
        } else {
            let lobby_id = Uuid::new_v4();
            let lobby = LobbySession::new(addr);
            println!(
                "Lobby {lobby_id} should now be waiting: {:?}",
                lobby.get_current_lobby_state()
            );
            self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
            // Left is default when creating a new lobby
            (PlayerSide::Left, lobby_id)
        };

        // restore score if user has connected before
        let user_score = self.disconnected_user_list.remove(&addr).unwrap_or_default();

        self.connected_user_list.insert(
            addr,
            UserData {
                lobby_id,
                player_side: player_side.clone(),
                sender,
                score: 0,
            },
        );

        self.broadcast_lobby_state(lobby_id)?;
        self.broadcast_game_state(lobby_id)?;

        Ok((player_side, lobby_id))
    }

    fn remove_user(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let Some(user_data) = self.connected_user_list.remove(&addr) else {
            return Ok(());
        };

        // removed player lobby is now empty
        let message = encode_server_message(&RpcServerMessage::LobbyState(LobbyState::Empty))?;
        // this can fail if the player totally disconnected
        let _ = user_data.sender.send(WsMessage::Binary(message.to_vec().into()));

        let lobby_id = user_data.lobby_id;
        // Pop lobby entry off, we can add it back in later
        let Some(lobby_entry) = self.lobby_list.remove(&lobby_id) else {
            return Ok(());
        };

        match lobby_entry {
            LobbyEntry::Waiting(mut lobby) => {
                let (_, state) = lobby.remove_player(addr)?;

                match state {
                    LobbyState::Empty => {
                        // delete lobby
                        println!("Waiting Lobby {lobby_id} is now destroyed, lobby was empty");
                    }
                    _ => unreachable!(
                        "Since we only have two players in lobby {lobby_id:?}, if we remove a player from a waiting lobby, its empty and can be deleted."
                    ),
                }
            }

            LobbyEntry::Running(mut lobby) => {
                let (_, state) = lobby.remove_player(addr)?;

                match state {
                    LobbyState::Waiting => {
                        self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
                        self.broadcast_lobby_state(lobby_id)?;
                        self.broadcast_game_state(lobby_id)?;
                    }
                    LobbyState::Empty => {
                        // delete lobby
                        println!("Running Lobby {lobby_id} is now destroyed, lobby was empty");
                    }
                    _ => unreachable!(),
                }
            }

            LobbyEntry::Finished(mut finished) => {
                let (_, state) = finished.lobby_session.remove_player(addr)?;

                match state {
                    LobbyState::Waiting => {
                        self.lobby_list
                            .insert(lobby_id, LobbyEntry::Waiting(finished.lobby_session));
                        self.broadcast_lobby_state(lobby_id)?;
                        self.broadcast_game_state(lobby_id)?;
                    }
                    LobbyState::Empty => {
                        // delete lobby
                        println!(
                            "Lobby {lobby_id} is now destroyed, both players rejected continuing to play"
                        );
                    }
                    _ => unreachable!(
                        "If we remove a player, the lobby {lobby_id:?} can't be running"
                    ),
                }
            }
        }

        // add disconnected user to list, store score
        self.disconnected_user_list.insert(addr, user_data.score);

        Ok(())
    }

    fn handle_user_rpc(&mut self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        let user = self
            .connected_user_list
            .get(&user_rpc_message.send_addr)
            .ok_or_else(|| anyhow!("user not found"))?;

        let lobby_id = user.lobby_id;

        let lobby_entry = self
            .lobby_list
            .remove(&lobby_id)
            .ok_or_else(|| anyhow!("lobby {lobby_id} not found"))?;

        match lobby_entry {
            LobbyEntry::Waiting(lobby) => {
                // ignore most messages while waiting
                self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
            }

            LobbyEntry::Running(mut lobby) => match user_rpc_message.message {
                RpcClientMessage::GameInput(input) => {
                    let side = lobby
                        .get_player_side(user_rpc_message.send_addr)
                        .ok_or_else(|| anyhow!("player has no side in running lobby"))?;

                    let current_state = match side {
                        PlayerSide::Left => lobby.set_left_input(input)?,
                        PlayerSide::Right => lobby.set_right_input(input)?,
                    };

                    if let RPSGameState::Win { state, .. } = current_state.clone() {
                        match state {
                            RPSWinState::Left => {
                                if let Some(winner) = lobby.get_left() {
                                    if let Some(user) = self.connected_user_list.get_mut(&winner) {
                                        user.score += 1;
                                    }
                                }
                            }
                            RPSWinState::Right => {
                                if let Some(winner) = lobby.get_right() {
                                    if let Some(user) = self.connected_user_list.get_mut(&winner) {
                                        user.score += 1;
                                    }
                                }
                            }
                            RPSWinState::Tie => {}
                        }

                        self.lobby_list.insert(
                            lobby_id,
                            LobbyEntry::Finished(FinishedLobbySession {
                                lobby_session: lobby,
                                left_side_continue: None,
                                right_side_continue: None,
                            }),
                        );
                    } else {
                        self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
                    }
                }
                _ => {
                    self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
                }
            },

            LobbyEntry::Finished(mut finished) => {
                match user_rpc_message.message {
                    RpcClientMessage::ContinueRound(vote) => {
                        let side = finished
                            .lobby_session
                            .get_player_side(user_rpc_message.send_addr)
                            .ok_or_else(|| anyhow!("player has no side in finished lobby"))?;

                        match side {
                            PlayerSide::Left => finished.left_side_continue = Some(vote),
                            PlayerSide::Right => finished.right_side_continue = Some(vote),
                        }

                        match (
                            finished.left_side_continue.clone(),
                            finished.right_side_continue.clone(),
                        ) {
                            (Some(YesOrNo::Yes), Some(YesOrNo::Yes)) => {
                                finished.lobby_session.reset_lobby();
                                self.lobby_list
                                    .insert(lobby_id, LobbyEntry::Running(finished.lobby_session));
                            }

                            (Some(YesOrNo::No), Some(YesOrNo::No)) => {
                                // delete lobby entirely
                                println!(
                                    "Finished Lobby {lobby_id} is now destroyed, both players rejected continuing to play"
                                );
                            }

                            (Some(YesOrNo::No), Some(YesOrNo::Yes)) => {
                                let leaving = finished.lobby_session.get_left().unwrap();
                                self.remove_user(leaving)?;

                                self.lobby_list
                                    .insert(lobby_id, LobbyEntry::Waiting(finished.lobby_session));
                            }

                            (Some(YesOrNo::Yes), Some(YesOrNo::No)) => {
                                let leaving = finished.lobby_session.get_right().unwrap();
                                self.remove_user(leaving)?;

                                self.lobby_list
                                    .insert(lobby_id, LobbyEntry::Waiting(finished.lobby_session));
                            }

                            _ => {
                                self.lobby_list
                                    .insert(lobby_id, LobbyEntry::Finished(finished));
                            }
                        }
                    }
                    _ => {
                        // ignore other messages now that we're finished
                        self.lobby_list
                            .insert(lobby_id, LobbyEntry::Finished(finished));
                    }
                }
            }
        }
        self.broadcast_lobby_state(lobby_id)?;
        self.broadcast_game_state(lobby_id)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerState {
    server_state: Arc<Mutex<ServerStateInner>>,
}

impl ServerState {
    pub fn new() -> ServerState {
        Self {
            server_state: Arc::new(Mutex::new(ServerStateInner::new())),
        }
    }

    pub async fn insert_user(
        &self,
        addr: SocketAddr,
        sender: UserSender,
    ) -> anyhow::Result<(PlayerSide, LobbyId)> {
        self.server_state.lock().await.insert_user(addr, sender)
    }

    pub async fn remove_user(&self, addr: SocketAddr) -> anyhow::Result<()> {
        self.server_state.lock().await.remove_user(addr)
    }

    pub async fn handle_user_rpc(&self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        self.server_state
            .lock()
            .await
            .handle_user_rpc(user_rpc_message)
    }
}
