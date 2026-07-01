use anyhow::{anyhow, bail};
use rpc::{
    HEADER_MESSAGE, LobbyId, PlayerSide, RPSGameState, RPSWinState, RpcClientMessage,
    RpcServerMessage, ScoreSize, UserId, YesOrNo, encode_server_message,
};
use std::error::Error;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use uuid::Uuid;

use crate::{
    lobby::{LobbyError, LobbySession},
    rps::GameSession,
};

use crate::lobby::UserSender;
use rpc::LobbyState;

#[derive(Debug, Clone)]
pub struct UserRPCMessage {
    pub message: RpcClientMessage,
    pub send_addr: UserId,
}

struct UserData {
    lobby_id: Option<LobbyId>,
    player_side: Option<PlayerSide>,
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
    disconnected_user_list: HashMap<UserId, ScoreSize>,
    connected_user_list: HashMap<UserId, UserData>,
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

    fn connect_user(&mut self, addr: UserId, sender: UserSender) -> anyhow::Result<()> {
        if let Some(existing) = self.connected_user_list.get(&addr) {
            bail!("Can't double connect a user to the server");
        }

        // restore score if user has connected before
        let user_score = self
            .disconnected_user_list
            .remove(&addr)
            .unwrap_or_default();

        self.connected_user_list.insert(
            addr,
            UserData {
                lobby_id: None,
                player_side: None,
                sender,
                score: user_score,
            },
        );

        println!("Connected user {addr} setup");

        Ok(())
    }

    fn put_connected_user_in_lobby(
        &mut self,
        addr: UserId,
    ) -> anyhow::Result<(PlayerSide, LobbyId)> {
        let sender = self
            .connected_user_list
            .get(&addr)
            .expect("If user is connected, they should be in connected list")
            .sender
            .clone();

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
            let (player_side, state) = lobby.insert_player((addr, sender))?;
            self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
            println!("Lobby {lobby_id} should now be running: {state:?}");
            assert_eq!(state, LobbyState::Running);
            (player_side, lobby_id)
        } else {
            let lobby_id = Uuid::new_v4();
            let lobby = LobbySession::new((addr, sender));
            println!(
                "Lobby {lobby_id} should now be waiting: {:?}",
                lobby.get_current_lobby_state()
            );
            self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
            // Left is default when creating a new lobby
            (PlayerSide::Left, lobby_id)
        };

        if let Some(user) = self.connected_user_list.get_mut(&addr) {
            user.player_side = Some(player_side.clone());
            user.lobby_id = Some(lobby_id);
        }

        // send our lobby id first
        let lobby_init = RpcServerMessage::LobbyInit(player_side.clone(), addr, lobby_id);
        let lobby_session = self
            .lobby_list
            .get(&lobby_id)
            .expect("Should be in lobby list")
            .session();
        println!("Lobby {lobby_id} init: {lobby_init:?} session: {lobby_session:?}");
        lobby_session.send_message_to_user(&lobby_init, &addr)?;

        lobby_session.broadcast_lobby_state()?;
        lobby_session.broadcast_game_state()?;

        println!("Connected user {addr} to lobby {lobby_init:?}");

        Ok((player_side, lobby_id))
    }

    fn disconnect_user(&mut self, addr: UserId) -> anyhow::Result<()> {
        let Some(user_data) = self.connected_user_list.get_mut(&addr) else {
            bail!("Cannot disconnect user {addr} from the server if it's not connected");
        };

        let score = user_data.score;
        if let Some(lobby_id) = user_data.lobby_id {
            // removed player lobby is now empty
            let message = encode_server_message(&RpcServerMessage::LobbyState(LobbyState::Empty))?;
            // this can fail if the player totally disconnected
            let _ = user_data.sender.send(message);
            user_data.lobby_id = None;
            user_data.player_side = None;

            // Pop lobby entry off, we can add it back in later
            let Some(lobby_entry) = self.lobby_list.remove(&lobby_id) else {
                bail!("Lobby {lobby_id} is invalid.");
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
                            lobby.broadcast_lobby_state()?;
                            lobby.broadcast_game_state()?;
                            self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
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
                            finished.lobby_session.broadcast_lobby_state()?;
                            finished.lobby_session.broadcast_game_state()?;
                            self.lobby_list
                                .insert(lobby_id, LobbyEntry::Waiting(finished.lobby_session));
                        }
                        LobbyState::Empty => {
                            // delete lobby
                            println!(
                                "Lobby {lobby_id} is now destroyed, both players rejected continuing to play"
                            );
                        }
                        LobbyState::Finished => {
                            unreachable!(
                                "If we remove a player, the lobby {lobby_id:?} can't be finished"
                            );
                        }
                        LobbyState::Running => unreachable!(
                            "If we remove a player, the lobby {lobby_id:?} can't be running"
                        ),
                    }
                }
            }
        }

        let Some(user_data) = self.connected_user_list.remove(&addr) else {
            return Ok(());
        };

        self.disconnected_user_list
            .insert(addr, user_data.score.max(score));

        Ok(())
    }

    fn remove_connected_user_from_lobby(&mut self, addr: UserId) -> anyhow::Result<()> {
        if let Some(user) = self.connected_user_list.get_mut(&addr) {
            user.lobby_id = None;
            user.player_side = None;
            let leaving_message =
                encode_server_message(&RpcServerMessage::LobbyState(LobbyState::Empty))?;
            user.sender.send(leaving_message)?;
            Ok(())
        } else {
            bail!("User {addr} is not actually connected")
        }
    }

    fn handle_user_rpc(&mut self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        let user = self
            .connected_user_list
            .get(&user_rpc_message.send_addr)
            .ok_or_else(|| anyhow!("user not found"))?;

        let lobby_id = user.lobby_id;
        if let Some(lobby_id) = lobby_id {
            let lobby_entry = self.lobby_list.remove(&lobby_id).unwrap();
            match lobby_entry {
                LobbyEntry::Waiting(lobby) => {
                    // ignore most messages while waiting
                    self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
                }

                LobbyEntry::Running(mut lobby) => match user_rpc_message.message {
                    RpcClientMessage::GameInput(input) => {
                        println!(
                            "Lobby input: {input:?} sent from {:?}",
                            user_rpc_message.send_addr
                        );
                        println!("Lobby session: {lobby:?}");
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
                                        if let Some(user) =
                                            self.connected_user_list.get_mut(&winner)
                                        {
                                            user.score += 1;
                                        }
                                    }
                                }
                                RPSWinState::Right => {
                                    if let Some(winner) = lobby.get_right() {
                                        if let Some(user) =
                                            self.connected_user_list.get_mut(&winner)
                                        {
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

                            println!("Finished lobby {lobby_id:?}");

                            match (
                                finished.left_side_continue.clone(),
                                finished.right_side_continue.clone(),
                            ) {
                                (Some(YesOrNo::Yes), Some(YesOrNo::Yes)) => {
                                    finished.lobby_session.reset_lobby();
                                    self.lobby_list.insert(
                                        lobby_id,
                                        LobbyEntry::Running(finished.lobby_session),
                                    );
                                }

                                (Some(YesOrNo::No), Some(YesOrNo::No)) => {
                                    self.remove_connected_user_from_lobby(
                                        finished
                                            .lobby_session
                                            .get_left()
                                            .expect("Should have both players"),
                                    )?;
                                    self.remove_connected_user_from_lobby(
                                        finished
                                            .lobby_session
                                            .get_right()
                                            .expect("Should have both players"),
                                    )?;
                                    // delete lobby entirely
                                    println!(
                                        "Finished Lobby {lobby_id} is now destroyed, both players rejected continuing to play"
                                    );
                                }

                                (Some(YesOrNo::No), Some(YesOrNo::Yes)) => {
                                    let leaving = finished
                                        .lobby_session
                                        .get_left()
                                        .expect("Should have both players in here");
                                    let (_, state) =
                                        finished.lobby_session.remove_player(leaving)?;
                                    assert_eq!(state, LobbyState::Waiting);

                                    self.remove_connected_user_from_lobby(leaving)?;

                                    finished.lobby_session.reset_lobby();
                                    self.lobby_list.insert(
                                        lobby_id,
                                        LobbyEntry::Waiting(finished.lobby_session),
                                    );
                                }

                                (Some(YesOrNo::Yes), Some(YesOrNo::No)) => {
                                    let leaving = finished
                                        .lobby_session
                                        .get_right()
                                        .expect("Should have both players in here");
                                    let (_, state) =
                                        finished.lobby_session.remove_player(leaving)?;
                                    assert_eq!(state, LobbyState::Waiting);

                                    self.remove_connected_user_from_lobby(leaving)?;

                                    finished.lobby_session.reset_lobby();
                                    self.lobby_list.insert(
                                        lobby_id,
                                        LobbyEntry::Waiting(finished.lobby_session),
                                    );
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
            // if lobby session hasn't been drop, rebroadcast state
            if let Some(lobby_entry) = self.lobby_list.get(&lobby_id) {
                lobby_entry.session().broadcast_lobby_state()?;
                lobby_entry.session().broadcast_game_state()?;
            }
        } else {
            match user_rpc_message.message {
                RpcClientMessage::JoinLobby => {
                    self.put_connected_user_in_lobby(user_rpc_message.send_addr)?;
                }
                _ => {}
            }
        }
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

    pub async fn connect_user(&self, addr: UserId, sender: UserSender) -> anyhow::Result<()> {
        self.server_state.lock().await.connect_user(addr, sender)
    }

    pub async fn disconnect_user(&self, addr: UserId) -> anyhow::Result<()> {
        self.server_state.lock().await.disconnect_user(addr)
    }

    pub async fn handle_user_rpc(&self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        self.server_state
            .lock()
            .await
            .handle_user_rpc(user_rpc_message)
    }
}
