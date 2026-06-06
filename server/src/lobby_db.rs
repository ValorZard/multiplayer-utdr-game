use anyhow::bail;
use rpc::{LobbyId, PlayerSide, RPSGameState, RPSWinState, RpcClientMessage, RpcServerMessage, YesOrNo};
use std::error::Error;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use tokio_tungstenite::tungstenite::{Message as WsMessage, Message};
use uuid::Uuid;

use crate::{
    encode_server_message,
    lobby::{LobbySession, LobbyError},
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
    score: u32,
}

struct FinishedLobbySession {
    lobby_session: LobbySession,
    left_side_continue: Option<YesOrNo>,
    right_side_continue: Option<YesOrNo>,
}

struct ServerStateInner {
    user_list: HashMap<SocketAddr, UserData>,
    running_lobby_list: HashMap<LobbyId, LobbySession>,
    waiting_lobby_list: HashMap<LobbyId, LobbySession>,
    finished_lobby_list: HashMap<LobbyId, FinishedLobbySession>,
}

impl ServerStateInner {
    fn new() -> Self {
        Self {
            user_list: HashMap::new(),
            running_lobby_list: HashMap::new(),
            waiting_lobby_list: HashMap::new(),
            finished_lobby_list: HashMap::new(),
        }
    }

    fn send_message_to_user(
        &self,
        message: &RpcServerMessage,
        user_addr: &SocketAddr,
    ) -> Result<(), SendError<Message>> {
        let bytes = encode_server_message(message).expect("Error serializing LobbyMessage");
        self.user_list
            .get(user_addr)
            .unwrap()
            .sender
            .send(WsMessage::Binary(bytes.to_vec().into()))
    }

    fn send_message_to_lobby(&self, message: &RpcServerMessage, lobby_id: &LobbyId) -> anyhow::Result<()> {
        let lobby = if let Some(lobby_session) = self.running_lobby_list.get(lobby_id) {
            lobby_session
        } else if let Some(lobby_session) = self.waiting_lobby_list.get(lobby_id) {
            lobby_session
        } else if let Some(FinishedLobbySession{lobby_session, ..}) = self.finished_lobby_list.get(lobby_id) {
            lobby_session
        } else {
            anyhow::bail!("Lobby id {} not found", lobby_id);
        };

        if let Some(left_addr) = lobby.get_left() {
            self.send_message_to_user(message, &left_addr)?;
        }

        if let Some(right_addr) = lobby.get_right() {
            self.send_message_to_user(message, &right_addr)?;
        }

        Ok(())
    }

    fn insert_user(&mut self, addr: SocketAddr, sender: UserSender) -> (PlayerSide, LobbyId) {
        if let Some(existing) = self.user_list.get(&addr) {
            return (existing.player_side.clone(), existing.lobby_id);
        }

        let (player_side, lobby_id) =
            if let Some(lobby_id) = self.waiting_lobby_list.keys().next().copied() {
                let mut lobby = self
                    .waiting_lobby_list
                    .remove(&lobby_id)
                    .expect("lobby id came from waiting list keys");
                let (player_side, lobby_state) = lobby
                    .insert_player(addr)
                    .expect("should be successful in starting game");
                // reset state when waiting lobby is now running (handles case of reusing existing lobby)
                lobby.reset_lobby();
                println!("Lobby {lobby_id} should now be running: {lobby_state:?}");
                assert_eq!(lobby_state, LobbyState::Running);
                self.running_lobby_list.insert(lobby_id, lobby);
                (player_side, lobby_id)
            } else {
                let lobby_id = Uuid::new_v4();
                let new_lobby = LobbySession::new(addr);

                let lobby_state = new_lobby.get_current_lobby_state();
                println!("Lobby {lobby_id} should now be waiting: {lobby_state:?}");
                assert_eq!(lobby_state, LobbyState::Waiting);

                self.waiting_lobby_list.insert(lobby_id, new_lobby);
                // default for new lobby is left side
                (PlayerSide::Left, lobby_id)
            };

        self.user_list.insert(
            addr,
            UserData {
                lobby_id,
                player_side: player_side.clone(),
                sender,
                score: 0,
            },
        );

        // update both players with game state once the lobby is full
        if let Some(lobby) = self.running_lobby_list.get(&lobby_id) {
            // update both players with current game state
            let current_game_state = lobby.get_current_game_state();
            // send players the current state of the game
            let state_message = RpcServerMessage::GameState(current_game_state);
            // we should be able to just send to both left and right side
            let _ = self.send_message_to_user(&state_message, &lobby.get_left().unwrap());
            let _ = self.send_message_to_user(&state_message, &lobby.get_right().unwrap());
        }

        (player_side, lobby_id)
    }

    fn remove_user(&mut self, addr: SocketAddr) {
        let Some(user_data) = self.user_list.remove(&addr) else {
            return;
        };

        if let Some(mut lobby) = self.running_lobby_list.remove(&user_data.lobby_id) {
            let (player_side, state) = lobby.remove_player(addr).unwrap();
            assert_eq!(LobbyState::Waiting, state);
            let current_game_state = lobby.get_current_game_state();
            let state_message = RpcServerMessage::GameState(current_game_state);
            // send player that's left the current state of the game
            match player_side {
                PlayerSide::Left => {
                    let _ = self.send_message_to_user(&state_message, &lobby.get_right().unwrap());
                }
                PlayerSide::Right => {
                    let _ = self.send_message_to_user(&state_message, &lobby.get_left().unwrap());
                }
            }

            self.waiting_lobby_list.insert(user_data.lobby_id, lobby);
            println!(
                "Removed player {addr} from running lobby {}, moving lobby to waiting",
                user_data.lobby_id
            );
        } else if let Some(mut lobby) = self.waiting_lobby_list.remove(&user_data.lobby_id) {
            let (side_that_left, state) = lobby.remove_player(addr).unwrap();
            match state {
                LobbyState::Empty => {
                    println!(
                        "Removed player {addr} from waiting lobby {}, lobby deleted",
                        user_data.lobby_id
                    );
                }
                LobbyState::Waiting => {
                    unreachable!(
                        "removing a player from a waiting lobby cannot leave a lobby waiting, there's only a max of 2 players"
                    )
                }
                LobbyState::Running | LobbyState::Finished => unreachable!("removing a player cannot leave a lobby full"),
            }
        }
    }

    fn handle_user_rpc(&mut self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        let Some(user_data) = self.user_list.get(&user_rpc_message.send_addr) else {
            bail!("Expect user to be in user list");
        };

        let lobby_id = user_data.lobby_id;

        let mut lobby_state = LobbyState::Empty;

        if let Some(lobby) = self.running_lobby_list.get_mut(&lobby_id) {
            let player_side = lobby.get_player_side(user_rpc_message.send_addr);

            match user_rpc_message.message {
                RpcClientMessage::GameInput(input) => {
                    let Some(player_side) = player_side else {
                        unreachable!("user in lobby should always have a side");
                    };

                    let current_state = match player_side {
                        PlayerSide::Left => {
                            lobby.set_left_input(input)
                        }
                        PlayerSide::Right => {
                            lobby.set_right_input(input)
                        }
                    }?;
                    println!("{lobby_id}: Current game state: {current_state:?}");
                    lobby_state = lobby.get_current_lobby_state();
                    println!("{lobby_id}: Lobby State: {lobby_state:?}");

                    // Special case: if a lobby is in a win state, pop it out and put it in finished list
                    if let RPSGameState::Win { state, ..} = current_state.clone() {
                        match state {
                            RPSWinState::Left => {
                                let winner = lobby.get_left().unwrap();
                                self.user_list.get_mut(&winner).unwrap().score += 1;
                            }
                            RPSWinState::Right => {
                                let winner = lobby.get_right().unwrap();
                                self.user_list.get_mut(&winner).unwrap().score += 1;
                            }
                            RPSWinState::Tie => {}
                        }
                    }
                    self.send_message_to_lobby(&RpcServerMessage::GameState(current_state), &lobby_id)?;
                }
                _ => {},
            }
        } else if let Some(mut lobby) = self.finished_lobby_list.remove(&lobby_id) {
            let player_side = lobby.lobby_session.get_player_side(user_rpc_message.send_addr).expect("Should be assigned a player side at this point");
            match user_rpc_message.message {
                RpcClientMessage::ContinueRound(yes_or_no) => {
                    match player_side {
                        PlayerSide::Left => {
                            lobby.left_side_continue = Some(yes_or_no);
                        },
                        PlayerSide::Right => {
                            lobby.right_side_continue = Some(yes_or_no);
                        }
                    }
                }
                _ => {}
            }

            // check to see if we've decided on what we are going to do with this lobby yet
            if let Some(left_yes_or_no) = lobby.left_side_continue.clone() && let Some(right_yes_or_no) = lobby.right_side_continue.clone() {
                // no matter what happens, we should reset the game state
                // TODO: Somehow notify the player that the lobby is either restarting or has gone back to waiting
                // Honestly, we should be sending the players the current state of the Lobby as well as the game
                match left_yes_or_no {
                    YesOrNo::Yes => {
                        match right_yes_or_no {
                            YesOrNo::Yes => {
                                // go right back to running
                                self.running_lobby_list.insert(lobby_id, lobby.lobby_session);
                            }
                            YesOrNo::No => {
                                println!("Right player is leaving lobby {lobby_id}");
                                let leaving_player = lobby.lobby_session.get_right().unwrap();
                                lobby.lobby_session.remove_player(leaving_player)?;
                                self.waiting_lobby_list.insert(lobby_id, lobby.lobby_session);
                            }
                        }
                    }
                    YesOrNo::No => {
                        match right_yes_or_no {
                            YesOrNo::Yes => {
                                println!("Left player is leaving lobby {lobby_id}");
                                let leaving_player = lobby.lobby_session.get_left().unwrap();
                                lobby.lobby_session.remove_player(leaving_player)?;
                                self.waiting_lobby_list.insert(lobby_id, lobby.lobby_session);
                            }
                            YesOrNo::No => {
                                // don't insert, we can just drop it
                                println!("Both players chose not to continue player, {lobby_id} is destroyed");
                                drop(lobby);
                            }
                        }
                    }
                }
            } else {
                // still haven't figured it out, place it back into finished
                println!("lobby {lobby_id} is currently finished");
                self.finished_lobby_list.insert(lobby_id, lobby);
            }

        }

        if lobby_state == LobbyState::Finished {
            println!("Popping out lobby {lobby_id} from running list, and putting it into finished lobby list");
            let lobby = self.running_lobby_list.remove(&lobby_id).unwrap();
            self.finished_lobby_list.insert(lobby_id, FinishedLobbySession{lobby_session: lobby, left_side_continue: None, right_side_continue: None});
        }

        // just send back current state of lobby to everyone
        self.send_message_to_lobby(&RpcServerMessage::LobbyState(lobby_state), &lobby_id)?;

        // TODO: Figure out a better way of handling this
        // for now, we can just return Ok and ignore messages unless the lobby is running
        return Ok(());

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

    pub async fn insert_user(&self, addr: SocketAddr, sender: UserSender) -> (PlayerSide, LobbyId) {
        self.server_state.lock().await.insert_user(addr, sender)
    }

    pub async fn remove_user(&self, addr: SocketAddr) {
        self.server_state.lock().await.remove_user(addr)
    }

    pub async fn handle_user_rpc(&self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        self.server_state
            .lock()
            .await
            .handle_user_rpc(user_rpc_message)
    }
}
