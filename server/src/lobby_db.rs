use anyhow::bail;
use rpc::{LobbyId, PlayerSide, RPSGameState, RpcClientMessage, RpcServerMessage};
use std::error::Error;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;

use crate::{
    encode_server_message,
    lobby::{LobbyData, LobbyError, LobbyState},
    rps::GameSession,
};

type UserSender = UnboundedSender<WsMessage>;

#[derive(Debug, Clone)]
pub struct UserRPCMessage {
    pub message: RpcClientMessage,
    pub send_addr: SocketAddr,
    pub player_side: Option<PlayerSide>,
}

#[derive(Debug, Clone)]
pub enum LobbyMessage {
    Heartbeat,
    Text(String),
    GameState(RPSGameState),
}

struct LobbySession {
    lobby_data: LobbyData,
    current_round: GameSession,
}

impl LobbySession {
    pub fn new(left_side: SocketAddr) -> Self {
        Self {
            lobby_data: LobbyData::new(left_side),
            current_round: GameSession::new(),
        }
    }

    pub fn insert_player(
        &mut self,
        new_player: SocketAddr,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        self.lobby_data.insert_player(new_player)
    }

    pub fn remove_player(
        &mut self,
        leaving_player: SocketAddr,
    ) -> Result<(PlayerSide, LobbyState), LobbyError> {
        self.lobby_data.remove_player(leaving_player)
    }

    pub fn get_left(&self) -> Option<SocketAddr> {
        self.lobby_data.left_side
    }

    pub fn get_right(&self) -> Option<SocketAddr> {
        self.lobby_data.right_side
    }

    pub fn get_current_state(&self) -> LobbyState {
        self.lobby_data.get_current_state()
    }

    pub fn get_player_side(&self, addr: SocketAddr) -> Option<PlayerSide> {
        if self.get_left() == Some(addr) {
            Some(PlayerSide::Left)
        } else if self.get_right() == Some(addr) {
            Some(PlayerSide::Right)
        } else {
            None
        }
    }
}

struct UserData {
    lobby_id: LobbyId,
    player_side: PlayerSide,
    sender: UserSender,
}

struct ServerStateInner {
    user_list: HashMap<SocketAddr, UserData>,
    running_lobby_list: HashMap<LobbyId, LobbySession>,
    waiting_lobby_list: HashMap<LobbyId, LobbySession>,
}

impl ServerStateInner {
    fn new() -> Self {
        Self {
            user_list: HashMap::new(),
            running_lobby_list: HashMap::new(),
            waiting_lobby_list: HashMap::new(),
        }
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

                println!("Lobby {lobby_id} should now be running: {lobby_state:?}");
                assert_eq!(lobby_state, LobbyState::Full);

                self.running_lobby_list.insert(lobby_id, lobby);
                (player_side, lobby_id)
            } else {
                let lobby_id = Uuid::new_v4();
                let new_lobby = LobbySession::new(addr);

                let lobby_state = new_lobby.get_current_state();
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
            },
        );
        (player_side, lobby_id)
    }

    fn remove_user(&mut self, addr: SocketAddr) {
        let Some(user_data) = self.user_list.remove(&addr) else {
            return;
        };

        if let Some(mut lobby) = self.running_lobby_list.remove(&user_data.lobby_id) {
            let (player_side, state) = lobby.remove_player(addr).unwrap();
            assert_eq!(LobbyState::Waiting, state);
            // reset game when player leaves
            lobby.current_round = GameSession::new();
            let current_game_state = lobby.current_round.compute_state();
            // send player that's left the current state of the game
            let bytes = encode_server_message(&RpcServerMessage::GameState(current_game_state))
                .expect("Error serializing LobbyMessage");
            match player_side {
                PlayerSide::Left => {
                    let _ = self
                        .user_list
                        .get(&lobby.get_right().unwrap())
                        .unwrap()
                        .sender
                        .send(WsMessage::Binary(bytes.to_vec().into()));
                }
                PlayerSide::Right => {
                    let _ = self
                        .user_list
                        .get(&lobby.get_left().unwrap())
                        .unwrap()
                        .sender
                        .send(WsMessage::Binary(bytes.to_vec().into()));
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
                LobbyState::Full => unreachable!("removing a player cannot leave a lobby full"),
            }
        }
    }

    fn handle_user_rpc(&mut self, user_rpc_message: UserRPCMessage) -> anyhow::Result<()> {
        let Some(user_data) = self.user_list.get(&user_rpc_message.send_addr) else {
            bail!("Expect user to be in user list");
        };

        let lobby_id = user_data.lobby_id;

        let maybe_lobby = self
            .running_lobby_list
            .get_mut(&lobby_id)
            .or_else(|| self.waiting_lobby_list.get_mut(&lobby_id));

        let Some(lobby) = maybe_lobby else {
            unreachable!(
                "If a user is in user_list, its lobby should exist in waiting or running list"
            );
        };

        let player_side = lobby.get_player_side(user_rpc_message.send_addr);

        match user_rpc_message.message {
            RpcClientMessage::GameInput(input) => {
                let Some(player_side) = player_side else {
                    unreachable!("user in lobby should always have a side");
                };

                match player_side {
                    PlayerSide::Left => {
                        let _ = lobby.current_round.set_left_input(input);
                    }
                    PlayerSide::Right => {
                        let _ = lobby.current_round.set_right_input(input);
                    }
                }

                let current_state = lobby.current_round.compute_state();
                println!("{lobby_id}: Current game state: {current_state:?}");

                let outgoing_messages =
                    self.collect_lobby_broadcast(lobby_id, LobbyMessage::GameState(current_state));
                for (sender, msg) in outgoing_messages {
                    sender.send(msg)?
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn collect_lobby_broadcast(
        &self,
        lobby_id: LobbyId,
        lobby_message: LobbyMessage,
    ) -> Vec<(UserSender, WsMessage)> {
        println!("Sending message to lobby users {lobby_message:?}");

        let rpc_message = match lobby_message {
            LobbyMessage::GameState(state) => RpcServerMessage::GameState(state),
            LobbyMessage::Text(text) => RpcServerMessage::Text(text),
            LobbyMessage::Heartbeat => RpcServerMessage::Text(format!("{lobby_id} : Heartbeat")),
        };

        let encoded = encode_server_message(&rpc_message).unwrap().to_vec();
        let ws_message = WsMessage::Binary(encoded.into());

        let lobby = self
            .running_lobby_list
            .get(&lobby_id)
            .or_else(|| self.waiting_lobby_list.get(&lobby_id));

        let Some(lobby) = lobby else {
            return vec![];
        };

        let mut out = Vec::new();

        // update both left and right side with new game state
        if let Some(addr) = lobby.get_left()
            && let Some(user) = self.user_list.get(&addr)
        {
            out.push((user.sender.clone(), ws_message.clone()));
        }

        if let Some(addr) = lobby.get_right()
            && let Some(user) = self.user_list.get(&addr)
        {
            out.push((user.sender.clone(), ws_message.clone()));
        }

        out
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
