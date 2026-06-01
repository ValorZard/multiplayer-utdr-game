use std::{collections::HashMap, net::SocketAddr};

use rpc::{RPSGameState, RpcClientMessage, RpcServerMessage};
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender, error::SendError},
        oneshot,
    },
    task,
};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;

use crate::{
    encode_server_message,
    lobby::{LobbyData, LobbyError, LobbyState},
    rps::GameSession,
};

type LobbyId = Uuid;

type UserSender = UnboundedSender<WsMessage>;
type LobbyDBSender = UnboundedSender<LobbyDBMessage>;
type LobbyDBReceiver = UnboundedReceiver<LobbyDBMessage>;

type LobbyRPCSender = mpsc::UnboundedSender<UserRPCMessage>;
type LobbyRPCReceiver = mpsc::UnboundedReceiver<UserRPCMessage>;

struct LobbySession {
    lobby_data: LobbyData,
    lobby_actor: task::JoinHandle<()>,
    // use this to send messages to the actor
    lobby_rpc_sender: LobbyRPCSender,
}

async fn run_lobby_actor(
    lobby_id: LobbyId,
    mut lobby_rpc_receiver: LobbyRPCReceiver,
    lobby_db_sender: LobbyDBSender,
) {
    let mut current_round = GameSession::new();
    while let Some(rpc_message) = lobby_rpc_receiver.recv().await {
        println!("{rpc_message:?}");
        if let RpcClientMessage::GameInput(input) = rpc_message.message {
            if let Some(player_side) = rpc_message.player_side {
                match player_side {
                    PlayerSide::Left => {
                        let _ = current_round.set_left_input(input);
                    }
                    PlayerSide::Right => {
                        let _ = current_round.set_right_input(input);
                    }
                }
                let current_state = current_round.compute_state();
                println!("{lobby_id}: Current game state: {current_state:?}");
                if let Err(e) = lobby_db_sender.send(LobbyDBMessage::LobbyMessage(
                    lobby_id,
                    LobbyMessage::GameState(current_state),
                )) {
                    println!("Failed to send lobby message to DB actor: {e}");
                    break;
                }
            } else {
                unreachable!(
                    "This should not be possible, do NOT send a RPC into a lobby from a user that's not inside."
                );
            }
        }
        if let Err(e) = lobby_db_sender.send(LobbyDBMessage::LobbyMessage(
            lobby_id,
            LobbyMessage::Heartbeat,
        )) {
            println!("Failed to send lobby message to DB actor: {e}");
            break;
        }
    }
}

impl LobbySession {
    pub fn new(
        lobby_id: LobbyId,
        left_side: SocketAddr,
        lobby_db_sender: mpsc::UnboundedSender<LobbyDBMessage>,
    ) -> Self {
        let (lobby_rpc_sender, lobby_rpc_receiver) = mpsc::unbounded_channel();
        let lobby_actor = tokio::spawn(run_lobby_actor(
            lobby_id,
            lobby_rpc_receiver,
            lobby_db_sender,
        ));
        Self {
            lobby_data: LobbyData::new(left_side),
            lobby_actor,
            lobby_rpc_sender,
        }
    }

    pub fn insert_player(&mut self, new_player: SocketAddr) -> Result<LobbyState, LobbyError> {
        self.lobby_data.insert_player(new_player)
    }

    pub fn remove_player(&mut self, leaving_player: SocketAddr) -> Result<LobbyState, LobbyError> {
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
    sender: UserSender,
}

// We are assuming that SocketAddrs are going to be unique per user for now
struct LobbyDB {
    user_list: HashMap<SocketAddr, UserData>,
    // lobbys that are full and are currently running
    running_lobby_list: HashMap<LobbyId, LobbySession>,
    // lobbys that are waiting on another player to continue
    waiting_lobby_list: HashMap<LobbyId, LobbySession>,
    // sender we clone to give to lobbies when they are created
    lobby_db_sender: LobbyDBSender,
}

impl LobbyDB {
    pub fn new(lobby_db_sender: LobbyDBSender) -> Self {
        Self {
            user_list: HashMap::new(),
            running_lobby_list: HashMap::new(),
            waiting_lobby_list: HashMap::new(),
            lobby_db_sender,
        }
    }

    pub fn insert_user(&mut self, addr: SocketAddr, sender: UserSender) -> LobbyId {
        self.user_list
            .entry(addr)
            .or_insert_with(|| {
                let lobby_id =
                // pop first lobby that is available
                if let Some(lobby_id) = self.waiting_lobby_list.keys().next().copied()
                    && let Some(mut lobby) = self.waiting_lobby_list.remove(&lobby_id)
                {
                    lobby
                        .insert_player(addr)
                        .expect("Should be successful in starting game");
                    let lobby_state = lobby.get_current_state();
                    println!("Lobby {lobby_id} should be now running: {lobby_state:?}");
                    assert_eq!(lobby_state, LobbyState::Full);
                    self.running_lobby_list.insert(lobby_id, lobby);
                    lobby_id
                } else {
                    // if there is no waiting lobby, then create a new one
                    let lobby_id = Uuid::new_v4();
                    let new_lobby = LobbySession::new(lobby_id, addr, self.lobby_db_sender.clone());
                    let lobby_state = new_lobby.get_current_state();
                    println!("Lobby {lobby_id} should now be waiting: {lobby_state:?}");
                    assert_eq!(lobby_state, LobbyState::Waiting);
                    self.waiting_lobby_list.insert(lobby_id, new_lobby);
                    lobby_id
                };
                // either way, we should insert the new user into user list
                UserData { lobby_id, sender }
            })
            .lobby_id
    }

    pub fn remove_user(&mut self, addr: SocketAddr) {
        if let Some(user_data) = self.user_list.get(&addr) {
            if self.running_lobby_list.contains_key(&user_data.lobby_id) {
                if let Some(mut lobby) = self.running_lobby_list.remove(&user_data.lobby_id) {
                    let state = lobby.remove_player(addr).unwrap();
                    // this should NEVER be empty, if you remove one player from a running lobby, this should always be half full
                    assert_eq!(LobbyState::Waiting, state);
                    // Because this lobby is now waiting for a new player, put this in the waiting queue
                    self.waiting_lobby_list.insert(user_data.lobby_id, lobby);
                    println!(
                        "Removed player {addr} from running lobby {}, moving lobby to waiting",
                        user_data.lobby_id
                    );
                }
            } else if self.waiting_lobby_list.contains_key(&user_data.lobby_id) {
                // because we're removing the lobby from the waiting lobby, this will automatically destroy the lobby
                if let Some(mut lobby) = self.waiting_lobby_list.remove(&user_data.lobby_id) {
                    let state = lobby.remove_player(addr).unwrap();
                    // this should ALWAYS be empty, if you remove one player from a waiting lobby, there should be no one in there
                    assert_eq!(LobbyState::Empty, state);
                    println!(
                        "Removed player {addr} from waiting lobby {}, lobby should now be deleted",
                        user_data.lobby_id
                    );
                }
            }
        }
    }

    pub fn send_message_from_user(&self, user_rpc_message: &UserRPCMessage) {
        // send messages into lobby actor (can only send into running lobby)
        if let Some(user_data) = self.user_list.get(&user_rpc_message.send_addr) {
            let mut routed_message = user_rpc_message.clone();
            if let Some(lobby) = self.running_lobby_list.get(&user_data.lobby_id) {
                routed_message.player_side = lobby.get_player_side(user_rpc_message.send_addr);
                let _ = lobby.lobby_rpc_sender.send(routed_message);
            } else if let Some(lobby) = self.waiting_lobby_list.get(&user_data.lobby_id) {
                routed_message.player_side = lobby.get_player_side(user_rpc_message.send_addr);
                let _ = lobby.lobby_rpc_sender.send(routed_message);
            } else {
                unreachable!(
                    "If a user is in the user list, it should also be in either waiting or running lobby list."
                );
            }
        } else {
            unreachable!(
                "If a user is able to send an RPC, that means they should be in the user list and assigned a lobby"
            );
        }
    }

    pub fn send_message_to_lobby_users(&self, lobby_id: LobbyId, lobby_message: LobbyMessage) {
        println!("Sending message to lobby users {lobby_message:?}");
        // send message
        let rpc_message = if let LobbyMessage::GameState(state) = lobby_message {
            RpcServerMessage::GameState(state)
        } else {
            RpcServerMessage::Text(format!("{lobby_id} : {lobby_message:?}"))
        };
        // broadcast message to everyone
        let encoded = encode_server_message(&rpc_message).unwrap().to_vec();
        if let Some(lobby) = self.running_lobby_list.get(&lobby_id) {
            if let Some(addr) = lobby.get_left() {
                let _ = self
                    .user_list
                    .get(&addr)
                    .unwrap()
                    .sender
                    .send(WsMessage::Binary(encoded.clone().into()));
            }
            if let Some(addr) = lobby.get_right() {
                let _ = self
                    .user_list
                    .get(&addr)
                    .unwrap()
                    .sender
                    .send(WsMessage::Binary(encoded.clone().into()));
            }
        } else if let Some(lobby) = self.waiting_lobby_list.get(&lobby_id) {
            if let Some(addr) = lobby.get_left() {
                let _ = self
                    .user_list
                    .get(&addr)
                    .unwrap()
                    .sender
                    .send(WsMessage::Binary(encoded.clone().into()));
            }
            if let Some(addr) = lobby.get_right() {
                let _ = self
                    .user_list
                    .get(&addr)
                    .unwrap()
                    .sender
                    .send(WsMessage::Binary(encoded.clone().into()));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserRPCMessage {
    pub message: RpcClientMessage,
    pub send_addr: SocketAddr,
    pub player_side: Option<PlayerSide>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerSide {
    Left,
    Right,
}

#[derive(Debug)]
pub enum LobbyMessage {
    Heartbeat,
    Text(String),
    GameState(RPSGameState),
}

pub enum LobbyDBMessage {
    NewUser(SocketAddr, oneshot::Sender<LobbyId>, UserSender),
    UserRPCMessage(UserRPCMessage),
    LobbyMessage(LobbyId, LobbyMessage),
    RemoveUser(SocketAddr),
}

pub async fn run_lobby_db_actor(
    lobby_db_sender: LobbyDBSender,
    mut lobby_db_receiver: LobbyDBReceiver,
) {
    let mut lobby_db = LobbyDB::new(lobby_db_sender);
    while let Some(message) = lobby_db_receiver.recv().await {
        match message {
            LobbyDBMessage::NewUser(socket_addr, lobby_id_sender, user_sender) => {
                let lobby_id = lobby_db.insert_user(socket_addr, user_sender);
                lobby_id_sender
                    .send(lobby_id)
                    .expect("Lobby setup message should be sent");
            }
            LobbyDBMessage::UserRPCMessage(user_rpc_message) => {
                // broadcast message to everyone except the send_addr
                lobby_db.send_message_from_user(&user_rpc_message);
            }
            LobbyDBMessage::RemoveUser(socket_addr) => {
                lobby_db.remove_user(socket_addr);
            }
            LobbyDBMessage::LobbyMessage(lobby_id, lobby_message) => {
                println!("Lobby DB received message from Lobby {lobby_id}");
                lobby_db.send_message_to_lobby_users(lobby_id, lobby_message);
            }
        }
    }
}
