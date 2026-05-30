use std::{collections::HashMap, net::SocketAddr};

use rpc::RpcMessage;
use tokio::{
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    task,
};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;

use crate::lobby::{LobbyData, LobbyError, LobbyMessage, LobbyState};

type LobbyId = Uuid;

type UserSender = UnboundedSender<WsMessage>;

struct LobbySession {
    lobby_data: LobbyData,
    lobby_actor: task::JoinHandle<()>,
    // poll this for messages from the lobby
    lobby_message_receiver: mpsc::UnboundedReceiver<LobbyDBMessage>,
    // use this to send messages to the actor
    lobby_rpc_sender: mpsc::UnboundedSender<UserRPCMessage>,
}

async fn run_lobby_actor(
    mut lobby_rpc_receiver: mpsc::UnboundedReceiver<UserRPCMessage>,
    lobby_db_sender: mpsc::UnboundedSender<LobbyDBMessage>,
) {
    while let Some(rpc_message) = lobby_rpc_receiver.recv().await {
        println!("{rpc_message:?}");
        lobby_db_sender.send(LobbyDBMessage::LobbyMessage(LobbyMessage::Heartbeat));
    }
}

impl LobbySession {
    pub fn new(left_side: SocketAddr) -> Self {
        let (lobby_rpc_sender, lobby_rpc_receiver) = mpsc::unbounded_channel();
        let (lobby_message_sender, lobby_message_receiver) = mpsc::unbounded_channel();
        let lobby_actor = tokio::spawn(run_lobby_actor(lobby_rpc_receiver, lobby_message_sender));
        Self {
            lobby_data: LobbyData::new(left_side),
            lobby_actor,
            lobby_message_receiver,
            lobby_rpc_sender,
        }
    }

    pub fn insert_player(&mut self, new_player: SocketAddr) -> Result<LobbyState, LobbyError> {
        self.lobby_data.insert_player(new_player)
    }

    pub fn remove_player(&mut self, leaving_player: SocketAddr) -> Result<LobbyState, LobbyError> {
        self.lobby_data.remove_player(leaving_player)
    }

    pub fn get_current_state(&self) -> LobbyState {
        self.lobby_data.get_current_state()
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
    lobby_db_sender: mpsc::UnboundedSender<LobbyDBMessage>,
}

impl LobbyDB {
    pub fn new(lobby_db_sender: mpsc::UnboundedSender<LobbyDBMessage>) -> Self {
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
                    let new_lobby = LobbySession::new(addr);
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
            if let Some(lobby) = self.running_lobby_list.get(&user_data.lobby_id) {
                lobby.lobby_rpc_sender.send(user_rpc_message.clone());
            } else if let Some(lobby) = self.waiting_lobby_list.get(&user_data.lobby_id) {
                lobby.lobby_rpc_sender.send(user_rpc_message.clone());
            }
        }
    }

    pub fn send_message_to_lobby_users(&self, lobby_message: LobbyMessage) {
        // send message
        let rpc_message = RpcMessage::Text(format!("{lobby_message:?}"));
        // broadcast message to everyone
        let encoded = rpc::encode_message(&rpc_message).unwrap().to_vec();
        for (_, user_data) in &self.user_list {
            // if this returns an error, that means that the connection has dropped, and we don't need to do anything
            let _ = user_data
                .sender
                .send(WsMessage::Binary(encoded.clone().into()));
        }
    }
}

#[derive(Debug, Clone)]
pub struct UserRPCMessage {
    pub message: RpcMessage,
    pub send_addr: SocketAddr,
}
pub enum LobbyDBMessage {
    NewUser(SocketAddr, oneshot::Sender<LobbyId>, UserSender),
    UserRPCMessage(UserRPCMessage),
    LobbyMessage(LobbyMessage),
    RemoveUser(SocketAddr),
}

pub async fn run_lobby_db_actor(
    lobby_db_sender: mpsc::UnboundedSender<LobbyDBMessage>,
    mut lobby_db_receiver: mpsc::UnboundedReceiver<LobbyDBMessage>,
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
            LobbyDBMessage::LobbyMessage(lobby_message) => {
                lobby_db.send_message_to_lobby_users(lobby_message);
            }
        }
    }
}
