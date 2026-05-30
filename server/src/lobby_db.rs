use std::{collections::HashMap, net::SocketAddr};

use rpc::RpcMessage;
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    oneshot,
};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;

use crate::{Lobby, lobby::LobbyState};

type LobbyId = Uuid;

type UserSender = UnboundedSender<WsMessage>;

struct UserData {
    lobby_id: LobbyId,
    sender: UserSender,
}

// We are assuming that SocketAddrs are going to be unique per user for now
struct LobbyDB {
    user_list: HashMap<SocketAddr, UserData>,
    // lobbys that are full and are currently running
    running_lobby_list: HashMap<LobbyId, Lobby>,
    // lobbys that are waiting on another player to continue
    waiting_lobby_list: HashMap<LobbyId, Lobby>,
}

impl LobbyDB {
    pub fn new() -> Self {
        Self {
            user_list: HashMap::new(),
            running_lobby_list: HashMap::new(),
            waiting_lobby_list: HashMap::new(),
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
                    let new_lobby = Lobby::new(addr);
                    let lobby_state = new_lobby.get_current_state();
                    println!("Lobby {lobby_id} should now be waiting: {lobby_state:?}");
                    assert_eq!(lobby_state, LobbyState::Waiting);
                    self.waiting_lobby_list.insert(lobby_id, Lobby::new(addr));
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
}

pub enum LobbyDBMessage {
    NewUser(SocketAddr, oneshot::Sender<LobbyId>, UserSender),
    RPCMessage {
        message: RpcMessage,
        send_addr: SocketAddr,
    },
    RemoveUser(SocketAddr),
}

pub async fn run_lobby_db_actor(mut lobby_receiver: mpsc::UnboundedReceiver<LobbyDBMessage>) {
    let mut lobby_db = LobbyDB::new();
    while let Some(message) = lobby_receiver.recv().await {
        match message {
            LobbyDBMessage::NewUser(socket_addr, lobby_id_sender, user_sender) => {
                let lobby_id = lobby_db.insert_user(socket_addr, user_sender);
                lobby_id_sender
                    .send(lobby_id)
                    .expect("Lobby setup message should be sent");
            }
            LobbyDBMessage::RPCMessage { message, send_addr } => {
                // broadcast message to everyone except the send_addr
                let encoded = rpc::encode_message(&message).unwrap().to_vec();
                for (addr, user_data) in &lobby_db.user_list {
                    if *addr != send_addr {
                        // if this returns an error, that means that the connection has dropped, and we don't need to do anything
                        let _ = user_data
                            .sender
                            .send(WsMessage::Binary(encoded.clone().into()));
                    }
                }
            }
            LobbyDBMessage::RemoveUser(socket_addr) => {
                lobby_db.remove_user(socket_addr);
            }
        }
    }
}
