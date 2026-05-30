use std::{
    cell::{LazyCell, OnceCell},
    collections::HashMap,
    hash::Hash,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{UnboundedSender, unbounded};
use futures_util::{SinkExt, StreamExt, future, pin_mut, stream::TryStreamExt};

use rpc::RpcMessage;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

use uuid::Uuid;

use crate::lobby::Lobby;

const SERVER_HOSTING_ADDRESS: &str = "0.0.0.0:12345";

type UserSender = UnboundedSender<WsMessage>;

type LobbyId = Uuid;

mod lobby;
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
                        .start_game(addr)
                        .expect("Should be successful in starting game");
                    self.running_lobby_list.insert(lobby_id, lobby);
                    lobby_id
                } else {
                    // if there is no waiting lobby, then create a new one
                    let lobby_id = Uuid::new_v4();
                    self.waiting_lobby_list.insert(lobby_id, Lobby::new(addr));
                    lobby_id
                };
                // either way, we should insert the new user into user list
                UserData { lobby_id, sender }
            })
            .lobby_id
    }
}

enum LobbyDBMessage {
    NewUser(SocketAddr, oneshot::Sender<LobbyId>, UserSender),
    RPCMessage {
        message: RpcMessage,
        send_addr: SocketAddr,
    },
}

async fn run_lobby_db_actor(mut lobby_receiver: mpsc::UnboundedReceiver<LobbyDBMessage>) {
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
                        user_data
                            .sender
                            .unbounded_send(WsMessage::Binary(encoded.clone().into()))
                            .expect("Message should be sent");
                    }
                }
            }
        }
    }
}

async fn handle_connection(
    raw_stream: TcpStream,
    addr: SocketAddr,
    lobby_sender: mpsc::UnboundedSender<LobbyDBMessage>,
) {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (user_sender, user_receiver) = unbounded();

    // assign this peer to a lobby
    let (id_sender, id_receiver) = oneshot::channel();
    lobby_sender
        .send(LobbyDBMessage::NewUser(addr, id_sender, user_sender))
        .expect("initial lobby message should be sent");
    // get our lobby id
    let lobby_id = id_receiver.await.expect("We should be assigned a LobbyId");

    println!("Player assigned to lobby {lobby_id}");

    let (outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.try_for_each(|msg| {
        match &msg {
            WsMessage::Binary(bytes) => match rpc::decode_message(bytes) {
                Ok(decoded) => {
                    println!("Received a binary message from {}: {:?}", addr, decoded);
                    // We want to broadcast the message to everyone except ourselves.
                    let _ = lobby_sender.send(LobbyDBMessage::RPCMessage {
                        message: decoded,
                        send_addr: addr,
                    });
                }
                Err(err) => {
                    println!("Failed to decode binary message from {}: {:?}", addr, err)
                }
            },
            WsMessage::Text(text) => {
                println!("Received a text message from {}: {}", addr, text);
            }
            other => {
                println!(
                    "Received a websocket control frame from {}: {:?}",
                    addr, other
                );
            }
        }

        future::ok(())
    });

    // forward the binary websocket messages from user receiver into the web socket stream itself
    let receive_from_others = user_receiver.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    println!("{} disconnected", &addr);
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(SERVER_HOSTING_ADDRESS).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", SERVER_HOSTING_ADDRESS);

    // spawn lobby actor
    let (lobby_sender, lobby_receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_lobby_db_actor(lobby_receiver));

    // Let's spawn the handling of each connection in a separate task.
    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, addr, lobby_sender.clone()));
    }

    Ok(())
}
