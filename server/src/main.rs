use std::{
    cell::{LazyCell, OnceCell},
    collections::HashMap,
    hash::Hash,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{UnboundedSender, unbounded};
use futures_util::{StreamExt, future, pin_mut, stream::TryStreamExt};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::tungstenite::protocol::Message;

use uuid::Uuid;

use crate::lobby::Lobby;

const SERVER_HOSTING_ADDRESS: &str = "0.0.0.0:12345";

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

type LobbyId = Uuid;

mod lobby;

// We are assuming that SocketAddrs are going to be unique per user for now
struct LobbyDB {
    user_list: HashMap<SocketAddr, Uuid>,
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

    pub fn insert_user(&mut self, addr: SocketAddr) -> LobbyId {
        if let Some(lobby_id) = self.user_list.get(&addr) {
            return *lobby_id;
        } else {
            // pop first lobby that is available
            if let Some(lobby_id) = self.waiting_lobby_list.keys().next().copied()
                && let Some(mut lobby) = self.waiting_lobby_list.remove(&lobby_id)
            {
                lobby
                    .start_game(addr)
                    .expect("Should be successful in starting game");
                self.running_lobby_list.insert(lobby_id, lobby);
                return lobby_id;
            } else {
                // if there is no waiting lobby, then create a new one
                let lobby_id = Uuid::new_v4();
                self.waiting_lobby_list.insert(lobby_id, Lobby::new(addr));
                return lobby_id;
            }
        }
    }
}

enum LobbyMessage {
    NewUser(SocketAddr, oneshot::Sender<LobbyId>),
}

async fn run_lobby_actor(mut lobby_receiver: mpsc::UnboundedReceiver<LobbyMessage>) {
    let mut lobby_db = LobbyDB::new();
    while let Some(message) = lobby_receiver.recv().await {
        match message {
            LobbyMessage::NewUser(socket_addr, sender) => {
                let lobby_id = lobby_db.insert_user(socket_addr);
                sender
                    .send(lobby_id)
                    .expect("Lobby setup message should be sent");
            }
        }
    }
}

async fn handle_connection(
    peer_map: PeerMap,
    raw_stream: TcpStream,
    addr: SocketAddr,
    lobby_sender: mpsc::UnboundedSender<LobbyMessage>,
) {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (tx, rx) = unbounded();
    peer_map.lock().unwrap().insert(addr, tx);

    // assign this peer to a lobby
    let (id_sender, id_receiver) = oneshot::channel();
    lobby_sender
        .send(LobbyMessage::NewUser(addr, id_sender))
        .expect("initial lobby message should be sent");
    // get our lobby id
    let lobby_id = id_receiver.await.expect("We should be assigned a LobbyId");

    println!("Player assigned to lobby {lobby_id}");

    let (outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.try_for_each(|msg| {
        match &msg {
            Message::Binary(bytes) => match rpc::decode_message(bytes) {
                Ok(decoded) => {
                    println!("Received a binary message from {}: {:?}", addr, decoded)
                }
                Err(err) => {
                    println!("Failed to decode binary message from {}: {:?}", addr, err)
                }
            },
            Message::Text(text) => {
                println!("Received a text message from {}: {}", addr, text);
            }
            other => {
                println!(
                    "Received a websocket control frame from {}: {:?}",
                    addr, other
                );
            }
        }

        let peers = peer_map.lock().unwrap();

        // We want to broadcast the message to everyone except ourselves.
        let broadcast_recipients = peers
            .iter()
            .filter(|(peer_addr, _)| peer_addr != &&addr)
            .map(|(_, ws_sink)| ws_sink);

        for recp in broadcast_recipients {
            recp.unbounded_send(msg.clone()).unwrap();
        }

        future::ok(())
    });

    let receive_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    println!("{} disconnected", &addr);
    peer_map.lock().unwrap().remove(&addr);
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    let state = PeerMap::new(Mutex::new(HashMap::new()));

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(SERVER_HOSTING_ADDRESS).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", SERVER_HOSTING_ADDRESS);

    // spawn lobby actor
    let (lobby_sender, lobby_receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_lobby_actor(lobby_receiver));

    // Let's spawn the handling of each connection in a separate task.
    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(
            state.clone(),
            stream,
            addr,
            lobby_sender.clone(),
        ));
    }

    Ok(())
}
