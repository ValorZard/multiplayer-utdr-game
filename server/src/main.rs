use futures_util::{SinkExt, StreamExt, future, pin_mut, stream::TryStreamExt};
use std::{
    cell::{LazyCell, OnceCell},
    collections::HashMap,
    hash::Hash,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use rpc::RpcMessage;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

use uuid::Uuid;

use crate::lobby_db::LobbyDBMessage;
use crate::{
    lobby::{Lobby, LobbyState},
    lobby_db::run_lobby_db_actor,
};

const SERVER_HOSTING_ADDRESS: &str = "0.0.0.0:12345";

mod lobby;
mod lobby_db;

async fn handle_connection(
    raw_stream: TcpStream,
    addr: SocketAddr,
    lobby_db_sender: mpsc::UnboundedSender<LobbyDBMessage>,
) {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (user_sender, user_receiver) = mpsc::unbounded_channel();

    // assign this peer to a lobby
    let (id_sender, id_receiver) = oneshot::channel();
    lobby_db_sender
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
                    let _ = lobby_db_sender.send(LobbyDBMessage::RPCMessage {
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
    let receive_from_others = UnboundedReceiverStream::new(user_receiver)
        .map(Ok)
        .forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    println!("{} disconnected", &addr);
    lobby_db_sender
        .send(LobbyDBMessage::RemoveUser(addr))
        .expect("This message is really important since it delete the user");
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(SERVER_HOSTING_ADDRESS).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", SERVER_HOSTING_ADDRESS);

    // spawn lobby actor
    let (lobby_db_sender, lobby_db_receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_lobby_db_actor(lobby_db_receiver));

    // Let's spawn the handling of each connection in a separate task.
    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(stream, addr, lobby_db_sender.clone()));
    }

    Ok(())
}
