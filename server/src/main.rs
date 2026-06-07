use futures_util::{SinkExt, StreamExt, future, pin_mut, stream::TryStreamExt};
use rkyv::rancor;
use rkyv::util::AlignedVec;
use rpc::{RpcClientMessage, RpcServerMessage};
use std::{
    cell::{LazyCell, OnceCell},
    collections::HashMap,
    hash::Hash,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

use uuid::Uuid;

use crate::lobby_db::ServerState;
use crate::lobby_db::UserRPCMessage;

const SERVER_HOSTING_ADDRESS: &str = "0.0.0.0:12345";

mod lobby;
mod lobby_db;
mod rps;

// messages sent from a websocket stream might not be aligned to what rkyv wants
pub fn decode_client_message(bytes: &[u8]) -> Result<RpcClientMessage, rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<RpcClientMessage, rancor::Error>(aligned.as_ref())
}

pub fn encode_server_message(message: &RpcServerMessage) -> Result<AlignedVec, rancor::Error> {
    rkyv::to_bytes::<rancor::Error>(message)
}

async fn handle_connection(
    raw_stream: TcpStream,
    addr: SocketAddr,
    server_state: ServerState,
) -> anyhow::Result<()> {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = match tokio_tungstenite::accept_async(raw_stream).await {
        Ok(ws) => ws,
        Err(err) => {
            eprintln!("WebSocket handshake failed for {}: {:?}", addr, err);
            return Ok(());
        }
    };
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (user_sender, user_receiver) = mpsc::unbounded_channel();

    // assign this peer to a lobby
    server_state.connect_user(addr, user_sender).await?;

    let (mut outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.try_for_each(|msg| {
        let server_state = server_state.clone();

        async move {
            match &msg {
                WsMessage::Binary(bytes) => match decode_client_message(bytes) {
                    Ok(decoded) => {
                        println!("Received a binary message from {}: {:?}", addr, decoded);

                        let user_rpc_message = UserRPCMessage {
                            message: decoded,
                            send_addr: addr,
                        };

                        server_state
                            .handle_user_rpc(user_rpc_message)
                            .await
                            .expect("Error handling user rpc");
                    }
                    Err(err) => {
                        println!("Failed to decode binary message from {}: {:?}", addr, err);
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

            Ok(())
        }
    });

    // forward the binary websocket messages from user receiver into the web socket stream itself
    let receive_from_others = UnboundedReceiverStream::new(user_receiver)
        .map(Ok)
        .forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    println!("{} disconnected", &addr);
    server_state.disconnect_user(addr).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(SERVER_HOSTING_ADDRESS).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", SERVER_HOSTING_ADDRESS);

    // spawn lobby actor
    let server_state = ServerState::new();

    // Let's spawn the handling of each connection in a separate task.
    let mut connection_set = JoinSet::new();
    while let Ok((stream, addr)) = listener.accept().await {
        connection_set.spawn(handle_connection(stream, addr, server_state.clone()));
    }

    Ok(())
}
