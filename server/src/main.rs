use anyhow::{Context, bail};
use clap::Parser;
use futures_util::StreamExt;
use rpc::{HEADER_MESSAGE, decode_message, encode_message};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path,
};
use tokio::{io::AsyncReadExt, sync::mpsc, task::JoinSet};
use web_transport_quinn::{Request, Server, proto::ConnectResponse};

use crate::lobby_db::{ServerState, UserReliableRPCMessage, UserUnreliableRPCMessage};
use rustls::pki_types::CertificateDer;
use tracing::{info, warn};
use web_transport_quinn::generic::Session;

#[deny(clippy::unwrap_used, clippy::panic)]

const SERVER_HOSTING_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 12345);

mod lobby;
mod lobby_db;
mod rps;

async fn handle_connection(request: Request, server_state: ServerState) -> anyhow::Result<()> {
    info!("WebTransport connection established: {}", request.url);

    // Accept the session.
    let response = ConnectResponse::OK;
    let session = request.respond(response).await?;

    // Insert the write part of this peer to the peer map.
    let (user_reliable_sender, mut user_reliable_receiver) = mpsc::unbounded_channel();
    let (user_unreliable_sender, mut user_unreliable_receiver) = mpsc::unbounded_channel();

    // assign this peer to a lobby
    let addr = session.remote_address();
    server_state
        .connect_user(addr, user_reliable_sender, user_unreliable_sender)
        .await?;

    let (mut outgoing, mut incoming) = session.open_bi().await?;

    let server_state_clone = server_state.clone();
    let broadcast_incoming = tokio::spawn(async move {
        let server_state = server_state_clone;
        let mut header_buf = [0_u8; HEADER_MESSAGE.len()];
        let mut message_size_buf = [0_u8; 4]; // u32 is 4 u8
        loop {
            let message_read_result = incoming.read_exact(&mut header_buf).await;
            if let Ok(()) = message_read_result {
                if header_buf != HEADER_MESSAGE {
                    bail!("Connection has received corrupted header, stopping...")
                }

                // read message size, (currently hardcoded to be size u32)
                incoming.read_exact(&mut message_size_buf).await?;
                let message_size: u32 = u32::from_be_bytes(message_size_buf);

                let chunk = incoming
                    .read_chunk(message_size as usize, true)
                    .await?
                    .expect("There should be a chunk here we can use");
                let message = decode_message(&chunk.bytes)?;
                info!("message received from {addr}: {message:?}");

                let user_rpc_message = UserReliableRPCMessage {
                    message,
                    send_addr: addr,
                };

                server_state
                    .handle_user_reliable_rpc(user_rpc_message)
                    .await
                    .expect("Error handling user rpc");
            } else if let Err(e) = message_read_result {
                warn!("Incoming messages have stopped, error {e}");
                break;
            }
        }

        Ok(())
    });

    let server_state_clone = server_state.clone();
    let session_for_datagram = session.clone();
    let datagram_incoming = tokio::spawn(async move {
        let server_state = server_state_clone;
        let _header_buf = [0_u8; HEADER_MESSAGE.len()];
        let _message_size_buf = [0_u8; 4]; // u32 is 4 u8
        while let Ok(datagram) = session_for_datagram.recv_datagram().await {
            let datagram = &datagram[(HEADER_MESSAGE.len() + 4)..];
            let message = decode_message(&datagram).expect("Should be fine");
            let user_rpc_message = UserUnreliableRPCMessage {
                message,
                send_addr: addr,
            };
            let _ = server_state
                .handle_user_unreliable_rpc(user_rpc_message)
                .await;
        }
    });

    // forward the binary web transport messages from user receiver into the web transport stream itself
    let send_reliable_to_clients = tokio::spawn(async move {
        while let Some(message) = user_reliable_receiver.recv().await {
            let message = encode_message(&message).expect("this should be fine");
            if let Err(e) = outgoing.write_all(&message).await {
                warn!("{e}");
            }
        }
        warn!("Receiver for user messages into outgoing stream stopped");
    });
    let session_for_unreliable = session.clone();
    let send_unreliable_to_clients = tokio::spawn(async move {
        while let Some(message) = user_unreliable_receiver.recv().await {
            let message = encode_message(&message).expect("this should be fine");
            if let Err(e) = session_for_unreliable.send_datagram(message.into()) {
                warn!("{e}");
            }
        }
        warn!("Receiver for user messages into outgoing stream stopped");
    });
    tokio::select! {
        _ = broadcast_incoming => {},
        _ = datagram_incoming => {},
        _ = send_reliable_to_clients => {},
        _ = send_unreliable_to_clients => {},
    }

    info!("{} disconnected", &addr);
    server_state.disconnect_user(addr).await?;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:12345")]
    addr: std::net::SocketAddr,

    /// Use the certificates at this path, encoded as PEM.
    #[arg(long)]
    pub tls_cert: path::PathBuf,

    /// Use the private key at this path, encoded as PEM.
    #[arg(long)]
    pub tls_key: path::PathBuf,

    /// Optional WebTransport subprotocol to support.
    #[arg(long)]
    pub protocol: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    // Create the event loop and TCP listener we'll accept connections on.
    let server_builder =
        web_transport_quinn::ServerBuilder::new().with_addr(SERVER_HOSTING_ADDRESS);

    let args = Args::parse();

    // Read the PEM certificate chain
    let chain = std::fs::File::open(args.tls_cert)?;
    let mut chain = std::io::BufReader::new(chain);

    let chain: Vec<CertificateDer> = rustls_pemfile::certs(&mut chain)
        .map(|c| c.expect("Could not load certificate"))
        .collect();

    anyhow::ensure!(!chain.is_empty(), "could not find certificate");

    // Read the PEM private key
    let keys = std::fs::File::open(args.tls_key).expect("failed to open key file");

    // Try to parse a PKCS#8 key
    // -----BEGIN PRIVATE KEY-----
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(keys))
        .context("failed to load private key")?
        .context("missing private key")?;

    let mut server: Server = server_builder.with_certificate(chain, key)?;
    info!("Listening on: {}", SERVER_HOSTING_ADDRESS);
    // spawn lobby actor
    let server_state = ServerState::new();

    // Let's spawn the handling of each connection in a separate task.
    let mut connection_set = JoinSet::new();
    while let Some(session) = server.accept().await {
        let server_state = server_state.clone();
        connection_set.spawn(async move {
            if let Err(e) = handle_connection(session, server_state).await {
                warn!("Connection error: {e}");
            }
        });
    }

    Ok(())
}
