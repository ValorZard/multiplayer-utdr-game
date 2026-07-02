use anyhow::{Context, bail};
use clap::Parser;
use rpc::{
    HEADER_MESSAGE, ReliableRpcServerMessage, UnreliableRpcServerMessage, decode_client_message,
    encode_server_message,
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path,
};
use tokio::{sync::mpsc, task::JoinSet};
use web_transport_quinn::{Request, Server, proto::ConnectResponse};

use crate::lobby_db::ServerState;
use crate::lobby_db::UserRPCMessage;
use crate::lobby::UserSender;
use rustls::pki_types::CertificateDer;
use tracing::{debug, info, warn};

#[deny(clippy::unwrap_used, clippy::panic)]

const SERVER_HOSTING_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 12345);

mod lobby;
mod lobby_db;
mod rps;

async fn handle_incoming_rpc_stream(
    incoming: &mut web_transport_quinn::RecvStream,
    addr: SocketAddr,
    server_state: &ServerState,
) -> anyhow::Result<()> {
    let mut header_buf = [0_u8; HEADER_MESSAGE.len()];
    let mut message_size_buf = [0_u8; 4]; // u32 is 4 u8

    incoming.read_exact(&mut header_buf).await?;
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
    let message = decode_client_message(&chunk.bytes)?;
    debug!("message received from {addr}: {message:?}");

    let user_rpc_message = UserRPCMessage {
        message,
        send_addr: addr,
    };

    server_state
        .handle_user_rpc(user_rpc_message)
        .await
        .expect("Error handling user rpc");

    Ok(())
}

async fn handle_connection(request: Request, server_state: ServerState) -> anyhow::Result<()> {
    info!("WebTransport connection established: {}", request.url);

    // Accept the session.
    let response = ConnectResponse::OK;
    let session = request.respond(response).await?;

    // Insert the write part of this peer to the peer map.
    let (user_reliable_sender, mut user_reliable_receiver) =
        mpsc::unbounded_channel::<ReliableRpcServerMessage>();
    let (user_unreliable_sender, mut user_unreliable_receiver) =
        mpsc::unbounded_channel::<UnreliableRpcServerMessage>();
    let user_sender = UserSender::new(user_reliable_sender, user_unreliable_sender);

    // assign this peer to a lobby
    let addr = session.remote_address();
    server_state.connect_user(addr, user_sender).await?;

    let (mut outgoing, mut incoming) = session.open_bi().await?;
    let session_for_uni_outgoing = session.clone();
    let session_for_uni_incoming = session.clone();

    let server_state_clone = server_state.clone();
    let mut broadcast_incoming = tokio::spawn(async move {
        let server_state = server_state_clone;
        loop {
            if let Err(e) = handle_incoming_rpc_stream(&mut incoming, addr, &server_state).await {
                let error_text = e.to_string();
                if error_text.contains("finished early") || error_text.contains("closed") {
                    debug!("Incoming messages stream ended for {addr}: {e}");
                } else {
                    warn!("Incoming messages have stopped, error {e}");
                }
                break;
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let server_state_clone = server_state.clone();
    let broadcast_incoming_unreliable = tokio::spawn(async move {
        let server_state = server_state_clone;
        let mut current_stream = match session_for_uni_incoming.accept_uni().await {
            Ok(stream) => stream,
            Err(e) => {
                debug!("Incoming unreliable stream accept stopped, error {e}");
                return Ok::<(), anyhow::Error>(());
            }
        };

        loop {
            tokio::select! {
                accepted = session_for_uni_incoming.accept_uni() => {
                    match accepted {
                        Ok(new_stream) => {
                            // Drop the old stream immediately and switch to the newest packet stream.
                            current_stream = new_stream;
                        }
                        Err(e) => {
                            debug!("Incoming unreliable stream accept stopped, error {e}");
                            break;
                        }
                    }
                }
                result = handle_incoming_rpc_stream(&mut current_stream, addr, &server_state) => {
                    if let Err(e) = result {
                        debug!("Incoming unreliable stream processing ended: {e}");
                    }
                    match session_for_uni_incoming.accept_uni().await {
                        Ok(new_stream) => {
                            current_stream = new_stream;
                        }
                        Err(e) => {
                            debug!("Incoming unreliable stream accept stopped, error {e}");
                            break;
                        }
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // forward reliable messages over the bidirectional stream
    let mut receive_reliable_from_others = tokio::spawn(async move {
        while let Some(message) = user_reliable_receiver.recv().await {
            let message = rpc::RpcServerMessage::Reliable(message);
            let message = encode_server_message(&message).expect("failed to encode message");
            if let Err(e) = outgoing.write_all(&message).await {
                let error_text = e.to_string();
                if error_text.contains("closed") || error_text.contains("STOP_SENDING") {
                    debug!("reliable write ended: {e}");
                } else {
                    warn!("{e}");
                }
            }
        }
        warn!("Reliable receiver for user messages into outgoing stream stopped");
    });

    // forward unreliable messages over unidirectional streams
    let receive_unreliable_from_others = tokio::spawn(async move {
        while let Some(message) = user_unreliable_receiver.recv().await {
            let message = rpc::RpcServerMessage::Unreliable(message);
            let message = encode_server_message(&message).expect("failed to encode message");
            match session_for_uni_outgoing.open_uni().await {
                Ok(mut uni_outgoing) => {
                    if let Err(e) = uni_outgoing.write_all(&message).await {
                        let error_text = e.to_string();
                        if error_text.contains("closed") || error_text.contains("STOP_SENDING") {
                            debug!("unreliable uni write ended: {e}");
                        } else {
                            warn!("failed to write unreliable message on uni stream: {e}");
                        }
                    }
                }
                Err(open_error) => {
                    let error_text = open_error.to_string();
                    if error_text.contains("closed") || error_text.contains("STOP_SENDING") {
                        debug!("failed to open uni stream for unreliable message: {open_error}");
                    } else {
                        warn!("failed to open uni stream for unreliable message: {open_error}");
                    }
                }
            }
        }
        warn!("Unreliable receiver for user messages into outgoing stream stopped");
    });

    // Connection lifetime is controlled by the WebTransport session itself,
    // not by individual reliable/unreliable stream task shutdown.
    tokio::select! {
        close_reason = session.closed() => {
            info!("session for {addr} closed: {close_reason}");
        }
    }

    broadcast_incoming.abort();
    broadcast_incoming_unreliable.abort();
    receive_reliable_from_others.abort();
    receive_unreliable_from_others.abort();

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
