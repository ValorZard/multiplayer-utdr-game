use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt, future, pin_mut, stream::TryStreamExt};
use rkyv::rancor;
use rkyv::util::AlignedVec;
use rpc::{HEADER_MESSAGE, RpcClientMessage, RpcServerMessage, decode_client_message};
use web_transport_quinn::{RecvStream, Request, SendStream, Server, Session, proto::ConnectResponse};
use std::{
    cell::{LazyCell, OnceCell}, collections::HashMap, hash::Hash, io::Error as IoError, net::{IpAddr, Ipv4Addr, SocketAddr}, path, str::FromStr, sync::{Arc, Mutex}
};
use tokio::{
    io::AsyncReadExt, net::{TcpListener, TcpStream}, sync::{mpsc, oneshot}, task::JoinSet
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use clap::Parser;

use uuid::Uuid;

use rustls::pki_types::CertificateDer;
use crate::lobby_db::ServerState;
use crate::lobby_db::UserRPCMessage;

const SERVER_HOSTING_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),12345);

mod lobby;
mod lobby_db;
mod rps;

async fn handle_connection(
    request: Request,
    server_state: ServerState,
) -> anyhow::Result<()> {
    println!("WebTransport connection established: {}", request.url);

    // Accept the session.
    let response = ConnectResponse::OK;
    let session = request
        .respond(response)
        .await?;

    // Insert the write part of this peer to the peer map.
    let (user_sender, mut user_receiver) = mpsc::unbounded_channel();

    // assign this peer to a lobby
    let addr = session.remote_address();
    server_state.connect_user(addr, user_sender).await?;

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
                    println!("Connection has received corrupted header, stopping...");
                    bail!("Connection has received corrupted header, stopping...")
                }

                // read message size, (currently hardcoded to be size u32)
                incoming.read_exact(&mut message_size_buf).await?;
                let message_size: u32 = u32::from_be_bytes(message_size_buf);

                let chunk = incoming.read_chunk(message_size as usize, true).await?.expect("There should be a chunk here we can use");
                let message = decode_client_message(&chunk.bytes)?;

                println!("message received!");

                let user_rpc_message = UserRPCMessage {
                    message,
                    send_addr: addr
                };

                server_state
                    .handle_user_rpc(user_rpc_message)
                    .await
                    .expect("Error handling user rpc");
            } else if let Err(e) = message_read_result {
                println!("Incoming messages have stopped, error {e}");
                break;
            }
        }

        Ok(())
    });    

    // forward the binary websocket messages from user receiver into the web socket stream itself
    let receive_from_others = tokio::spawn(async move {
        while let Some(message) = user_receiver.recv().await {
            if let Err(e) = outgoing.write_all(&message).await {
                println!("{e}");
            }
        }
        println!("Receiver for user messages into outgoing stream stopped");
    });

    pin_mut!(broadcast_incoming, receive_from_others);
    future::select(broadcast_incoming, receive_from_others).await;

    println!("{} disconnected", &addr);
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
    // Create the event loop and TCP listener we'll accept connections on.
    let server_builder = web_transport_quinn::ServerBuilder::new().with_addr(SERVER_HOSTING_ADDRESS);

    let args = Args::parse();

     // Read the PEM certificate chain
    let chain = std::fs::File::open(args.tls_cert)?;
    let mut chain = std::io::BufReader::new(chain);

    let chain: Vec<CertificateDer> = rustls_pemfile::certs(&mut chain).map(|c| {c.unwrap()}).collect();

    anyhow::ensure!(!chain.is_empty(), "could not find certificate");

    // Read the PEM private key
    let keys = std::fs::File::open(args.tls_key).expect("failed to open key file");

    // Try to parse a PKCS#8 key
    // -----BEGIN PRIVATE KEY-----
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(keys))
        .context("failed to load private key")?
        .context("missing private key")?;


    let mut server : Server = server_builder.with_certificate(chain, key)?;
    println!("Listening on: {}", SERVER_HOSTING_ADDRESS);
    // spawn lobby actor
    let server_state = ServerState::new();

    // Let's spawn the handling of each connection in a separate task.
    let mut connection_set = JoinSet::new();
    while let Some(session) = server.accept().await {
        connection_set.spawn(handle_connection(session, server_state.clone()));
    }

    Ok(())
}
