use anyhow::Result;
use futures_util::{SinkExt, StreamExt, future, pin_mut, stream::TryStreamExt};
use ipnet::IpNet;
use rkyv::rancor;
use rkyv::util::AlignedVec;
use rpc::{RpcClientMessage, RpcServerMessage};
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgPool, Postgres};
use std::{
    cell::{LazyCell, OnceCell},
    collections::HashMap,
    env,
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

const SERVER_HOSTING_ADDRESS: &str = "0.0.0.0:12345";
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
    mut db_executor: PgPool,
) -> Result<()> {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // insert user into table
    let ip: IpNet = addr.ip().into();
    let user_id = sqlx::query!(
        r#"
INSERT INTO users ( ip, port )
VALUES ( $1, $2 )
RETURNING id
        "#,
        ip,
        addr.port() as i32
    )
    .fetch_one(&db_executor)
    .await?;
    println!("New user {user_id:?}");

    // Insert the write part of this peer to the peer map.
    let (user_sender, user_receiver) = mpsc::unbounded_channel();

    // TODO: We can probably fit this into a single database call, gotta figure out how to do that.
    // assign this peer to a lobby
    // first check if we have a lobby waiting a player
    let lobby_id;

    // Try lobby with left occupied / right empty
    if let Ok(row) = sqlx::query!(
        r#"
SELECT id
FROM lobbies
WHERE left_player IS NOT NULL
  AND right_player IS NULL
FOR UPDATE SKIP LOCKED
LIMIT 1
    "#
    )
    .fetch_one(&db_executor)
    .await
    {
        sqlx::query!(
            r#"
UPDATE lobbies
SET right_player = $1
WHERE id = $2
        "#,
            user_id.id,
            row.id
        )
        .execute(&db_executor)
        .await?;
        lobby_id = row.id;
    }
    // Try lobby with right occupied / left empty
    else if let Ok(row) = sqlx::query!(
        r#"
SELECT id
FROM lobbies
WHERE left_player IS NULL
  AND right_player IS NOT NULL
FOR UPDATE SKIP LOCKED
LIMIT 1
    "#
    )
    .fetch_one(&db_executor)
    .await
    {
        sqlx::query!(
            r#"
UPDATE lobbies
SET left_player = $1
WHERE id = $2
        "#,
            user_id.id,
            row.id
        )
        .execute(&db_executor)
        .await?;

        lobby_id = row.id;
    } else {
        // Otherwise create a new lobby and place player in left slot
        lobby_id = sqlx::query!(
            r#"
INSERT INTO lobbies (left_player, right_player)
VALUES ($1, NULL)
RETURNING id
    "#,
            user_id.id
        )
        .fetch_one(&db_executor)
        .await?
        .id;
    }

    println!("Player assigned to lobby {lobby_id:?}");

    let (mut outgoing, incoming) = ws_stream.split();

    // send our lobby id first
    let bytes = encode_server_message(&RpcServerMessage::Lobby(lobby_id))
        .expect("Error serializing LobbyMessage");
    outgoing
        .send(WsMessage::Binary(bytes.to_vec().into()))
        .await
        .expect("initial lobby message should be sent");

    let broadcast_incoming = incoming.try_for_each(|msg| {
        match &msg {
            WsMessage::Binary(bytes) => match decode_client_message(bytes) {
                Ok(decoded) => {
                    println!("Received a binary message from {}: {:?}", addr, decoded);
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

    // remove user from database and lobby
    sqlx::query!("DELETE FROM users where id = $1", user_id.id).execute(&db_executor)
        .await?;
    sqlx::query!("UPDATE lobbies SET left_player = NULL where left_player = $1", user_id.id).execute(&db_executor)
        .await?;
    sqlx::query!("UPDATE lobbies SET right_player = NULL where right_player = $1", user_id.id).execute(&db_executor)
        .await?;
    sqlx::query!("DELETE FROM lobbies WHERE left_player IS NULL AND right_player IS NULL").execute(&db_executor)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(SERVER_HOSTING_ADDRESS).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", SERVER_HOSTING_ADDRESS);

    // set up database
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&env::var("DATABASE_URL")?)
        .await?;

    // create table for users if not exists

    // Let's spawn the handling of each connection in a separate task.
    while let Ok((stream, addr)) = listener.accept().await {
        let pool = pool.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, addr, pool).await {
                eprintln!("connection handler error for {addr}: {err:#}");
            }
        });
    }

    Ok(())
}
