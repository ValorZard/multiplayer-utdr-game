use futures::select;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_util::FutureExt;
use futures_util::{SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use rkyv::util::AlignedVec;
use rpc::{GameInput, RPSGameState, RpcClientMessage, RpcServerMessage};
#[cfg(target_arch = "wasm32")]
use ws_stream_wasm::{WsMessage, WsMeta};

pub type ClientRpcSender = UnboundedSender<RpcClientMessage>;
pub type ClientRpcReceiver = UnboundedReceiver<RpcClientMessage>;
pub type ServerRpcSender = UnboundedSender<RpcServerMessage>;
pub type ServerRpcReceiver = UnboundedReceiver<RpcServerMessage>;

pub type ConnectionFinishedSender = oneshot::Sender<()>;
pub type ConnectionFinishedReceiver = oneshot::Receiver<()>;

pub fn make_channels() -> (
    ClientRpcSender,
    ClientRpcReceiver,
    ServerRpcSender,
    ServerRpcReceiver,
) {
    let (client_rpc_sender, client_rpc_receiver) = unbounded();
    let (server_rpc_sender, server_rpc_receiver) = unbounded();
    (
        client_rpc_sender,
        client_rpc_receiver,
        server_rpc_sender,
        server_rpc_receiver,
    )
}

// now that we are hosting on a proper server, we have to match the URL for it exactly for the websocket server to connect
const SERVER_ADDRESS: &str = "wss://167.233.56.216/server/";

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        #[cfg(target_arch = "wasm32")]
        web_sys::console::log_1(&format!($($arg)*).into());
        #[cfg(not(target_arch = "wasm32"))]
        println!($($arg)*);
    };
}

// messages sent from a websocket stream might not be aligned to what rkyv wants
pub fn decode_server_message(bytes: &[u8]) -> Result<RpcServerMessage, rkyv::rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<RpcServerMessage, rkyv::rancor::Error>(aligned.as_ref())
}

pub fn encode_client_message(
    message: &RpcClientMessage,
) -> Result<AlignedVec, rkyv::rancor::Error> {
    rkyv::to_bytes::<rkyv::rancor::Error>(message)
}

#[cfg(target_arch = "wasm32")]
pub async fn connect_to_websocket_server_wasm(
    client_rpc_receiver: ClientRpcReceiver,
    server_rpc_sender: ServerRpcSender,
    connection_finished_sender: ConnectionFinishedSender,
) {
    let (ws, wsio) = match WsMeta::connect(SERVER_ADDRESS, None).await {
        Ok(parts) => parts,
        Err(e) => {
            log!("WebSocket connect failed for {}: {:?}", SERVER_ADDRESS, e);
            let _ = connection_finished_sender.send(());
            return;
        }
    };

    let (mut send_stream, mut recv_stream) = wsio.split();

    let (loop_finished_sender, loop_finished_receiver) = futures_channel::oneshot::channel::<()>();

    spawn_local(async move {
        use futures_util::{FutureExt, SinkExt, StreamExt, select};

        let mut client_rpc_receiver = client_rpc_receiver;
        let mut loop_finished_receiver = loop_finished_receiver.fuse();

        'send_loop: loop {
            select! {
                rpc_message = client_rpc_receiver.next().fuse() => {
                    match rpc_message {
                        Some(rpc_message) => {
                            match rkyv::to_bytes::<rkyv::rancor::Error>(&rpc_message) {
                                Ok(bytes) => {
                                    if let Err(e) = send_stream
                                        .send(WsMessage::Binary(bytes.to_vec().into()))
                                        .await
                                    {
                                        log!("Error! Breaking send loop: {:?}", e);
                                        break 'send_loop;
                                    }
                                }
                                Err(e) => {
                                    log!("Failed to encode client message: {:?}", e);
                                    break 'send_loop;
                                }
                            }
                        }
                        None => break 'send_loop,
                    }
                },

                _ = loop_finished_receiver => {
                    log!("Stopping send loop because receive loop finished");
                    break 'send_loop;
                }
            }
        }
    });

    while let Some(reply) = recv_stream.next().await {
        match reply {
            WsMessage::Text(msg) => {
                log!("Received text: {:?}", msg);
            }
            WsMessage::Binary(msg) => {
                let deserialized = decode_server_message(msg.as_ref()).unwrap();
                log!("Received binary: {:?}", deserialized);
                let _ = server_rpc_sender.unbounded_send(deserialized);
            }
        }
    }

    log!("WebSocket connection closed for {}", SERVER_ADDRESS);

    let _ = loop_finished_sender.send(());

    if let Err(e) = ws.close().await {
        log!("WebSocket close failed for {}: {:?}", SERVER_ADDRESS, e);
    }

    let _ = connection_finished_sender.send(());
}

#[cfg(not(target_arch = "wasm32"))]
pub fn connect_to_websocket_server_native(
    mut client_rpc_receiver: ClientRpcReceiver,
    server_rpc_sender: ServerRpcSender,
    connection_finished_sender: ConnectionFinishedSender,
) {
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let connection_result = connect_async(SERVER_ADDRESS).await;
            if let Ok((socket, response)) = connection_result {
                println!("Connected to the server");
                println!("Response HTTP code: {}", response.status());
                println!("Response contains the following headers:");
                for (header, _value) in response.headers() {
                    println!("* {header}");
                }

                let (mut send_stream, mut recv_stream) = socket.split();

                let send_loop = tokio::spawn(async move {
                    while let Some(rpc_message) = client_rpc_receiver.next().await {
                        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&rpc_message).unwrap();
                        if let Err(e) = send_stream
                            .send(Message::Binary(bytes.to_vec().into()))
                            .await
                        {
                            println!("Error! Breaking send loop: {e}");
                            break;
                        }
                    }
                });

                let recv_loop = tokio::spawn(async move {
                    while let Some(msg) = recv_stream.next().await {
                        match msg {
                            Ok(Message::Text(msg)) => {
                                println!("Received text: {:?}", msg);
                            }
                            Ok(Message::Binary(msg)) => {
                                let deserialized = decode_server_message(msg.as_ref()).unwrap();
                                println!("Received binary: {:?}", deserialized);
                                let _ = server_rpc_sender.unbounded_send(deserialized);
                            }
                            Ok(msg) => {
                                println!("Unexpected message: {:?}", msg);
                            }
                            Err(e) => {
                                println!("Error! Breaking receive loop: {e}");
                                break;
                            }
                        }
                    }
                });

                // we want both loops to break if one of them drops
                tokio::select! {
                    _ = send_loop => (),
                    _ = recv_loop => (),
                }
            } else if let Err(e) = connection_result {
                eprintln!("WebSocket connect failed for {}: {:?}", SERVER_ADDRESS, e);
                return;
            }

            let _ = connection_finished_sender.send(());
        })
}
