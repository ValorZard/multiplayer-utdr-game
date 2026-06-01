use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
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

pub fn make_channels() -> (ClientRpcSender, ClientRpcReceiver, ServerRpcSender, ServerRpcReceiver) {
    let (client_rpc_sender, client_rpc_receiver) = unbounded();
    let (server_rpc_sender, server_rpc_receiver) = unbounded();
    (client_rpc_sender, client_rpc_receiver, server_rpc_sender, server_rpc_receiver)
}

const SERVER_ADDRESS: &str = "ws://127.0.0.1:12345/";

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
) {
    let (ws, wsio) = WsMeta::connect(SERVER_ADDRESS, None)
        .await
        .expect("websocket connection should succeed");

    let (mut send_stream, mut recv_stream) = wsio.split();

    spawn_local(async move {
        let mut  client_rpc_receiver =  client_rpc_receiver;
        loop {
            match  client_rpc_receiver.next().await {
                Some(rpc_message) => {
                    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&rpc_message).unwrap();
                    if let Err(e) = send_stream
                        .send(WsMessage::Binary(bytes.to_vec().into()))
                        .await
                    {
                        println!("Error! Breaking send loop: {e}");
                        break;
                    }
                }
                None => break,
            }
        }
    });
    loop {
        if let Some(reply) = recv_stream.next().await {
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
        } else {
            // error, break
            break;
        }
    }

    ws.close().await.expect("close should succeed");
}

#[cfg(not(target_arch = "wasm32"))]
pub fn connect_to_websocket_server_native(
    mut client_rpc_receiver: ClientRpcReceiver,
    server_rpc_sender: ServerRpcSender,
) {
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (socket, response) = connect_async(SERVER_ADDRESS).await.expect("Can't connect");

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

            let _ = tokio::join!(send_loop, recv_loop);
        })
}
