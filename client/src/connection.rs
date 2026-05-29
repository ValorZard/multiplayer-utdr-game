#[cfg(target_arch = "wasm32")]
use futures_util::{SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;
use std::time::Duration;
#[cfg(target_arch = "wasm32")]
use ws_stream_wasm::{WsMessage, WsMeta};

const SERVER_ADDRESS: &str = "ws://127.0.0.1:12345/";

fn decode_message(bytes: &[u8]) -> Result<rpc::Message, rkyv::rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<rpc::Message, rkyv::rancor::Error>(aligned.as_ref())
}

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        #[cfg(target_arch = "wasm32")]
        web_sys::console::log_1(&format!($($arg)*).into());
        #[cfg(not(target_arch = "wasm32"))]
        println!($($arg)*);
    };
}

#[cfg(target_arch = "wasm32")]
pub async fn connect_to_websocket_server_wasm() {
    let (ws, mut wsio) = WsMeta::connect(SERVER_ADDRESS, None)
        .await
        .expect("websocket connection should succeed");

    let mut i = 0;
    loop {
        let message_to_send = rpc::Message::Text(format!("Hello WebSocket WASM {i}").to_string());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_to_send).unwrap();
        wsio.send(WsMessage::Binary(bytes.to_vec()))
            .await
            .expect("send should succeed");

        if let Some(reply) = wsio.next().await {
            match reply {
                WsMessage::Text(msg) => {
                    log!("Received text: {:?}", msg);
                }
                WsMessage::Binary(msg) => {
                    let deserialized = decode_message(msg.as_ref()).unwrap();
                    log!("Received binary: {:?}", deserialized);
                }
                _ => {
                    log!("Unexpected message: {:?}", reply);
                }
            }
        }
        i += 1;
        TimeoutFuture::new(1_000).await;
    }

    ws.close().await.expect("close should succeed");
}

#[cfg(not(target_arch = "wasm32"))]
pub fn connect_to_websocket_server_native() {
    use tungstenite::{Message, connect};
    let (mut socket, response) = connect(SERVER_ADDRESS).expect("Can't connect");

    println!("Connected to the server");
    println!("Response HTTP code: {}", response.status());
    println!("Response contains the following headers:");
    for (header, _value) in response.headers() {
        println!("* {header}");
    }

    let mut i = 0;

    loop {
        let message_to_send = rpc::Message::Text(format!("Hello WebSocket WASM {i}").to_string());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_to_send).unwrap();
        socket.send(Message::Binary(bytes.to_vec().into())).unwrap();
        let msg = socket.read().expect("Error reading message");
        match msg {
            Message::Text(msg) => {
                println!("Received text: {:?}", msg);
            }
            Message::Binary(msg) => {
                let deserialized = decode_message(msg.as_ref()).unwrap();
                println!("Received binary: {:?}", deserialized);
            }
            _ => {
                println!("Unexpected message: {:?}", msg);
            }
        }
        i += 1;
        std::thread::sleep(Duration::from_millis(1000));
    }
}
