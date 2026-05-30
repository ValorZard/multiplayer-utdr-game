#[cfg(target_arch = "wasm32")]
use futures_util::{SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use gloo_timers::future::TimeoutFuture;
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use std::time::Duration;
#[cfg(target_arch = "wasm32")]
use ws_stream_wasm::{WsMessage, WsMeta};

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

#[cfg(target_arch = "wasm32")]
pub async fn connect_to_websocket_server_wasm() {
    let (ws, mut wsio) = WsMeta::connect(SERVER_ADDRESS, None)
        .await
        .expect("websocket connection should succeed");

    let (mut send_stream, mut recv_stream) = wsio.split();

    spawn_local(async move {
        let mut i = 0;
        loop {
            let message_to_send =
                rpc::RpcMessage::Text(format!("Hello WebSocket WASM {i}").to_string());
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_to_send).unwrap();
            if let Err(e) = send_stream.send(WsMessage::Binary(bytes.to_vec())).await {
                break;
            }
            i += 1;
            TimeoutFuture::new(1000).await;
        }
    });
    loop {
        if let Some(reply) = recv_stream.next().await {
            match reply {
                WsMessage::Text(msg) => {
                    log!("Received text: {:?}", msg);
                }
                WsMessage::Binary(msg) => {
                    let deserialized = rpc::decode_message(msg.as_ref()).unwrap();
                    log!("Received binary: {:?}", deserialized);
                }
                _ => {
                    log!("Unexpected message: {:?}", reply);
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
pub fn connect_to_websocket_server_native() {
    use std::sync::{Arc, Mutex};
    use tungstenite::{Message, connect};
    let (socket, response) = connect(SERVER_ADDRESS).expect("Can't connect");

    println!("Connected to the server");
    println!("Response HTTP code: {}", response.status());
    println!("Response contains the following headers:");
    for (header, _value) in response.headers() {
        println!("* {header}");
    }

    let socket = Arc::new(Mutex::new(socket));
    let send_socket = Arc::clone(&socket);

    let send_loop = std::thread::spawn(move || {
        let mut i = 0;
        loop {
            let message_to_send =
                rpc::RpcMessage::Text(format!("Hello WebSocket Native {i}").to_string());
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_to_send).unwrap();
            let mut socket = match send_socket.lock() {
                Ok(socket) => socket,
                Err(e) => {
                    println!("Error! Breaking send loop (mutex poisoned): {e}");
                    break;
                }
            };

            if let Err(e) = socket.send(Message::Binary(bytes.to_vec().into())) {
                println!("Error! Breaking send loop: {e}");
                break;
            }

            drop(socket);
            i += 1;
            std::thread::sleep(Duration::from_millis(1000));
        }
    });

    loop {
        let msg = {
            match socket.lock() {
                Ok(mut socket) => socket.read(),
                Err(e) => {
                    println!("Error! Breaking receive loop (mutex poisoned): {e}");
                    break;
                }
            }
        };

        match msg {
            Ok(Message::Text(msg)) => {
                println!("Received text: {:?}", msg);
            }
            Ok(Message::Binary(msg)) => {
                let deserialized = rpc::decode_message(msg.as_ref()).unwrap();
                println!("Received binary: {:?}", deserialized);
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

    let _ = send_loop.join();
}
