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
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let (socket, response) = connect_async(SERVER_ADDRESS)
                .await
                .expect("Can't connect");

            println!("Connected to the server");
            println!("Response HTTP code: {}", response.status());
            println!("Response contains the following headers:");
            for (header, _value) in response.headers() {
                println!("* {header}");
            }

            let (mut send_stream, mut recv_stream) = socket.split();

            let send_loop = tokio::spawn(async move {
                let mut i = 0;
                loop {
                    let message_to_send =
                        rpc::RpcMessage::Text(format!("Hello WebSocket Native {i}").to_string());
                    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&message_to_send).unwrap();
                    if let Err(e) = send_stream.send(Message::Binary(bytes.to_vec().into())).await {
                        println!("Error! Breaking send loop: {e}");
                        break;
                    }
                    i += 1;
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            });

            let recv_loop = tokio::spawn(async move {
                while let Some(msg) = recv_stream.next().await {
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
            });

            let _ = tokio::join!(send_loop, recv_loop);
        })
}
