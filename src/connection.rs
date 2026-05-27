#[cfg(target_arch = "wasm32")]
use futures_util::{SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use ws_stream_wasm::{WsMessage, WsMeta};

const SERVER_ADDRESS: &str = "wss://echo.websocket.org";

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

    wsio.send(WsMessage::Text("hello from kiss3d".to_string()))
        .await
        .expect("send should succeed");

    if let Some(reply) = wsio.next().await {
        log!("WebSocket reply: {:?}", reply);
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

    socket
        .send(Message::Text("Hello WebSocket".into()))
        .unwrap();
    let msg = socket.read().expect("Error reading message");
    println!("Received: {msg}");
}
