use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_util::{FutureExt, SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use rpc::{
    HEADER_MESSAGE, RpcClientMessage, RpcServerMessage, decode_server_message,
    encode_client_message,
};
use std::sync::LazyLock;
use url::Url;
use web_transport::{Client, ClientBuilder, RecvStream, SendStream};

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

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        #[cfg(target_arch = "wasm32")]
        web_sys::console::log_1(&format!($($arg)*).into());
        #[cfg(not(target_arch = "wasm32"))]
        println!($($arg)*);
    };
}

async fn send_loop(mut client_rpc_receiver: ClientRpcReceiver, mut send_stream: SendStream) {
    while let Some(rpc_message) = client_rpc_receiver.next().await {
        log!("sending rpc message {rpc_message:?}");
        let bytes = encode_client_message(&rpc_message).expect("should have message encoded");
        if let Err(error) = send_stream.write(&bytes).await {
            log!("send stopped while writing message: {error:?}");
            break;
        }
    }
}

async fn read_exact_bytes(recv_stream: &mut RecvStream, len: usize) -> Option<Vec<u8>> {
    let mut buffer = Vec::with_capacity(len);

    while buffer.len() < len {
        let remaining = len - buffer.len();
        let chunk = match recv_stream.read(remaining).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => return None,
            Err(error) => {
                log!("recv stopped while reading {len} bytes: {error:?}");
                return None;
            }
        };

        if chunk.is_empty() {
            continue;
        }

        buffer.extend_from_slice(&chunk);
    }

    Some(buffer)
}

async fn recv_loop(server_rpc_sender: ServerRpcSender, mut recv_stream: RecvStream) {
    while let Some(header_buf) = read_exact_bytes(&mut recv_stream, HEADER_MESSAGE.len()).await {
        if header_buf.as_slice() != HEADER_MESSAGE {
            log!("Connection has received corrupted header, stopping...");
            break;
        }

        // read message size, (currently hardcoded to be size u32)
        let Some(message_size_buf) = read_exact_bytes(&mut recv_stream, 4).await else {
            break;
        };
        let message_size_buf_slice: [u8; 4] = message_size_buf
            .as_slice()
            .try_into()
            .expect("Should be able to convert this to a 4 byte array.");
        let message_size: u32 = u32::from_be_bytes(message_size_buf_slice);

        let Some(chunk) = read_exact_bytes(&mut recv_stream, message_size as usize).await else {
            break;
        };
        let message = match decode_server_message(&chunk) {
            Ok(message) => message,
            Err(error) => {
                log!("Failed to decode server message, stopping: {error:?}");
                break;
            }
        };

        println!("Received binary: {:?}", message);
        let _ = server_rpc_sender.unbounded_send(message);
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn connect_to_webtransport_server_wasm(
    server_address: String,
    client_rpc_receiver: ClientRpcReceiver,
    server_rpc_sender: ServerRpcSender,
    connection_finished_sender: ConnectionFinishedSender,
) {
    let client_builder = ClientBuilder::new();
    let client: Client = client_builder
        .with_system_roots()
        .expect("trying to build client failed");
    let request_url = Url::parse(&*server_address).expect("should be valid url");
    let connection_result = client.connect(request_url).await;
    if let Ok(session) = connection_result {
        log!("Connected to the server");

        let (send_stream, recv_stream) = session.accept_bi().await.expect("Accept bi");

        // we want both loops to break if one of them drops
        futures_util::select! {
            _ = send_loop(client_rpc_receiver, send_stream).fuse() => (),
            _ = recv_loop(server_rpc_sender, recv_stream).fuse() => (),
        }
    } else if let Err(e) = connection_result {
        eprintln!(
            "WebTransport connect failed for {}: {:?}",
            server_address, e
        );
        return;
    }

    let _ = connection_finished_sender.send(());
}

#[cfg(not(target_arch = "wasm32"))]
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("This should succeed since we need to setup tokio with threads. If this fails, we got bigger problems")
});
#[cfg(not(target_arch = "wasm32"))]
pub fn connect_to_webtransport_server_native(
    server_address: String,
    client_rpc_receiver: ClientRpcReceiver,
    server_rpc_sender: ServerRpcSender,
    connection_finished_sender: ConnectionFinishedSender,
) -> anyhow::Result<()> {
    RUNTIME.block_on(async {
        let client_builder = ClientBuilder::new();
        let client: Client = client_builder
            .with_system_roots()
            .expect("trying to build client failed");
        let request_url = Url::parse(&server_address).expect("should be valid url");
        let connection_result = client.connect(request_url).await;
        if let Ok(session) = connection_result {
            log!("Connected to the server");

            let (send_stream, recv_stream) = session.accept_bi().await.expect("Accept bi");

            // we want both loops to break if one of them drops
            tokio::select! {
                _ = send_loop(client_rpc_receiver, send_stream) => (),
                _ = recv_loop(server_rpc_sender, recv_stream) => (),
            }
        } else if let Err(e) = connection_result {
            eprintln!(
                "WebTransport connect failed for {}: {:?}",
                server_address, e
            );
            return;
        }

        let _ = connection_finished_sender.send(());
    });

    Ok(())
}
