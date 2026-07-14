use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_util::{SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use shared::rpc::{
    HEADER_MESSAGE, ReliableRpcClientMessage, ReliableRpcServerMessage, UnreliableRpcClientMessage,
    UnreliableRpcServerMessage, decode_message, encode_message,
};
use std::sync::LazyLock;
use url::Url;
use web_transport::{Client, ClientBuilder, RecvStream, SendStream};

pub type ReliableClientRpcSender = UnboundedSender<ReliableRpcClientMessage>;
pub type ReliableClientRpcReceiver = UnboundedReceiver<ReliableRpcClientMessage>;
pub type UnreliableClientRpcSender = UnboundedSender<UnreliableRpcClientMessage>;
pub type UnreliableClientRpcReceiver = UnboundedReceiver<UnreliableRpcClientMessage>;
pub type ReliableServerRpcSender = UnboundedSender<ReliableRpcServerMessage>;
pub type ReliableServerRpcReceiver = UnboundedReceiver<ReliableRpcServerMessage>;
pub type UnreliableServerRpcSender = UnboundedSender<UnreliableRpcServerMessage>;
pub type UnreliableServerRpcReceiver = UnboundedReceiver<UnreliableRpcServerMessage>;

pub type ConnectionFinishedSender = oneshot::Sender<()>;
pub type ConnectionFinishedReceiver = oneshot::Receiver<()>;

pub fn make_channels() -> (
    ReliableClientRpcSender,
    ReliableClientRpcReceiver,
    UnreliableClientRpcSender,
    UnreliableClientRpcReceiver,
    ReliableServerRpcSender,
    ReliableServerRpcReceiver,
    UnreliableServerRpcSender,
    UnreliableServerRpcReceiver,
) {
    let (reliable_client_rpc_sender, reliable_client_rpc_receiver) = unbounded();
    let (unreliable_client_rpc_sender, unreliable_client_rpc_receiver) = unbounded();
    let (reliable_server_rpc_sender, reliable_server_rpc_receiver) = unbounded();
    let (unreliable_server_rpc_sender, unreliable_server_rpc_receiver) = unbounded();
    (
        reliable_client_rpc_sender,
        reliable_client_rpc_receiver,
        unreliable_client_rpc_sender,
        unreliable_client_rpc_receiver,
        reliable_server_rpc_sender,
        reliable_server_rpc_receiver,
        unreliable_server_rpc_sender,
        unreliable_server_rpc_receiver,
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

async fn reliable_send_loop(
    mut client_rpc_receiver: ReliableClientRpcReceiver,
    mut send_stream: SendStream,
) {
    while let Some(rpc_message) = client_rpc_receiver.next().await {
        //log!("sending shared message {rpc_message:?}");
        let bytes = encode_message(&rpc_message).expect("should have message encoded");
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

async fn reliable_recv_loop(
    server_rpc_sender: ReliableServerRpcSender,
    mut recv_stream: RecvStream,
) {
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
        let message = match decode_message(&chunk) {
            Ok(message) => message,
            Err(error) => {
                log!("Failed to decode server message, stopping: {error:?}");
                break;
            }
        };

        //println!("Received binary: {:?}", message);
        let _ = server_rpc_sender.unbounded_send(message);
    }
}

async fn unreliable_send_loop(
    mut unreliable_client_rpc_receiver: UnreliableClientRpcReceiver,
    session: web_transport::Session,
) {
    while let Some(message) = unreliable_client_rpc_receiver.next().await {
        //log!("sending unreliable shared message {message:?}");
        let bytes = encode_message(&message).expect("should have message encoded");
        let _ = session.send_datagram(bytes.into()).await;
    }
}

// We don't need to parse a header for a datagram since it's a single message
async fn unreliable_recv_loop(
    unreliable_rpc_server_sender: UnreliableServerRpcSender,
    session: web_transport::Session,
) {
    while let Ok(datagram) = session.recv_datagram().await {
        let rpc_message = decode_message(&datagram).expect("should have message decoded");
        let _ = unreliable_rpc_server_sender.unbounded_send(rpc_message);
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn connect_to_webtransport_server_wasm(
    server_address: String,
    reliable_client_rpc_receiver: ReliableClientRpcReceiver,
    unreliable_client_rpc_receiver: UnreliableClientRpcReceiver,
    reliable_server_rpc_sender: ReliableServerRpcSender,
    unreliable_server_rpc_sender: UnreliableServerRpcSender,
    connection_finished_sender: ConnectionFinishedSender,
) {
    use futures_util::FutureExt;
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
            _ = reliable_send_loop(reliable_client_rpc_receiver, send_stream).fuse() => (),
            _ = reliable_recv_loop(reliable_server_rpc_sender, recv_stream).fuse() => (),
            _ = unreliable_send_loop(unreliable_client_rpc_receiver, session.clone()).fuse() => (),
            _ = unreliable_recv_loop(unreliable_server_rpc_sender, session.clone()).fuse() => (),
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
    reliable_client_rpc_receiver: ReliableClientRpcReceiver,
    unreliable_client_rpc_receiver: UnreliableClientRpcReceiver,
    reliable_server_rpc_sender: ReliableServerRpcSender,
    unreliable_server_rpc_sender: UnreliableServerRpcSender,
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
                _ = reliable_send_loop(reliable_client_rpc_receiver, send_stream) => (),
                _ = reliable_recv_loop(reliable_server_rpc_sender, recv_stream) => (),
                _ = unreliable_send_loop(unreliable_client_rpc_receiver, session.clone()) => (),
                _ = unreliable_recv_loop(unreliable_server_rpc_sender, session.clone()) => (),
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
