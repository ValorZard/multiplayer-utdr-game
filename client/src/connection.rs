use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_util::{FutureExt, StreamExt, future, pin_mut};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use rpc::{
    HEADER_MESSAGE, ReliableRpcClientMessage, ReliableRpcServerMessage, RpcClientMessage,
    RpcServerMessage, UnreliableRpcClientMessage, UnreliableRpcServerMessage,
    decode_server_message, encode_client_message,
};
use std::sync::LazyLock;
use url::Url;
use web_transport::{Client, ClientBuilder, RecvStream, SendStream};

pub type ClientReliableRpcSender = UnboundedSender<ReliableRpcClientMessage>;
pub type ClientReliableRpcReceiver = UnboundedReceiver<ReliableRpcClientMessage>;
pub type ClientUnreliableRpcSender = UnboundedSender<UnreliableRpcClientMessage>;
pub type ClientUnreliableRpcReceiver = UnboundedReceiver<UnreliableRpcClientMessage>;
pub type ServerReliableRpcSender = UnboundedSender<ReliableRpcServerMessage>;
pub type ServerReliableRpcReceiver = UnboundedReceiver<ReliableRpcServerMessage>;
pub type ServerUnreliableRpcSender = UnboundedSender<UnreliableRpcServerMessage>;
pub type ServerUnreliableRpcReceiver = UnboundedReceiver<UnreliableRpcServerMessage>;

pub type ConnectionFinishedSender = oneshot::Sender<()>;
pub type ConnectionFinishedReceiver = oneshot::Receiver<()>;

pub fn make_channels() -> (
    ClientReliableRpcSender,
    ClientReliableRpcReceiver,
    ClientUnreliableRpcSender,
    ClientUnreliableRpcReceiver,
    ServerReliableRpcSender,
    ServerReliableRpcReceiver,
    ServerUnreliableRpcSender,
    ServerUnreliableRpcReceiver,
) {
    let (client_reliable_rpc_sender, client_reliable_rpc_receiver) = unbounded();
    let (client_unreliable_rpc_sender, client_unreliable_rpc_receiver) = unbounded();
    let (server_reliable_rpc_sender, server_reliable_rpc_receiver) = unbounded();
    let (server_unreliable_rpc_sender, server_unreliable_rpc_receiver) = unbounded();
    (
        client_reliable_rpc_sender,
        client_reliable_rpc_receiver,
        client_unreliable_rpc_sender,
        client_unreliable_rpc_receiver,
        server_reliable_rpc_sender,
        server_reliable_rpc_receiver,
        server_unreliable_rpc_sender,
        server_unreliable_rpc_receiver,
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

async fn read_next_server_message(recv_stream: &mut RecvStream) -> Option<RpcServerMessage> {
    let Ok(Some(header_buf)) = recv_stream.read(HEADER_MESSAGE.len()).await else {
        return None;
    };

    if *header_buf != HEADER_MESSAGE {
        log!("Connection has received corrupted header, stopping...");
        return None;
    }

    let Ok(Some(message_size_buf)) = recv_stream.read(4).await else {
        return None;
    };
    let message_size_buf: Vec<u8> = message_size_buf.into();
    let message_size_buf_slice: [u8; 4] = message_size_buf
        .try_into()
        .expect("Should be able to convert this to a 4 byte array.");
    let message_size: u32 = u32::from_be_bytes(message_size_buf_slice);

    let Ok(Some(chunk)) = recv_stream.read(message_size as usize).await else {
        return None;
    };

    decode_server_message(&chunk).ok()
}

async fn send_loop(
    mut client_reliable_rpc_receiver: ClientReliableRpcReceiver,
    mut send_stream: SendStream,
) {
    while let Some(rpc_message) = client_reliable_rpc_receiver.next().await {
        let message = RpcClientMessage::Reliable(rpc_message);
        log!("sending reliable rpc message {message:?}");
        let bytes = encode_client_message(&message).expect("should have message encoded");
        if let Err(error) = send_stream.write(&bytes).await {
            log!("reliable send loop ended: {error}");
            break;
        }
    }
}

async fn send_unreliable_loop(
    mut client_unreliable_rpc_receiver: ClientUnreliableRpcReceiver,
    session: web_transport::Session,
) {
    while let Some(rpc_message) = client_unreliable_rpc_receiver.next().await {
        let message = RpcClientMessage::Unreliable(rpc_message);
        log!("sending unreliable rpc message {message:?}");
        let bytes = encode_client_message(&message).expect("should have message encoded");
        match session.open_uni_stream().await {
            Ok(mut uni_stream) => {
                if let Err(error) = uni_stream.write(&bytes).await {
                    log!("failed to write unreliable message on uni stream: {error}");
                }
            }
            Err(open_error) => {
                log!("failed to open unreliable uni stream: {open_error}");
            }
        }
    }
}

async fn recv_reliable_loop(
    server_reliable_rpc_sender: ServerReliableRpcSender,
    mut recv_stream: RecvStream,
) {
    while let Some(message) = read_next_server_message(&mut recv_stream).await {
        if let RpcServerMessage::Reliable(message) = message {
            let _ = server_reliable_rpc_sender.unbounded_send(message);
        }
    }
}

async fn recv_unreliable_loop(
    server_unreliable_rpc_sender: ServerUnreliableRpcSender,
    session: web_transport::Session,
) {
    enum UnreliableStep {
        Replaced(RecvStream),
        Message(Option<RpcServerMessage>),
        Closed,
    }

    let Ok(mut current_stream) = session.accept_uni_stream().await else {
        return;
    };

    loop {
        let step = {
            let accept_fut = session.accept_uni_stream().fuse();
            let read_fut = read_next_server_message(&mut current_stream).fuse();
            pin_mut!(accept_fut, read_fut);

            match future::select(accept_fut, read_fut).await {
                future::Either::Left((accepted, _)) => match accepted {
                    Ok(new_stream) => UnreliableStep::Replaced(new_stream),
                    Err(_) => UnreliableStep::Closed,
                },
                future::Either::Right((message, _)) => UnreliableStep::Message(message),
            }
        };

        match step {
            UnreliableStep::Replaced(new_stream) => {
                // Drop the previous stream and always keep reading the newest stream.
                current_stream = new_stream;
            }
            UnreliableStep::Message(message) => {
                if let Some(RpcServerMessage::Unreliable(message)) = message {
                    let _ = server_unreliable_rpc_sender.unbounded_send(message);
                }
                // Each unreliable packet should come on its own stream; move to the next stream.
                match session.accept_uni_stream().await {
                    Ok(new_stream) => {
                        current_stream = new_stream;
                    }
                    Err(_) => return,
                }
            }
            UnreliableStep::Closed => return,
        }
    }
}

trait SessionUniExt {
    fn open_uni_stream(
        &self,
    ) -> impl std::future::Future<Output = Result<SendStream, web_transport::Error>>;
    fn accept_uni_stream(
        &self,
    ) -> impl std::future::Future<Output = Result<RecvStream, web_transport::Error>>;
}

impl SessionUniExt for web_transport::Session {
    async fn open_uni_stream(&self) -> Result<SendStream, web_transport::Error> {
        self.open_uni().await
    }

    async fn accept_uni_stream(&self) -> Result<RecvStream, web_transport::Error> {
        self.accept_uni().await
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn connect_to_webtransport_server_wasm(
    server_address: String,
    client_reliable_rpc_receiver: ClientReliableRpcReceiver,
    client_unreliable_rpc_receiver: ClientUnreliableRpcReceiver,
    server_reliable_rpc_sender: ServerReliableRpcSender,
    server_unreliable_rpc_sender: ServerUnreliableRpcSender,
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
        let send_unreliable_session = session.clone();
        let recv_unreliable_session = session.clone();

        spawn_local(send_unreliable_loop(
            client_unreliable_rpc_receiver,
            send_unreliable_session,
        ));
        spawn_local(recv_unreliable_loop(
            server_unreliable_rpc_sender,
            recv_unreliable_session,
        ));

        // Reliable stream lifecycle controls overall connection lifetime.
        futures_util::select! {
            _ = send_loop(client_reliable_rpc_receiver, send_stream).fuse() => (),
            _ = recv_reliable_loop(server_reliable_rpc_sender, recv_stream).fuse() => (),
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
    client_reliable_rpc_receiver: ClientReliableRpcReceiver,
    client_unreliable_rpc_receiver: ClientUnreliableRpcReceiver,
    server_reliable_rpc_sender: ServerReliableRpcSender,
    server_unreliable_rpc_sender: ServerUnreliableRpcSender,
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
            let send_unreliable_session = session.clone();
            let recv_unreliable_session = session.clone();

            let send_unreliable_handle =
                tokio::spawn(send_unreliable_loop(client_unreliable_rpc_receiver, send_unreliable_session));
            let recv_unreliable_handle =
                tokio::spawn(recv_unreliable_loop(server_unreliable_rpc_sender, recv_unreliable_session));

            // Reliable stream lifecycle controls overall connection lifetime.
            tokio::select! {
                _ = send_loop(client_reliable_rpc_receiver, send_stream) => (),
                _ = recv_reliable_loop(server_reliable_rpc_sender, recv_stream) => (),
            }

            send_unreliable_handle.abort();
            recv_unreliable_handle.abort();
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
