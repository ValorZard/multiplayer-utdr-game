use futures::select;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_util::FutureExt;
use futures_util::{SinkExt, StreamExt};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use rkyv::util::AlignedVec;
use rpc::{
    GameInput, HEADER_MESSAGE, RPSGameState, RpcClientMessage, RpcServerMessage,
    decode_client_message, decode_server_message, encode_client_message,
};
use url::Url;
use web_transport::quinn::proto::ConnectRequest;
use web_transport::{Client, ClientBuilder};
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
const SERVER_ADDRESS: &str = "https://127.0.0.1:12345/";

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        #[cfg(target_arch = "wasm32")]
        web_sys::console::log_1(&format!($($arg)*).into());
        #[cfg(not(target_arch = "wasm32"))]
        println!($($arg)*);
    };
}

pub fn connect_to_webtransport_server(
    mut client_rpc_receiver: ClientRpcReceiver,
    server_rpc_sender: ServerRpcSender,
    connection_finished_sender: ConnectionFinishedSender,
) -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let client_builder = ClientBuilder::new();
            let client: Client = client_builder
                .with_system_roots()
                .expect("trying to build client failed");
            let mut request_url = Url::parse(SERVER_ADDRESS).expect("should be valid url");
            let connection_result = client.connect(request_url).await;
            if let Ok(session) = connection_result {
                log!("Connected to the server");

                let (mut send_stream, mut recv_stream) =
                    session.accept_bi().await.expect("Accept bi");

                let send_loop = tokio::spawn(async move {
                    while let Some(rpc_message) = client_rpc_receiver.next().await {
                        log!("sending rpc message {rpc_message:?}");
                        let bytes = encode_client_message(&rpc_message)
                            .expect("should have message encoded");
                        send_stream.write(&bytes).await.expect("send should work");
                    }
                });

                let recv_loop = tokio::spawn(async move {
                    while let Ok(Some(header_buf)) = recv_stream.read(HEADER_MESSAGE.len()).await {
                        if *header_buf != HEADER_MESSAGE {
                            log!("Connection has received corrupted header, stopping...");
                            break;
                        }

                        // read message size, (currently hardcoded to be size u32)
                        let message_size_buf = recv_stream
                            .read(4)
                            .await
                            .expect("Has message size")
                            .expect("Should have chunk ready for message size");
                        let message_size_buf: Vec<u8> = message_size_buf.into();
                        let message_size_buf_slice: [u8; 4] = message_size_buf
                            .try_into()
                            .expect("Should be able to convert this to a 4 byte array.");
                        let message_size: u32 = u32::from_be_bytes(message_size_buf_slice);

                        let chunk = recv_stream
                            .read(message_size as usize)
                            .await
                            .expect("There should be a chunk here we can use")
                            .expect("Can unwrap option");
                        let message =
                            decode_server_message(&chunk).expect("Should be able to get message");

                        println!("Received binary: {:?}", message);
                        let _ = server_rpc_sender.unbounded_send(message);
                    }
                });

                // we want both loops to break if one of them drops
                tokio::select! {
                    _ = send_loop => (),
                    _ = recv_loop => (),
                }
            } else if let Err(e) = connection_result {
                eprintln!(
                    "WebTransport connect failed for {}: {:?}",
                    SERVER_ADDRESS, e
                );
                return;
            }

            let _ = connection_finished_sender.send(());
        });

    Ok(())
}
