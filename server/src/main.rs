use anyhow::{Context, bail};
use axum::{
    Router,
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
};
use clap::Parser;
use futures_util::StreamExt;
use oauth2::basic::{BasicClient, BasicErrorResponse, BasicRevocationErrorResponse, BasicTokenIntrospectionResponse, BasicTokenResponse};
use oauth2::{AuthUrl, ClientId, CsrfToken, EndpointSet, RedirectUrl, Scope, StandardRevocableToken};
use rpc::{HEADER_MESSAGE, decode_message, encode_message, ReliableRpcServerMessage, UserId};
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path,
    sync::Arc,
};
use axum::routing::post;
use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, mpsc, oneshot},
    task::JoinSet,
};
use web_transport_quinn::{Request, Server, proto::ConnectResponse};

use crate::lobby_db::{ServerState, UserReliableRPCMessage, UserUnreliableRPCMessage};
use rustls::pki_types::CertificateDer;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use url::Url;
use web_transport_quinn::generic::Session;

#[deny(clippy::unwrap_used, clippy::panic)]

const SERVER_HOSTING_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 12345);


mod lobby;
mod lobby_db;
mod rps;

const OAUTH_BIND_ADDRESS: &str = "127.0.0.1:34567";
const OAUTH_PUBLIC_BASE_URL: &str = "http://127.0.0.1:34567";
const OAUTH_START_ENDPOINT: &str = "/oauth/start";
// this endpoint has to be set in itch.io itself when you create the OAuth thing on their end.
const REDIRECT_ENDPOINT: &str = "/oauth/callback?a=b";
const FRAGMENT_CAPTURE_ENDPOINT: &str = "/oauth/fragment";
// basically, when we do the redirect back to our redirect endpoint, we need a way of actually getting the info stored in the as hash (stored as a fragment)
// so, we use a tiny bit of javascript to grab the hash from the URI, and then do another request, this time sending the hash
// this is because we are using the Implicit Flow for OAuth
// https://itch.io/docs/api/oauth
// https://developer.mozilla.org/en-US/docs/Web/API/URL/hash
const FRAGMENT_FORWARDER_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>itch.io OAuth</title>
</head>
<body>
    <p>Finishing sign-in...</p>
    <script>
        const hash = window.location.hash.startsWith('#') ? window.location.hash.slice(1) : '';
        const query = hash.length > 0 ? `?${hash}` : '';
        window.location.replace('/oauth/fragment' + query);
    </script>
</body>
</html>
"#;


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct ItchProfileResponse {
    user: ItchUser,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct ItchUser {
    id: UserId,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct OAuthRequest {
    authorize_url: Url,
    csrf_state: CsrfToken,
}

impl From<(Url, CsrfToken)> for OAuthRequest {
    fn from((authorize_url, csrf): (Url, CsrfToken)) -> Self {
        Self {
            authorize_url,
            csrf_state: csrf,
        }
    }
}

#[derive(Clone)]
struct PendingOAuthRequests {
    map: Arc<tokio::sync::Mutex<HashMap<String, oneshot::Sender<ItchUser>>>>,
}

impl PendingOAuthRequests {
    fn new() -> Self {
        Self {
            map: Arc::new(tokio::sync::Mutex::new(HashMap::new()))
        }
    }

    async fn get_receiver(&self, csrf_token: &CsrfToken) -> oneshot::Receiver<ItchUser> {
        let (sender, receiver) = oneshot::channel();
        self.map.lock().await.insert(csrf_token.secret().clone(), sender);
        receiver
    }

    async fn submit_oauth_result(&self, csrf: &str, oauth_answer: ItchUser) -> Option<()>{
        let mut map = self.map.lock().await;
        let sender = map.remove(csrf)?;
        sender.send(oauth_answer).ok()
    }
}

#[derive(Clone)]
struct OAuthCallbackState{
    oauth_client: oauth2::Client<BasicErrorResponse, BasicTokenResponse, BasicTokenIntrospectionResponse, StandardRevocableToken, BasicRevocationErrorResponse, EndpointSet>,
    http_client : reqwest::Client,
    pending_oauth_requests: PendingOAuthRequests,
}


async fn start_oauth(State(state): State<OAuthCallbackState>) -> impl IntoResponse {
    // Generate the authorization URL to which we'll redirect the user.
    let oauth_request : OAuthRequest = state.oauth_client
        .authorize_url(CsrfToken::new_random)
        .use_implicit_flow()
        .add_scope(Scope::new("profile:me".to_string()))
        .url()
        .into();
    serde_json::to_string(&oauth_request).expect("failed to serialize oauth request")
}
async fn oauth_callback_handler() -> impl IntoResponse {
    Html(FRAGMENT_FORWARDER_HTML)
}

async fn oauth_fragment_handler(
    State(state): State<OAuthCallbackState>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let access_token = match params.get("access_token") {
        Some(token) if !token.is_empty() => token.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                "Missing access_token in fragment relay",
            );
        }
    };

    let returned_state = match params.get("state") {
        Some(csrf) if !csrf.is_empty() => CsrfToken::new(csrf.to_string()),
        _ => {
            return (StatusCode::BAD_REQUEST, "Missing state in fragment relay");
        }
    };

    /*
    if returned_state != state.expected_csrf_state {
        return (StatusCode::UNAUTHORIZED, "Invalid OAuth state");
    }
    */

    let scope = params.get("scope").cloned().unwrap_or_default();
    let token_type = params.get("token_type").cloned().unwrap_or_default();
    let expires_in = params.get("expires_in").cloned().unwrap_or_default();

    info!("itch.io returned the following access token:\n{access_token}\n");
    info!(
        "itch.io returned the following state:\n{:?}",
        returned_state
    );

    info!("itch.io returned the following token type:\n{token_type}\n");
    info!("itch.io returned the following expiration:\n{expires_in}\n");

    let scopes = if scope.is_empty() {
        Vec::new()
    } else {
        scope.split(',').collect::<Vec<_>>()
    };
    info!("itch.io returned the following scopes:\n{scopes:?}\n");

    let profile_response = match state.http_client
        .get("https://api.itch.io/profile")
        .header("Authorization", format!("Bearer {access_token}"))
        .header("Accept", "application/json")
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            warn!("Failed to fetch itch.io profile: {error}");
            return (StatusCode::BAD_GATEWAY, "Failed to fetch itch.io profile");
        }
    };

    let profile_status = profile_response.status();
    let profile_body = match profile_response.text().await {
        Ok(body) => body,
        Err(error) => {
            warn!("Failed to read itch.io profile response body: {error}");
            return (StatusCode::BAD_GATEWAY, "Failed to read itch.io profile response");
        }
    };

    if !profile_status.is_success() {
        warn!(
            "itch.io profile request returned non-success status {} with body: {}",
            profile_status,
            profile_body
        );
        return (StatusCode::BAD_GATEWAY, "itch.io profile request failed");
    }

    let profile = match serde_json::from_str::<ItchProfileResponse>(&profile_body) {
        Ok(response) => response,
        Err(error) => {
            warn!(
                "Failed to parse itch.io profile response: {error}. Raw body: {}",
                profile_body
            );
            return (
                StatusCode::BAD_GATEWAY,
                "Invalid itch.io profile response format",
            );
        }
    };

    info!("itch.io profile response status:\n{profile_status}\n");
    // we use the "id" field as a unique username
    info!("itch.io profile response body:\n{profile:?}\n");

    if let None = state.pending_oauth_requests.submit_oauth_result(returned_state.secret(), profile.user).await {
        (StatusCode::UNAUTHORIZED, "CSRF Token did not match!")
    } else {
        (StatusCode::OK, "You can return to the game client.")
    }
}

async fn handle_connection(request: Request, server_state: ServerState, http_client: reqwest::Client, pending_oauth_requests: PendingOAuthRequests) -> anyhow::Result<()> {
    info!("WebTransport connection established: {}", request.url);

    // Accept the session.
    let response = ConnectResponse::OK;
    let session = request.respond(response).await?;

    let (mut outgoing, mut incoming) = session.open_bi().await?;

    // assign this peer to a lobby
    // get oauth redirect url
    let oauth_request = http_client.post(OAUTH_PUBLIC_BASE_URL.to_owned() + OAUTH_START_ENDPOINT)
        .send()
        .await?
        .json::<OAuthRequest>()
        .await?;
    // wait until we receive an authorization response from itch.io's OAuth server
    let oauth_receiver = pending_oauth_requests.get_receiver(&oauth_request.csrf_state).await;
    let message = encode_message(&ReliableRpcServerMessage::ConnectionInit(oauth_request.authorize_url.into()))?;
    outgoing.write_all(&message).await?;
    // TODO: add timeout here
    let oauth_answer = oauth_receiver.await?;
    let addr = oauth_answer.id;

    // Insert the write part of this peer to the peer map.
    let (user_reliable_sender, mut user_reliable_receiver) = mpsc::unbounded_channel();
    let (user_unreliable_sender, mut user_unreliable_receiver) = mpsc::unbounded_channel();

    server_state
        .connect_user(addr, user_reliable_sender, user_unreliable_sender)
        .await?;

    let server_state_clone = server_state.clone();
    let broadcast_incoming = tokio::spawn(async move {
        let server_state = server_state_clone;
        let mut header_buf = [0_u8; HEADER_MESSAGE.len()];
        let mut message_size_buf = [0_u8; 4]; // u32 is 4 u8
        loop {
            let message_read_result = incoming.read_exact(&mut header_buf).await;
            if let Ok(()) = message_read_result {
                if header_buf != HEADER_MESSAGE {
                    bail!("Connection has received corrupted header, stopping...")
                }

                // read message size, (currently hardcoded to be size u32)
                incoming.read_exact(&mut message_size_buf).await?;
                let message_size: u32 = u32::from_be_bytes(message_size_buf);

                let chunk = incoming
                    .read_chunk(message_size as usize, true)
                    .await?
                    .expect("There should be a chunk here we can use");
                let message = decode_message(&chunk.bytes)?;
                info!("message received from {addr}: {message:?}");

                let user_rpc_message = UserReliableRPCMessage {
                    message,
                    send_addr: addr,
                };

                server_state
                    .handle_user_reliable_rpc(user_rpc_message)
                    .await
                    .expect("Error handling user rpc");
            } else if let Err(e) = message_read_result {
                warn!("Incoming messages have stopped, error {e}");
                break;
            }
        }

        Ok(())
    });

    let server_state_clone = server_state.clone();
    let session_for_datagram = session.clone();
    // We don't need to parse a header for a datagram since it's a single message
    let datagram_incoming = tokio::spawn(async move {
        let server_state = server_state_clone;
        while let Ok(datagram) = session_for_datagram.recv_datagram().await {
            let message = decode_message(&datagram).expect("Should be fine");
            let user_rpc_message = UserUnreliableRPCMessage {
                message,
                send_addr: addr,
            };
            let _ = server_state
                .handle_user_unreliable_rpc(user_rpc_message)
                .await;
        }
    });

    // forward the binary web transport messages from user receiver into the web transport stream itself
    let send_reliable_to_clients = tokio::spawn(async move {
        while let Some(message) = user_reliable_receiver.recv().await {
            let message = encode_message(&message).expect("this should be fine");
            if let Err(e) = outgoing.write_all(&message).await {
                warn!("{e}");
            }
        }
        warn!("Receiver for user messages into outgoing stream stopped");
    });
    let session_for_unreliable = session.clone();
    let send_unreliable_to_clients = tokio::spawn(async move {
        while let Some(message) = user_unreliable_receiver.recv().await {
            let message = encode_message(&message).expect("this should be fine");
            if let Err(e) = session_for_unreliable.send_datagram(message.into()) {
                warn!("{e}");
            }
        }
        warn!("Receiver for user messages into outgoing stream stopped");
    });

    tokio::select! {
        _ = broadcast_incoming => {},
        _ = datagram_incoming => {},
        _ = send_reliable_to_clients => {},
        _ = send_unreliable_to_clients => {},
    }

    info!("{} disconnected", &addr);
    server_state.disconnect_user(addr).await?;
    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:12345")]
    addr: std::net::SocketAddr,

    /// Use the certificates at this path, encoded as PEM.
    #[arg(long)]
    pub tls_cert: path::PathBuf,

    /// Use the private key at this path, encoded as PEM.
    #[arg(long)]
    pub tls_key: path::PathBuf,

    /// Optional WebTransport subprotocol to support.
    #[arg(long)]
    pub protocol: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;
    tracing_subscriber::fmt::init();
    // Create the event loop and TCP listener we'll accept connections on.
    let server_builder =
        web_transport_quinn::ServerBuilder::new().with_addr(SERVER_HOSTING_ADDRESS);

    let args = Args::parse();

    // Read the PEM certificate chain
    let chain = std::fs::File::open(args.tls_cert)?;
    let mut chain = std::io::BufReader::new(chain);

    let chain: Vec<CertificateDer> = rustls_pemfile::certs(&mut chain)
        .map(|c| c.expect("Could not load certificate"))
        .collect();

    anyhow::ensure!(!chain.is_empty(), "could not find certificate");

    // Read the PEM private key
    let keys = std::fs::File::open(args.tls_key).expect("failed to open key file");

    // Try to parse a PKCS#8 key
    // -----BEGIN PRIVATE KEY-----
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(keys))
        .context("failed to load private key")?
        .context("missing private key")?;

    let mut server: Server = server_builder.with_certificate(chain, key)?;
    info!("Listening on: {}", SERVER_HOSTING_ADDRESS);

    let pending_oauth_requests = PendingOAuthRequests::new();

    let pending_oauth_for_main_server = pending_oauth_requests.clone();
    let main_server_loop = tokio::spawn(async move {
        // spawn lobby actor
        let server_state = ServerState::new();

        // Let's spawn the handling of each connection in a separate task.
        let mut connection_set = JoinSet::new();

        // http request client for oauth
        let http_client = reqwest::Client::new();

        let pending_oauth_requests = pending_oauth_for_main_server;

        while let Some(session) = server.accept().await {
            let server_state = server_state.clone();
            let http_client = http_client.clone();
            let pending_oauth_requests = pending_oauth_requests.clone();
            connection_set.spawn(async move {
                if let Err(e) = handle_connection(session, server_state, http_client, pending_oauth_requests).await {
                    warn!("Connection error: {e}");
                }
            });
        }
    });

    let oauth_server_loop: JoinHandle<std::io::Result<()>> = tokio::spawn(async move {
        let itch_client_id = ClientId::new(env::var("ITCH_CLIENT_ID").expect("Should be set in .env file"));
        let auth_url = AuthUrl::new("https://itch.io/user/oauth".to_string())
            .expect("Invalid authorization endpoint URL");

        let redirect_url =
            RedirectUrl::new(OAUTH_PUBLIC_BASE_URL.to_string() + REDIRECT_ENDPOINT).expect("Should be able to generate a proper redirect url");

        // Set up the config for the itch.io OAuth2 process.
        let client = BasicClient::new(itch_client_id)
            .set_auth_uri(auth_url)
            .set_redirect_uri(redirect_url);

        let app_state = OAuthCallbackState {
            oauth_client : client,
            http_client: reqwest::Client::new(),
            pending_oauth_requests
        };

        let app = Router::new()
            .route("/oauth/start", post(start_oauth))
            .route("/oauth/callback", get(oauth_callback_handler))
            .route(FRAGMENT_CAPTURE_ENDPOINT, get(oauth_fragment_handler))
            .with_state(app_state);

        let listener = tokio::net::TcpListener::bind(OAUTH_BIND_ADDRESS).await?;
        info!("OAuth callback server listening on {OAUTH_BIND_ADDRESS}");

        axum::serve(listener, app).await
    });

    let _ = main_server_loop.await;
    if let Ok(Err(e)) = oauth_server_loop.await {
        warn!("OAuth loop exited with error: {e}");
    }

    Ok(())
}
