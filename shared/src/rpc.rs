use crate::game::{MoveGameState, MoveInputState, PlayerSide, RPSGameState, TurnInput};
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize, rancor};
use std::collections::VecDeque;
use url::Url;
use uuid::Uuid;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum YesOrNo {
    Yes,
    No,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum ReliableRpcClientMessage {
    Text(String),
    TurnInput(TurnInput),
    ContinueRound(YesOrNo),
    JoinServer,
}

pub type InputSequence = u32;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct PendingMoveInput {
    pub input: MoveInputState,
    pub sequence: InputSequence,
}

// Right now, this is milliseconds in unix epoch time
pub type RemoteTimestamp = i64;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum UnreliableRpcClientMessage {
    Input {
        pending_inputs: VecDeque<PendingMoveInput>,
        // client-local send time (ms since Unix epoch). The server echoes this back
        // verbatim so the client can measure real round-trip time from its own clock,
        // instead of inferring latency from how far the input backlog has grown.
        client_send_time_ms: RemoteTimestamp,
    },
}

pub type LobbyId = Uuid;
pub type UserId = u64;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct RpcUrl(pub String);

impl From<Url> for RpcUrl {
    fn from(value: Url) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for RpcUrl {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Into<String> for RpcUrl {
    fn into(self) -> String {
        self.0
    }
}

impl TryFrom<RpcUrl> for Url {
    type Error = url::ParseError;

    fn try_from(value: RpcUrl) -> Result<Self, Self::Error> {
        Url::parse(&value.0)
    }
}

impl TryFrom<&RpcUrl> for Url {
    type Error = url::ParseError;

    fn try_from(value: &RpcUrl) -> Result<Self, Self::Error> {
        Url::parse(&value.0)
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum ConnectionInitMessage {
    FirstTime,
    WelcomeBack,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum ReliableRpcServerMessage {
    GameState(RPSGameState),
    // send oauth url for client to open up
    ConnectionAuthentication(RpcUrl),
    ConnectionInit(UserId, ConnectionInitMessage),
    LobbyInit(PlayerSide, LobbyId),
    LobbyState(LobbyState),
    Text(String),
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum UnreliableRpcServerMessage {
    GameState {
        state: MoveGameState,
        // the latest input the server received from the client and acknowledged
        acknowledged_sequence: InputSequence,
        // the server timestamp for this message
        tick: InputSequence,
        // client_send_time_ms echoed back verbatim from the most recent input message
        // received from this client, used purely for round-trip timing
        echo_client_time_ms: RemoteTimestamp,
    },
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum LobbyState {
    Empty,
    Waiting,
    Running,
    Finished,
}

pub const HEADER_MESSAGE: [u8; 4] = [0, 3, 4, 5];

pub fn decode_message<T>(bytes: &[u8]) -> Result<T, rancor::Error>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, rkyv::rancor::Strategy<rkyv::de::Pool, rancor::Error>>,
{
    let mut aligned = AlignedVec::<1>::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<T, rancor::Error>(aligned.as_ref())
}

pub fn encode_message<T>(message: &T) -> Result<Vec<u8>, rancor::Error>
where
    T: for<'a> Serialize<
        HighSerializer<AlignedVec, rkyv::ser::allocator::ArenaHandle<'a>, rancor::Error>,
    >,
{
    let mut out = HEADER_MESSAGE.to_vec();
    let message_as_bytes = rkyv::to_bytes::<rancor::Error>(message)?;
    let message_size = message_as_bytes.len() as u32;
    out.extend_from_slice(&message_size.to_be_bytes());
    out.extend_from_slice(message_as_bytes.as_ref());
    Ok(out)
}
