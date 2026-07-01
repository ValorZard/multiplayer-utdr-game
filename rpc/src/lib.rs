use glam::Vec2;
use rkyv::{Archive, Deserialize, Serialize, rancor, util::AlignedVec};
use std::net::SocketAddr;
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
pub struct InputState {
    pub up: bool,
    pub down: bool,
    pub left: bool,
    pub right: bool,
}
impl Default for InputState {
    fn default() -> Self {
        Self {
            up: false,
            down: false,
            left: false,
            right: false,
        }
    }
}

impl InputState {
    pub fn as_normalized_vec(&self) -> Vec2 {
        Vec2::new(
            (self.right as i8 - self.left as i8) as f32,
            (self.up as i8 - self.down as i8) as f32,
        )
        .normalize_or_zero()
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
pub enum RpcClientMessage {
    Text(String),
    GameInput(GameInput),
    ContinueRound(YesOrNo),
    JoinLobby,
    Heartbeat,
}

pub type LobbyId = Uuid;
pub type UserId = SocketAddr;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum PlayerSide {
    Left,
    Right,
}

pub type ScoreSize = u32;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum RpcServerMessage {
    GameState {
        state: RPSGameState,
        left_side_score: ScoreSize,
        right_side_score: ScoreSize,
    },
    LobbyInit(PlayerSide, LobbyId),
    LobbyState(LobbyState),
    Text(String),
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

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum GameInput {
    Rock,
    Paper,
    Scissors,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum RPSWinState {
    Left,
    Right,
    Tie,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum RPSGameState {
    // waiting on inputs from both players here
    StartRound,
    WaitingForLeftInput {
        right_input: GameInput,
    },
    WaitingForRightInput {
        left_input: GameInput,
    },
    Win {
        state: RPSWinState,
        left_input: GameInput,
        right_input: GameInput,
    },
}

pub const HEADER_MESSAGE: [u8; 4] = [0, 3, 4, 5];

// messages sent from a websocket stream might not be aligned to what rkyv wants
pub fn decode_client_message(bytes: &[u8]) -> Result<RpcClientMessage, rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<RpcClientMessage, rancor::Error>(aligned.as_ref())
}

pub fn encode_server_message(message: &RpcServerMessage) -> Result<Vec<u8>, rancor::Error> {
    let mut message_byte_vec = Vec::new();
    message_byte_vec.append(&mut HEADER_MESSAGE.to_vec());
    let message_as_bytes = rkyv::to_bytes::<rancor::Error>(message)?;
    let message_size = message_as_bytes.len() as u32;
    let message_size_buf = message_size.to_be_bytes();
    message_byte_vec.append(&mut message_size_buf.to_vec());
    message_byte_vec.append(&mut message_as_bytes.into_vec());
    Ok(message_byte_vec)
}

// messages sent from a websocket stream might not be aligned to what rkyv wants
pub fn decode_server_message(bytes: &[u8]) -> Result<RpcServerMessage, rkyv::rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<RpcServerMessage, rkyv::rancor::Error>(aligned.as_ref())
}

pub fn encode_client_message(message: &RpcClientMessage) -> Result<Vec<u8>, rkyv::rancor::Error> {
    let mut message_byte_vec = Vec::new();
    message_byte_vec.append(&mut HEADER_MESSAGE.to_vec());
    let message_as_bytes = rkyv::to_bytes::<rancor::Error>(message)?;
    let message_size = message_as_bytes.len() as u32;
    let message_size_buf = message_size.to_be_bytes();
    message_byte_vec.append(&mut message_size_buf.to_vec());
    message_byte_vec.append(&mut message_as_bytes.into_vec());
    Ok(message_byte_vec)
}
