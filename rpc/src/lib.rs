use glam::Vec2;
use rkyv::net::ArchivedSocketAddr;
use rkyv::{Archive, Deserialize, Serialize, rancor, util::AlignedVec};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
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

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct MoveInputState {
    pub up: bool,
    pub down: bool,
    pub left: bool,
    pub right: bool,
}
impl Default for MoveInputState {
    fn default() -> Self {
        Self {
            up: false,
            down: false,
            left: false,
            right: false,
        }
    }
}

impl MoveInputState {
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
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct MoveGameState {
    pub left_position: Vec2,
    pub right_position: Vec2,
}

impl MoveGameState {
    pub fn new() -> Self {
        Self {
            left_position: Vec2::ZERO,
            right_position: Vec2::ZERO,
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum RpcServerMessage {
    GameState {
        state: RPSGameState,
        left_side_score: ScoreSize,
        right_side_score: ScoreSize,
    },
    MoveGameState(MoveGameState),
    LobbyInit(PlayerSide, UserId, LobbyId),
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
pub enum TurnInput {
    Rock,
    Paper,
    Scissors,
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
    Turn(TurnInput),
    Move(MoveInputState),
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
        right_input: TurnInput,
    },
    WaitingForRightInput {
        left_input: TurnInput,
    },
    Win {
        state: RPSWinState,
        left_input: TurnInput,
        right_input: TurnInput,
    },
}

pub const HEADER_MESSAGE: [u8; 4] = [0, 3, 4, 5];

// deltarune runs on 30 TPS
pub const GAME_TIME_STEP: Duration = Duration::from_millis(33);
pub const GAME_TIME_DELTA: f32 = GAME_TIME_STEP.as_secs_f32();
pub const PLAYER_SPEED: f32 = 100.;

pub fn update_position(position: Vec2, input: &MoveInputState) -> Vec2 {
    position + (input.as_normalized_vec() * PLAYER_SPEED * GAME_TIME_DELTA)
}

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
