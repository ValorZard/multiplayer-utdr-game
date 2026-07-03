use glam::Vec2;
use rapier2d::prelude::PhysicsPipeline;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize, rancor, util::AlignedVec};
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
pub enum ReliableRpcClientMessage {
    Text(String),
    TurnInput(TurnInput),
    ContinueRound(YesOrNo),
    JoinLobby,
    Heartbeat,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum UnreliableRpcClientMessage {
    MoveInput {
        input: MoveInputState,
        sequence: InputSequence,
    },
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
pub type InputSequence = u32;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct MoveGameState {
    pub left_position: Vec2,
    pub right_position: Vec2,
    pub left_last_processed_input: InputSequence,
    pub right_last_processed_input: InputSequence,
}

impl MoveGameState {
    pub fn new() -> Self {
        Self {
            left_position: Vec2::ZERO,
            right_position: Vec2::ZERO,
            left_last_processed_input: 0,
            right_last_processed_input: 0,
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum ReliableRpcServerMessage {
    GameState {
        state: RPSGameState,
        left_side_score: ScoreSize,
        right_side_score: ScoreSize,
    },
    LobbyInit(PlayerSide, UserId, LobbyId),
    LobbyState(LobbyState),
    Text(String),
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum UnreliableRpcServerMessage {
    MoveGameState(MoveGameState),
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
#[derive(Debug, PartialEq)]
pub enum LogicError {
    PlayerAlreadyExists(PlayerSide),
    PlayerDoesNotExist(PlayerSide),
}

impl std::fmt::Display for LogicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicError::PlayerAlreadyExists(side) => {
                write!(f, "Error! Player {side:?} already exists!")
            }
            LogicError::PlayerDoesNotExist(side) => {
                write!(f, "Error! Player {side:?} does not exist!")
            }
        }
    }
}

impl std::error::Error for LogicError {}

pub struct GameLogic {
    world: hecs::World,
    physics: PhysicsPipeline,
    left_player: hecs::Entity,
    right_player: hecs::Entity,
}

impl GameLogic {
    pub fn new() -> Self {
        // TODO: for now, we hardcode left and right side players and require there to be only one of each
        let mut world = hecs::World::new();
        let physics = PhysicsPipeline::new();
        let left_player = world.spawn((PlayerSide::Left, Vec2::ZERO));
        let right_player = world.spawn((PlayerSide::Left, Vec2::ZERO));
        Self {
            world,
            physics,
            left_player,
            right_player,
        }
    }

    pub fn setup_game(&mut self) {
        // Easier way to do this is to just totally reset all state
        *self = Self::new();
    }

    pub fn update_position_with_input(
        &mut self,
        player_side: PlayerSide,
        input: &MoveInputState,
    ) -> Vec2 {
        match player_side {
            PlayerSide::Left => {
                let entity = self.left_player;
                let position = self
                    .world
                    .query_one_mut::<&mut Vec2>(entity)
                    .expect("Player should exist here");
                *position += input.as_normalized_vec() * PLAYER_SPEED * GAME_TIME_DELTA ;
                position.clone()
            }
            PlayerSide::Right => {
                let entity = self.right_player;
                let position = self
                    .world
                    .query_one_mut::<&mut Vec2>(entity)
                    .expect("Player should exist here");
                *position += input.as_normalized_vec() * PLAYER_SPEED * GAME_TIME_DELTA ;
                position.clone()
            }
        }
    }

    pub fn update_position_with_vec(
        &mut self,
        player_side: PlayerSide,
        new_position: Vec2,
    ) -> Vec2 {
        match player_side {
            PlayerSide::Left => {
                let entity = self.left_player;
                let position = self
                    .world
                    .query_one_mut::<&mut Vec2>(entity)
                    .expect("Player should exist here");
                *position = new_position;
                position.clone()
            }
            PlayerSide::Right => {
                let entity = self.right_player;
                let position = self
                    .world
                    .query_one_mut::<&mut Vec2>(entity)
                    .expect("Player should exist here");
                *position = new_position;
                position.clone()
            }
        }
    }

    pub fn get_position(&mut self, player_side: PlayerSide) -> Vec2 {
        match player_side {
            PlayerSide::Left => {
                let entity = self.left_player;
                let position = self
                    .world
                    .query_one_mut::<&mut Vec2>(entity)
                    .expect("Player should exist here");
                position.clone()
            }
            PlayerSide::Right => {
                let entity = self.right_player;
                let position = self
                    .world
                    .query_one_mut::<&mut Vec2>(entity)
                    .expect("Player should exist here");
                position.clone()
            }
        }
    }

    pub fn get_state_to_send_to_client(&mut self) -> MoveGameState {
        let left_position = self
            .world
            .query_one_mut::<&Vec2>(self.left_player)
            .expect("Left Player should exist")
            .clone();
        let right_position = self
            .world
            .query_one_mut::<&Vec2>(self.right_player)
            .expect("Left Player should exist")
            .clone();

        MoveGameState {
            left_position,
            right_position,
            left_last_processed_input: 0,
            right_last_processed_input: 0,
        }
    }
}

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
