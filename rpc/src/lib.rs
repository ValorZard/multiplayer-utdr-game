use glam::Vec2;
use rapier2d::prelude::*;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize, rancor, util::AlignedVec};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Duration;
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
    JoinServer,
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
pub type UserId = u64;

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
    compare(PartialEq)
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
    GameState {
        state: RPSGameState,
        left_side_score: ScoreSize,
        right_side_score: ScoreSize,
    },
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
    MoveGameState {
        state: MoveGameState,
        acknowledged_sequence: InputSequence,
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
    physics: PhysicsWorld,
    left_player: hecs::Entity,
    right_player: hecs::Entity,
}

pub const PHYSICS_TO_PIXEL_SCALE: f32 = 50.0; // 1 meter in physics engine equals 50 pixels
pub const PIXEL_TO_PHYSICS_SCALE: f32 = 1.0 / PHYSICS_TO_PIXEL_SCALE;
pub const PLAYER_PHYSICS_RADIUS: f32 = 1.0; // in physics scale

impl GameLogic {
    pub fn new() -> Self {
        // TODO: for now, we hardcode left and right side players and require there to be only one of each
        let mut world = hecs::World::new();
        let mut physics = PhysicsWorld::new();

        let left_player = Self::spawn_player(&mut world, &mut physics, PlayerSide::Left);
        let right_player = Self::spawn_player(&mut world, &mut physics, PlayerSide::Right);

        Self {
            world,
            left_player,
            right_player,
            physics,
        }
    }

    fn spawn_player(
        world: &mut hecs::World,
        physics: &mut PhysicsWorld,
        side: PlayerSide,
    ) -> hecs::Entity {
        let rigid_body = RigidBodyBuilder::kinematic_position_based()
            .translation(Vec2::ZERO)
            .build();

        let collider = ColliderBuilder::ball(PLAYER_PHYSICS_RADIUS).build();

        let (body_handle, collider_handle) = physics.insert(rigid_body, collider);

        world.spawn((side, body_handle, collider_handle))
    }

    fn physics_handle_for(&mut self, player_side: PlayerSide) -> RigidBodyHandle {
        let entity = match player_side {
            PlayerSide::Left => self.left_player,
            PlayerSide::Right => self.right_player,
        };
        *self
            .world
            .query_one_mut::<&RigidBodyHandle>(entity)
            .expect("Player should exist here")
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
        let handle = self.physics_handle_for(player_side);
        let body = &mut self.physics.bodies[handle];
        let current_position = body.translation();
        let delta = input.as_normalized_vec() * PLAYER_SPEED * GAME_TIME_DELTA;
        let new_pos = Vec2::new(current_position.x + delta.x, current_position.y + delta.y);

        // Kinematic bodies don't move until you tell the physics step
        // where they're going next.
        body.set_next_kinematic_translation(new_pos);
        Vec2::new(new_pos.x, new_pos.y)
    }

    pub fn update_position_with_vec(
        &mut self,
        player_side: PlayerSide,
        new_position: Vec2,
    ) -> Vec2 {
        let handle = self.physics_handle_for(player_side);
        let body = &mut self.physics.bodies[handle];
        body.set_next_kinematic_translation(new_position);
        new_position
    }

    pub fn get_position(&mut self, player_side: PlayerSide) -> Vec2 {
        let handle = self.physics_handle_for(player_side);
        let t = self.physics.bodies[handle].translation();
        Vec2::new(t.x, t.y)
    }

    /// Advances the physics simulation. Call once per tick, after inputs
    /// have been applied via update_position_with_input/_vec.
    pub fn step_physics(&mut self) {
        self.physics.step();
    }

    pub fn get_state_to_send_to_client(&mut self) -> MoveGameState {
        let left_position = self.get_position(PlayerSide::Left);
        let right_position = self.get_position(PlayerSide::Right);

        MoveGameState {
            left_position,
            right_position,
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
