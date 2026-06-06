use rkyv::{Archive, Deserialize, Serialize, util::AlignedVec};
use uuid::Uuid;

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
}

pub type LobbyId = Uuid;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
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

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum RpcServerMessage {
    GameState(RPSGameState),
    Lobby(PlayerSide, LobbyId),
    Text(String),
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
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
pub enum RPSGameState {
    // waiting on inputs from both players here
    StartRound,
    WaitingForLeftInput {
        right_input: GameInput,
    },
    WaitingForRightInput {
        left_input: GameInput,
    },
    LeftWin {
        left_input: GameInput,
        right_input: GameInput,
    },
    RightWin {
        left_input: GameInput,
        right_input: GameInput,
    },
    Tie {
        left_input: GameInput,
        right_input: GameInput,
    },
}
