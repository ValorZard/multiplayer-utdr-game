use rkyv::{Archive, Deserialize, Serialize, util::AlignedVec};

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum RpcMessage {
    Text(String),
    GameInput(GameInput),
    // TODO: make sending RPS game state server only
    GameState(RPSGameState),
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
    WaitingForLeftInput,
    WaitingForRightInput,
    LeftWin,
    RightWin,
    Tie,
}

// messages sent from a websocket stream might not be aligned to what rkyv wants
pub fn decode_message(bytes: &[u8]) -> Result<RpcMessage, rkyv::rancor::Error> {
    let mut aligned: rkyv::util::AlignedVec = rkyv::util::AlignedVec::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<RpcMessage, rkyv::rancor::Error>(aligned.as_ref())
}

pub fn encode_message(message: &RpcMessage) -> Result<AlignedVec, rkyv::rancor::Error> {
    rkyv::to_bytes::<rkyv::rancor::Error>(message)
}
