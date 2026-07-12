use std::collections::BTreeMap;

use rpc::{
    GameLogic, InputSequence, MoveGameState, MoveInputState, PlayerSide, RPSGameState,
    RPSWinState, TurnInput,
};

#[derive(Debug, PartialEq)]
pub enum GameError {
    InvalidInput,
    LeftInputAlreadySet,
    RightInputAlreadySet,
    RoundOver,
}

impl std::fmt::Display for GameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GameError::InvalidInput => write!(f, "Input is invalid!"),
            GameError::LeftInputAlreadySet => write!(f, "Input for left player already set!"),
            GameError::RightInputAlreadySet => write!(f, "Input for right player already set!"),
            GameError::RoundOver => write!(
                f,
                "Round is over! Please start a new round to take in new inputs"
            ),
        }
    }
}

impl std::error::Error for GameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

pub struct GameSession {
    left_input: Option<rpc::TurnInput>,
    right_input: Option<rpc::TurnInput>,
    game_logic: GameLogic,
    // the latest ack the server received
    right_remote_clock_ack: InputSequence,
    left_remote_clock_ack: InputSequence,
    left_pending_move_inputs: BTreeMap<InputSequence, MoveInputState>,
    right_pending_move_inputs: BTreeMap<InputSequence, MoveInputState>,
    // held/duplicated when nothing new arrived (dead reckoning)
    left_last_applied_input: MoveInputState,
    right_last_applied_input: MoveInputState,
    // this is to add timestamps to the snapshots we're sending to the client
    tick: InputSequence,
}

impl GameSession {
    pub fn new() -> Self {
        Self {
            left_input: None,
            right_input: None,
            game_logic: GameLogic::new(),
            left_remote_clock_ack: 0,
            right_remote_clock_ack: 0,
            left_pending_move_inputs: BTreeMap::new(),
            right_pending_move_inputs: BTreeMap::new(),
            left_last_applied_input: MoveInputState::default(),
            right_last_applied_input: MoveInputState::default(),
            tick: 0,
        }
    }

    pub fn compute_state(&self) -> RPSGameState {
        match self.left_input {
            None => match self.right_input {
                None => RPSGameState::StartRound,
                Some(right_input) => RPSGameState::WaitingForLeftInput { right_input },
            },
            Some(left_input) => match self.right_input {
                None => RPSGameState::WaitingForRightInput { left_input },
                Some(right_input) => match left_input {
                    TurnInput::Rock => match right_input {
                        TurnInput::Rock => RPSGameState::Win {
                            state: RPSWinState::Tie,
                            left_input,
                            right_input,
                        },
                        TurnInput::Paper => RPSGameState::Win {
                            state: RPSWinState::Right,
                            left_input,
                            right_input,
                        },
                        TurnInput::Scissors => RPSGameState::Win {
                            state: RPSWinState::Left,
                            left_input,
                            right_input,
                        },
                    },
                    TurnInput::Paper => match right_input {
                        TurnInput::Rock => RPSGameState::Win {
                            state: RPSWinState::Left,
                            left_input,
                            right_input,
                        },
                        TurnInput::Paper => RPSGameState::Win {
                            state: RPSWinState::Tie,
                            left_input,
                            right_input,
                        },
                        TurnInput::Scissors => RPSGameState::Win {
                            state: RPSWinState::Right,
                            left_input,
                            right_input,
                        },
                    },
                    TurnInput::Scissors => match right_input {
                        TurnInput::Rock => RPSGameState::Win {
                            state: RPSWinState::Right,
                            left_input,
                            right_input,
                        },
                        TurnInput::Paper => RPSGameState::Win {
                            state: RPSWinState::Left,
                            left_input,
                            right_input,
                        },
                        TurnInput::Scissors => RPSGameState::Win {
                            state: RPSWinState::Tie,
                            left_input,
                            right_input,
                        },
                    },
                },
            },
        }
    }

    pub fn get_left_remote_clock_ack(&self) -> InputSequence {
        self.left_remote_clock_ack
    }

    pub fn get_right_remote_clock_ack(&self) -> InputSequence {
        self.right_remote_clock_ack
    }

    // you can only set this once per turn
    pub fn set_left_turn_input(
        &mut self,
        input: rpc::TurnInput,
    ) -> Result<RPSGameState, GameError> {
        if self.left_input.is_none() {
            self.left_input = Some(input);
        }
        Ok(self.compute_state())
    }

    // you can only set this once per turn
    pub fn set_right_turn_input(
        &mut self,
        input: rpc::TurnInput,
    ) -> Result<RPSGameState, GameError> {
        if self.right_input.is_none() {
            self.right_input = Some(input);
        }
        Ok(self.compute_state())
    }

    pub fn set_left_move_input(&mut self, input: rpc::MoveInputState, sequence: InputSequence) {
        // accept only strictly newer sequence ids than the latest applied
        if sequence > self.left_remote_clock_ack {
            self.left_pending_move_inputs.entry(sequence).or_insert(input);
        }
    }

    pub fn set_right_move_input(&mut self, input: rpc::MoveInputState, sequence: InputSequence) {
        // accept only strictly newer sequence ids than the latest applied
        if sequence > self.right_remote_clock_ack {
            self.right_pending_move_inputs.entry(sequence).or_insert(input);
        }
    }

    pub fn get_move_state(&mut self) -> MoveGameState {
        self.game_logic.get_state_to_send_to_client()
    }

    pub fn get_tick(&self) -> InputSequence { self.tick }

    pub fn step(&mut self) {
        // Unreliable packets can be dropped permanently, so do not stall waiting
        // for contiguous sequences. Apply the next available received input.
        // TODO: Add server-side prediction maybe?
        let left_input = self
            .left_pending_move_inputs
            .pop_first()
            .map(|(seq, input)| {
                self.left_remote_clock_ack = seq;
                input
            })
            .unwrap_or(self.left_last_applied_input);
        let right_input = self
            .right_pending_move_inputs
            .pop_first()
            .map(|(seq, input)| {
                self.right_remote_clock_ack = seq;
                input
            })
            .unwrap_or(self.right_last_applied_input);

        self.left_last_applied_input = left_input;
        self.right_last_applied_input = right_input;

        self.game_logic.update_position_with_input(PlayerSide::Left, &left_input);
        self.game_logic.update_position_with_input(PlayerSide::Right, &right_input);
        self.game_logic.step_physics();

        self.tick = self.tick.wrapping_add(1);
    }
}
