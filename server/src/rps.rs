use rpc::{GameLogic, InputSequence, MoveGameState, PlayerSide, RPSGameState, RPSWinState, TurnInput, UnreliableRpcClientMessage};
use ringbuffer::{AllocRingBuffer, RingBuffer};

const MAX_QUEUED_MOVE_INPUTS: usize = 512;

#[derive(Clone, Copy)]
struct QueuedMoveInput {
    input: rpc::MoveInputState,
    sequence: InputSequence,
}

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
    // TODO: maybe make this a separate struct for the unreliable rpc client message to wrap around?
    left_pending_move_inputs: AllocRingBuffer<QueuedMoveInput>,
    right_pending_move_inputs: AllocRingBuffer<QueuedMoveInput>,
}

impl GameSession {
    pub fn new() -> Self {
        Self {
            left_input: None,
            right_input: None,
            game_logic: GameLogic::new(),
            left_remote_clock_ack: 0,
            right_remote_clock_ack: 0,
            left_pending_move_inputs: AllocRingBuffer::new(MAX_QUEUED_MOVE_INPUTS),
            right_pending_move_inputs: AllocRingBuffer::new(MAX_QUEUED_MOVE_INPUTS),
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
        self.left_pending_move_inputs.enqueue(QueuedMoveInput { input, sequence });
    }

    pub fn set_right_move_input(&mut self, input: rpc::MoveInputState, sequence: InputSequence) {
        self.right_pending_move_inputs.enqueue(QueuedMoveInput { input, sequence });
    }

    pub fn get_move_state(&mut self) -> MoveGameState {
        self.game_logic.get_state_to_send_to_client()
    }

    pub fn step(&mut self) {
        // Right now, we don't do any server side rollback, we just directly read the inputs as they come in
        // the only stuff we do here is update the remote clock ack for the player side
        for next_input in self.left_pending_move_inputs.drain() {
            self.game_logic
                .update_position_with_input(PlayerSide::Left, &next_input.input);
            if self.left_remote_clock_ack <= next_input.sequence {
                self.left_remote_clock_ack = next_input.sequence;
            }
        }

        for next_input in self.right_pending_move_inputs.drain() {
            self.game_logic
                .update_position_with_input(PlayerSide::Right, &next_input.input);
            if self.right_remote_clock_ack <= next_input.sequence {
                self.right_remote_clock_ack = next_input.sequence;
            }
        }

        self.game_logic.step_physics();
    }
}
