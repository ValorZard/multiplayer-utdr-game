use rpc::{
    GameLogic, InputSequence, MoveGameState, PlayerSide, RPSGameState, RPSWinState, TurnInput,
};
use std::collections::VecDeque;

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
    left_last_processed_input: InputSequence,
    right_last_processed_input: InputSequence,
    left_pending_move_inputs: VecDeque<QueuedMoveInput>,
    right_pending_move_inputs: VecDeque<QueuedMoveInput>,
}

impl GameSession {
    pub fn new() -> Self {
        Self {
            left_input: None,
            right_input: None,
            game_logic: GameLogic::new(),
            left_last_processed_input: 0,
            right_last_processed_input: 0,
            left_pending_move_inputs: VecDeque::new(),
            right_pending_move_inputs: VecDeque::new(),
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
        Self::push_move_input(
            &mut self.left_pending_move_inputs,
            QueuedMoveInput { input, sequence },
        );
    }

    pub fn set_right_move_input(&mut self, input: rpc::MoveInputState, sequence: InputSequence) {
        Self::push_move_input(
            &mut self.right_pending_move_inputs,
            QueuedMoveInput { input, sequence },
        );
    }

    pub fn get_move_state(&mut self) -> MoveGameState {
        let mut state = self.game_logic.get_state_to_send_to_client();
        state.left_last_processed_input = self.left_last_processed_input;
        state.right_last_processed_input = self.right_last_processed_input;
        state
    }

    pub fn step(&mut self) {
        if let Some(next_input) = self.left_pending_move_inputs.pop_front() {
            self.game_logic
                .update_position_with_input(PlayerSide::Left, &next_input.input);
            self.left_last_processed_input = next_input.sequence;
        }

        if let Some(next_input) = self.right_pending_move_inputs.pop_front() {
            self.game_logic
                .update_position_with_input(PlayerSide::Right, &next_input.input);
            self.right_last_processed_input = next_input.sequence;
        }

        self.game_logic.step_physics();
    }

    fn push_move_input(queue: &mut VecDeque<QueuedMoveInput>, next_input: QueuedMoveInput) {
        if let Some(last) = queue.back()
            && next_input.sequence <= last.sequence
        {
            return;
        }

        queue.push_back(next_input);
        while queue.len() > MAX_QUEUED_MOVE_INPUTS {
            let _ = queue.pop_front();
        }
    }
}
