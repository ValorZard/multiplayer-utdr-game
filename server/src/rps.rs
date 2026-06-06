use rpc::{GameInput, RPSGameState};

#[derive(Debug)]
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
    left_input: Option<rpc::GameInput>,
    right_input: Option<rpc::GameInput>,
}

impl GameSession {
    pub fn new() -> Self {
        Self {
            left_input: None,
            right_input: None,
        }
    }

    pub fn compute_state(&self) -> RPSGameState {
        match self.left_input {
            None => match self.right_input.clone() {
                None => RPSGameState::StartRound,
                Some(right_input) => RPSGameState::WaitingForLeftInput { right_input },
            },
            Some(left_input) => match self.right_input {
                None => RPSGameState::WaitingForRightInput { left_input },
                Some(right_input) => match left_input {
                    GameInput::Rock => match right_input {
                        GameInput::Rock => RPSGameState::Tie {
                            left_input,
                            right_input,
                        },
                        GameInput::Paper => RPSGameState::RightWin {
                            left_input,
                            right_input,
                        },
                        GameInput::Scissors => RPSGameState::LeftWin {
                            left_input,
                            right_input,
                        },
                    },
                    GameInput::Paper => match right_input {
                        GameInput::Rock => RPSGameState::LeftWin {
                            left_input,
                            right_input,
                        },
                        GameInput::Paper => RPSGameState::Tie {
                            left_input,
                            right_input,
                        },
                        GameInput::Scissors => RPSGameState::RightWin {
                            left_input,
                            right_input,
                        },
                    },
                    GameInput::Scissors => match right_input {
                        GameInput::Rock => RPSGameState::RightWin {
                            left_input,
                            right_input,
                        },
                        GameInput::Paper => RPSGameState::LeftWin {
                            left_input,
                            right_input,
                        },
                        GameInput::Scissors => RPSGameState::Tie {
                            left_input,
                            right_input,
                        },
                    },
                },
            },
        }
    }

    pub fn set_left_input(&mut self, input: rpc::GameInput) -> Result<RPSGameState, GameError> {
        if self.left_input.is_none() {
            self.left_input = Some(input);
            return Ok(self.compute_state());
        }
        Err(GameError::LeftInputAlreadySet)
    }

    pub fn set_right_input(&mut self, input: rpc::GameInput) -> Result<RPSGameState, GameError> {
        if self.right_input.is_none() {
            self.right_input = Some(input);
            return Ok(self.compute_state());
        }
        Err(GameError::RightInputAlreadySet)
    }
}
