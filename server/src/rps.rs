use rpc::RPSGameState;

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
    current_state: RPSGameState,
}

impl GameSession {
    pub fn new() -> Self {
        Self {
            left_input: None,
            right_input: None,
            current_state: RPSGameState::StartRound,
        }
    }

    pub fn compute_state(&mut self) -> RPSGameState {
        if self.left_input.is_none() && self.right_input.is_none() {
            self.current_state = RPSGameState::StartRound;
        } else if let Some(input) = &self.left_input
            && self.right_input.is_none()
        {
            self.current_state = RPSGameState::WaitingForRightInput {
                left_input: input.clone(),
            };
        } else if self.left_input.is_none()
            && let Some(input) = &self.right_input
        {
            self.current_state = RPSGameState::WaitingForLeftInput {
                right_input: input.clone(),
            };
        } else if let Some(left_input) = &self.left_input
            && let Some(right_input) = &self.right_input
        {
            match left_input {
                rpc::GameInput::Rock => match right_input {
                    rpc::GameInput::Rock => {
                        self.current_state = RPSGameState::Tie {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                    rpc::GameInput::Paper => {
                        self.current_state = RPSGameState::RightWin {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                    rpc::GameInput::Scissors => {
                        self.current_state = RPSGameState::LeftWin {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                },
                rpc::GameInput::Paper => match right_input {
                    rpc::GameInput::Rock => {
                        self.current_state = RPSGameState::LeftWin {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                    rpc::GameInput::Paper => {
                        self.current_state = RPSGameState::Tie {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                    rpc::GameInput::Scissors => {
                        self.current_state = RPSGameState::RightWin {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                },
                rpc::GameInput::Scissors => match right_input {
                    rpc::GameInput::Rock => {
                        self.current_state = RPSGameState::RightWin {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                    rpc::GameInput::Paper => {
                        self.current_state = RPSGameState::LeftWin {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                    rpc::GameInput::Scissors => {
                        self.current_state = RPSGameState::Tie {
                            left_input: left_input.clone(),
                            right_input: right_input.clone(),
                        }
                    }
                },
            }
        }

        return self.current_state.clone();
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
