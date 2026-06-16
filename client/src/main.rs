use crate::connection::{ClientRpcSender, ConnectionFinishedReceiver, ServerRpcReceiver};
use futures_channel::oneshot;
use futures_util::future::FusedFuture;
use futures_util::{FutureExt, SinkExt, StreamExt};
use include_dir::{Dir, include_dir};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use kiss3d::{egui, prelude::*};
use rpc::{
    GameInput, LobbyId, LobbyState, PlayerSide, RPSGameState, RPSWinState, RpcClientMessage,
    RpcServerMessage, ScoreSize, YesOrNo,
};
#[cfg(not(target_arch = "wasm32"))]
use std::thread;
use time::{Duration, OffsetDateTime};

mod connection;

static ASSET_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR\\assets");

fn get_connection_receivers() -> (
    ClientRpcSender,
    ServerRpcReceiver,
    ConnectionFinishedReceiver,
) {
    let (client_rpc_sender, client_rpc_receiver, server_rpc_sender, server_rpc_receiver) =
        connection::make_channels();
    let (connection_finished_sender, connection_finished_receiver) = oneshot::channel::<()>();

    
    let connection_handle = thread::spawn(move || {
        crate::connection::connect_to_webtransport_server(
            client_rpc_receiver,
            server_rpc_sender,
            connection_finished_sender,
        )
    });
    

    (
        client_rpc_sender,
        server_rpc_receiver,
        connection_finished_receiver,
    )
}

#[derive(Debug)]
struct UiGameState {
    lobby_state: LobbyState,
    lobby_id: Option<LobbyId>,
    remote_right_input: Option<GameInput>,
    remote_left_input: Option<GameInput>,
    remote_right_score: ScoreSize,
    remote_left_score: ScoreSize,
    player_side: Option<PlayerSide>,
    win_state: Option<RPSWinState>,
    current_game_state: Option<RPSGameState>,
}

impl UiGameState {
    fn new() -> Self {
        Self {
            lobby_state: LobbyState::Empty,
            lobby_id: None,
            remote_right_input: None,
            remote_left_input: None,
            remote_right_score: 0,
            remote_left_score: 0,
            player_side: None,
            win_state: None,
            current_game_state: None,
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[kiss3d::main]
async fn main() {
    let mut window = Window::new("Kiss3d: rectangle").await;
    let mut camera = PanZoomCamera2d::new(Vec2::ZERO, 2.0);
    let mut scene = SceneNode2d::empty();

    let image_buffer = ASSET_DIR.get_file("background_concept_2.png").unwrap();
    let mut texture_manager = TextureManager::new();
    let image_texture =
        texture_manager.add_image_from_memory(image_buffer.contents(), "background_concept_2.png");

    let (mut client_rpc_sender, mut server_rpc_receiver, mut connection_finished_receiver) =
        get_connection_receivers();

    // UI state
    let mut ui_game_state = UiGameState::new();
    let mut is_input_selected = false;
    let mut previous_time = OffsetDateTime::now_utc();
    let mut timer = Duration::new(0, 0);
    let max_time_for_heartbeat = Duration::new(1, 0);
    while window.render_2d(&mut scene, &mut camera).await {
        let current_time = OffsetDateTime::now_utc();
        let time_since_last_frame = current_time - previous_time;
        timer += time_since_last_frame;
        if timer >= max_time_for_heartbeat {
            timer = Duration::new(0, 0);
            let _ = client_rpc_sender.unbounded_send(RpcClientMessage::Heartbeat);
            log!("Heartbeat");
        }
        previous_time = current_time;
        // set lobby state to empty if connection lost
        let check_current_connection_state = connection_finished_receiver.try_recv();
        let disconnected_from_server = if let Ok(Some(_)) = check_current_connection_state {
            log!("Connection dropped.");
            ui_game_state.reset();
            true
        } else if let Err(_) = check_current_connection_state {
            true
        } else {
            false
        };
        // immediately pool the receiver even if there isn't a value there.
        while let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten() {
            log!("{rpc_message:?}");
            match rpc_message {
                RpcServerMessage::Text(text) => {
                    // TODO: Do something here I guess
                }
                RpcServerMessage::GameState {
                    state,
                    left_side_score,
                    right_side_score,
                } => {
                    match &state {
                        RPSGameState::StartRound => {
                            // reset all game state on Start Round
                            ui_game_state.remote_left_input = None;
                            ui_game_state.remote_right_input = None;
                        }
                        RPSGameState::WaitingForLeftInput { right_input } => {
                            ui_game_state.remote_right_input = Some(right_input.clone());
                        }
                        RPSGameState::WaitingForRightInput { left_input } => {
                            ui_game_state.remote_left_input = Some(left_input.clone());
                        }
                        RPSGameState::Win {
                            state,
                            left_input,
                            right_input,
                        } => {
                            ui_game_state.win_state = Some(state.clone());
                            ui_game_state.remote_right_input = Some(right_input.clone());
                            ui_game_state.remote_left_input = Some(left_input.clone());
                        }
                    }
                    ui_game_state.remote_left_score = left_side_score;
                    ui_game_state.remote_right_score = right_side_score;
                    ui_game_state.current_game_state = Some(state);
                }
                RpcServerMessage::LobbyInit(side, id) => {
                    ui_game_state.lobby_id = Some(id);
                    ui_game_state.player_side = Some(side);
                }
                RpcServerMessage::LobbyState(state) => {
                    // unless lobby state is finished, we really shouldn't have a win state
                    match state {
                        LobbyState::Finished => {}
                        _ => {
                            ui_game_state.win_state = None;
                            is_input_selected = false;
                        }
                    }
                    ui_game_state.lobby_state = state;
                }
            }
        }

        for event in window.events().iter() {
            match event.value {
                WindowEvent::Key(Key::Space, Action::Press, _) => {
                    log!("Space pressed");
                }
                WindowEvent::MouseButton(MouseButton::Button1, Action::Press, _) => {
                    log!("Left click");
                }
                WindowEvent::Char(c) => {
                    log!("Typed char: {c}");
                }
                _ => {}
            }
        }

        // Draw UI
        window.draw_ui(|ctx| {
            egui::Window::new("Kiss3d egui Example")
                .default_width(300.0)
                .show(ctx, |ui| {
                    ui.label(format!("{ui_game_state:#?}"));

                    ui.separator();

                    match ui_game_state.lobby_state {
                        LobbyState::Empty => {
                            if ui.button("Join Lobby").clicked() {
                                let _ =
                                    client_rpc_sender.unbounded_send(RpcClientMessage::JoinLobby);
                                if disconnected_from_server {
                                    // reset the connection
                                    let parts = get_connection_receivers();
                                    client_rpc_sender = parts.0;
                                    server_rpc_receiver = parts.1;
                                    connection_finished_receiver = parts.2;
                                }
                            }
                        }
                        LobbyState::Waiting => {}
                        LobbyState::Running => {
                            if let Some(game_state) = &ui_game_state.current_game_state
                                && let Some(side) = ui_game_state.player_side
                            {
                                let round_start = if let RPSGameState::StartRound = game_state {
                                    true
                                } else {
                                    false
                                };
                                let waiting_on_us =
                                    if let RPSGameState::WaitingForLeftInput { .. } = game_state
                                        && side == PlayerSide::Left
                                    {
                                        true
                                    } else if let RPSGameState::WaitingForRightInput { .. } =
                                        game_state
                                        && side == PlayerSide::Right
                                    {
                                        true
                                    } else {
                                        false
                                    };
                                if round_start || waiting_on_us {
                                    if ui.button("Rock").clicked() {
                                        let _ = client_rpc_sender.unbounded_send(
                                            RpcClientMessage::GameInput(GameInput::Rock),
                                        );
                                    }
                                    if ui.button("Paper").clicked() {
                                        let _ = client_rpc_sender.unbounded_send(
                                            RpcClientMessage::GameInput(GameInput::Paper),
                                        );
                                    }
                                    if ui.button("Scissors").clicked() {
                                        let _ = client_rpc_sender.unbounded_send(
                                            RpcClientMessage::GameInput(GameInput::Scissors),
                                        );
                                    }
                                }
                            }
                        }
                        LobbyState::Finished => {
                            if !is_input_selected {
                                if ui.button("Yes").clicked() {
                                    let _ = client_rpc_sender.unbounded_send(
                                        RpcClientMessage::ContinueRound(YesOrNo::Yes),
                                    );
                                    is_input_selected = true;
                                }
                                if ui.button("No").clicked() {
                                    let _ = client_rpc_sender.unbounded_send(
                                        RpcClientMessage::ContinueRound(YesOrNo::No),
                                    );
                                    is_input_selected = true;
                                }
                            }
                        }
                    }
                });
        });
    }
}
