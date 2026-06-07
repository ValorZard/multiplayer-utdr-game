use crate::connection::{ClientRpcSender, ConnectionFinishedReceiver, ServerRpcReceiver};
use futures_channel::oneshot;
use futures_util::future::FusedFuture;
use futures_util::{FutureExt, StreamExt};
use include_dir::{Dir, include_dir};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use kiss3d::{egui, prelude::*};
use rpc::{
    GameInput, LobbyId, LobbyState, PlayerSide, RPSGameState, RPSWinState, RpcClientMessage,
    RpcServerMessage, YesOrNo,
};
#[cfg(not(target_arch = "wasm32"))]
use std::thread;

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
    #[cfg(target_arch = "wasm32")]
    {
        console_error_panic_hook::set_once();
        spawn_local(async move {
            crate::connection::connect_to_websocket_server_wasm(
                client_rpc_receiver,
                server_rpc_sender,
                connection_finished_sender,
            )
            .await;
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _connection_handle = thread::spawn(move || {
            crate::connection::connect_to_websocket_server_native(
                client_rpc_receiver,
                server_rpc_sender,
                connection_finished_sender,
            )
        });
    }

    (
        client_rpc_sender,
        server_rpc_receiver,
        connection_finished_receiver,
    )
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

    let (client_rpc_sender, mut server_rpc_receiver, mut connection_finished_receiver) =
        get_connection_receivers();

    // UI state
    let mut current_game_state: Option<RPSGameState> = None;
    let mut lobby_id: Option<LobbyId> = None;
    let mut player_side: Option<PlayerSide> = None;
    let mut win_state: Option<RPSWinState> = None;
    let mut remote_right_input: Option<GameInput> = None;
    let mut remote_left_input: Option<GameInput> = None;
    let mut remote_right_score = 0;
    let mut remote_left_score = 0;
    let mut lobby_state: LobbyState = LobbyState::Empty;
    let mut is_input_selected = false;
    while window.render_2d(&mut scene, &mut camera).await {
        // set lobby state to empty if connection lost
        let check_current_connection_state = connection_finished_receiver.try_recv();
        if let Ok(Some(_)) = check_current_connection_state {
            log!("Connection dropped.");
            lobby_state = LobbyState::Empty;
        }

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
                            remote_left_input = None;
                            remote_right_input = None;
                        }
                        RPSGameState::WaitingForLeftInput { right_input } => {
                            remote_right_input = Some(right_input.clone());
                        }
                        RPSGameState::WaitingForRightInput { left_input } => {
                            remote_left_input = Some(left_input.clone());
                        }
                        RPSGameState::Win {
                            state,
                            left_input,
                            right_input,
                        } => {
                            win_state = Some(state.clone());
                        }
                    }
                    remote_left_score = left_side_score;
                    remote_right_score = right_side_score;
                    current_game_state = Some(state);
                }
                RpcServerMessage::LobbyInit(side, id) => {
                    lobby_id = Some(id);
                    player_side = Some(side);
                }
                RpcServerMessage::LobbyState(state) => {
                    // unless lobby state is finished, we really shouldn't have a win state
                    match state {
                        LobbyState::Finished => {}
                        _ => {
                            win_state = None;
                            is_input_selected = false;
                        }
                    }
                    lobby_state = state;
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
                    ui.label(format!("lobby id: {lobby_id:#?}"));
                    ui.label(format!("player side: {player_side:#?}"));
                    ui.label(format!(
                        "Left input {remote_left_input:?}, Right input {remote_right_input:?}"
                    ));
                    ui.label(format!("Game State: {current_game_state:?}"));
                    ui.label(format!("Lobby state: {lobby_state:?}"));
                    ui.label(format!("Win state: {win_state:?}"));
                    ui.label(format!("Left side score: {remote_left_score}, Right side score: {remote_right_score}"));

                    ui.separator();

                    match lobby_state {
                        LobbyState::Empty => {
                            if ui.button("Join Lobby").clicked() {
                                let _ = client_rpc_sender
                                    .unbounded_send(RpcClientMessage::JoinLobby);
                            }
                        }
                        LobbyState::Waiting => {}
                        LobbyState::Running => {
                            if let Some(game_state) = &current_game_state && let Some(side) = player_side {
                                let round_start = if let RPSGameState::StartRound = game_state {
                                    true
                                } else {
                                    false
                                };
                                let waiting_on_us = if let RPSGameState::WaitingForLeftInput { .. } = game_state && side == PlayerSide::Left {
                                    true
                                } else if let RPSGameState::WaitingForRightInput { .. } = game_state && side == PlayerSide::Right {
                                    true
                                } else {
                                    false
                                };
                                if round_start || waiting_on_us {
                                    if ui.button("Rock").clicked() {
                                        let _ = client_rpc_sender
                                            .unbounded_send(RpcClientMessage::GameInput(GameInput::Rock));
                                    }
                                    if ui.button("Paper").clicked() {
                                        let _ = client_rpc_sender
                                            .unbounded_send(RpcClientMessage::GameInput(GameInput::Paper));
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
