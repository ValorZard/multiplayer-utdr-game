use futures_util::{FutureExt, StreamExt};
use include_dir::{Dir, include_dir};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use kiss3d::{egui, prelude::*};
use rpc::{GameInput, LobbyId, LobbyState, PlayerSide, RPSGameState, RPSWinState, RpcClientMessage, RpcServerMessage, YesOrNo};
#[cfg(not(target_arch = "wasm32"))]
use std::thread;

mod connection;

static ASSET_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR\\assets");

#[kiss3d::main]
async fn main() {
    let mut window = Window::new("Kiss3d: rectangle").await;
    let mut camera = PanZoomCamera2d::new(Vec2::ZERO, 2.0);
    let mut scene = SceneNode2d::empty();

    let image_buffer = ASSET_DIR.get_file("background_concept_2.png").unwrap();
    let mut texture_manager = TextureManager::new();
    let image_texture =
        texture_manager.add_image_from_memory(image_buffer.contents(), "background_concept_2.png");

    let (client_rpc_sender, client_rpc_receiver, server_rpc_sender, mut server_rpc_receiver) =
        connection::make_channels();

    #[cfg(target_arch = "wasm32")]
    {
        spawn_local(async move {
            crate::connection::connect_to_websocket_server_wasm(
                client_rpc_receiver,
                server_rpc_sender,
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
            )
        });
    }

    // UI state
    let mut current_game_state: Option<RPSGameState> = None;
    let mut lobby_id: Option<LobbyId> = None;
    let mut player_side: Option<PlayerSide> = None;
    let mut win_state: Option<RPSWinState> = None;
    let mut remote_right_input: Option<GameInput> = None;
    let mut remote_left_input: Option<GameInput> = None;
    let mut lobby_state: LobbyState = LobbyState::Empty;
    while window.render_2d(&mut scene, &mut camera).await {
        // immediately pool the receiver even if there isn't a value there.
        while let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten() {
            log!("{rpc_message:?}");
            match rpc_message {
                RpcServerMessage::Text(text) => {
                    // TODO: Do something here I guess
                }
                RpcServerMessage::GameState(game_state) => {
                    match &game_state {
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
                    current_game_state = Some(game_state);
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

                    ui.separator();

                    match lobby_state {
                        LobbyState::Empty => {}
                        LobbyState::Waiting => {}
                        LobbyState::Running => {
                            if ui.button("Rock").clicked() {
                                let _ = client_rpc_sender
                                    .unbounded_send(RpcClientMessage::GameInput(GameInput::Rock));
                            }
                            if ui.button("Paper").clicked() {
                                let _ = client_rpc_sender
                                    .unbounded_send(RpcClientMessage::GameInput(GameInput::Paper));
                            }
                            if ui.button("Scissors").clicked() {
                                let _ = client_rpc_sender
                                    .unbounded_send(RpcClientMessage::GameInput(GameInput::Scissors));
                            }
                        }
                        LobbyState::Finished => {
                            if ui.button("Yes").clicked() {
                                let _ = client_rpc_sender
                                    .unbounded_send(RpcClientMessage::ContinueRound(YesOrNo::Yes));
                            }
                            if ui.button("No").clicked() {
                                let _ = client_rpc_sender
                                    .unbounded_send(RpcClientMessage::ContinueRound(YesOrNo::No));
                            }
                        }
                    }
                });
        });
    }
}
