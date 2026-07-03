use crate::connection::{ConnectionFinishedReceiver, ReliableClientRpcSender, ReliableServerRpcReceiver, UnreliableClientRpcReceiver, UnreliableClientRpcSender, UnreliableServerRpcReceiver};
use futures_channel::oneshot;
use futures_util::{FutureExt, StreamExt};
use include_dir::{Dir, include_dir};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use kiss3d::{egui, prelude::*};
use rpc::{GAME_TIME_STEP, GameLogic, InputSequence, LobbyId, LobbyState, PlayerSide, RPSGameState, RPSWinState, ScoreSize, TurnInput, UserId, YesOrNo, ReliableRpcClientMessage, ReliableRpcServerMessage, UnreliableRpcServerMessage, UnreliableRpcClientMessage};
use std::collections::VecDeque;
#[cfg(not(target_arch = "wasm32"))]
use std::thread;
use time::{Duration, OffsetDateTime};

mod connection;

static ASSET_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/assets");

fn get_connection_receivers(
    server_address: String,
) -> (
    ReliableClientRpcSender,
    UnreliableClientRpcSender,
    ReliableServerRpcReceiver,
    UnreliableServerRpcReceiver,
    ConnectionFinishedReceiver,
) {
    let (reliable_client_rpc_sender, reliable_client_rpc_receiver, unreliable_client_rpc_sender, unreliable_client_rpc_receiver,
        reliable_server_rpc_sender, reliable_server_rpc_receiver, unreliable_server_rpc_sender, unreliable_server_rpc_receiver) =
        connection::make_channels();
    let (connection_finished_sender, connection_finished_receiver) = oneshot::channel::<()>();

    #[cfg(target_arch = "wasm32")]
    {
        console_error_panic_hook::set_once();
        spawn_local(async move {
            crate::connection::connect_to_webtransport_server_wasm(
                server_address,
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
            crate::connection::connect_to_webtransport_server_native(
                server_address,
                reliable_client_rpc_receiver,
                unreliable_client_rpc_receiver,
                reliable_server_rpc_sender,
                unreliable_server_rpc_sender,
                connection_finished_sender,
            )
        });
    }

    (
        reliable_client_rpc_sender,
        unreliable_client_rpc_sender,
        reliable_server_rpc_receiver,
        unreliable_server_rpc_receiver,
        connection_finished_receiver,
    )
}

#[derive(Debug)]
struct UiGameState {
    lobby_state: LobbyState,
    lobby_id: Option<LobbyId>,
    remote_right_input: Option<TurnInput>,
    remote_left_input: Option<TurnInput>,
    remote_right_score: ScoreSize,
    remote_left_score: ScoreSize,
    player_side: Option<PlayerSide>,
    win_state: Option<RPSWinState>,
    current_game_state: Option<RPSGameState>,
    user_id: Option<UserId>,
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
            user_id: None,
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[derive(Debug, serde::Deserialize)]
struct ClientConfig {
    servers: Vec<String>,
}

#[derive(Clone, Copy)]
struct PendingMoveInput {
    input: rpc::MoveInputState,
    sequence: InputSequence,
}

#[derive(Clone, Copy)]
struct TimedRemoteSnapshot {
    received_at: OffsetDateTime,
    position: Vec2,
}

const MAX_PENDING_INPUTS: usize = 256;
const MAX_REMOTE_SNAPSHOTS: usize = 64;
const REMOTE_INTERPOLATION_DELAY: Duration = Duration::milliseconds(100);

fn interpolate_remote_position(
    snapshots: &VecDeque<TimedRemoteSnapshot>,
    target_time: OffsetDateTime,
) -> Option<Vec2> {
    if snapshots.is_empty() {
        return None;
    }
    if snapshots.len() == 1 {
        return snapshots.front().map(|snapshot| snapshot.position);
    }

    if let Some(first) = snapshots.front()
        && target_time <= first.received_at
    {
        return Some(first.position);
    }

    for index in 1..snapshots.len() {
        let previous = snapshots
            .get(index - 1)
            .expect("snapshot index should be in bounds");
        let next = snapshots
            .get(index)
            .expect("snapshot index should be in bounds");

        if target_time <= next.received_at {
            let span = (next.received_at - previous.received_at).as_seconds_f32();
            if span <= 0.0 {
                return Some(next.position);
            }
            let alpha = ((target_time - previous.received_at).as_seconds_f32() / span)
                .clamp(0.0, 1.0);
            return Some(previous.position.lerp(next.position, alpha));
        }
    }

    snapshots.back().map(|snapshot| snapshot.position)
}

#[kiss3d::main]
async fn main() {
    let mut window = Window::new("Kiss3d: rectangle").await;
    let _camera = PanZoomCamera2d::new(Vec2::ZERO, 2.0);
    let _scene = SceneNode2d::empty();

    let image_buffer = ASSET_DIR
        .get_file("background_concept_2.png")
        .expect("File should be here");
    let mut texture_manager = TextureManager::new();
    let _image_texture =
        texture_manager.add_image_from_memory(image_buffer.contents(), "background_concept_2.png");

    let mut reliable_client_rpc_sender: Option<ReliableClientRpcSender> = None;
    let mut unreliable_client_rpc_sender: Option<UnreliableClientRpcSender> = None;
    let mut reliable_server_rpc_receiver: Option<ReliableServerRpcReceiver> = None;
    let mut unreliable_server_rpc_receiver: Option<UnreliableServerRpcReceiver> = None;
    let mut connection_finished_receiver: Option<ConnectionFinishedReceiver> = None;

    let mut camera = PanZoomCamera2d::new(Vec2::ZERO, 5.0);
    let mut scene = SceneNode2d::empty();
    let mut local_player = scene.add_rectangle(10.0, 10.0).set_color(RED);
    let mut remote_player = scene.add_rectangle(10.0, 10.0).set_color(BLUE);

    // UI state
    let mut ui_game_state = UiGameState::new();
    let mut is_input_selected = false;

    // timer stuff
    let mut previous_time = OffsetDateTime::now_utc();
    let mut heartbeat_timer = Duration::new(0, 0);
    let max_time_for_heartbeat = Duration::new(1, 0);
    // deltarune runs on 30 TPS
    let mut game_time_step_timer = Duration::new(0, 0);

    // input state
    let mut input = rpc::MoveInputState::default();

    // game state
    let mut game_logic = GameLogic::new();
    let mut next_input_sequence: InputSequence = 1;
    let mut pending_inputs: VecDeque<PendingMoveInput> = VecDeque::new();
    let mut remote_snapshots: VecDeque<TimedRemoteSnapshot> = VecDeque::new();

    // Client config
    let client_config = include_str!("../client_config.toml");
    let client_config: ClientConfig =
        toml::from_str(client_config).expect("Should be able to convert");
    let mut direct_connect_addr = String::new();
    while window.render_2d(&mut scene, &mut camera).await {
        let current_time = OffsetDateTime::now_utc();
        let time_since_last_frame = current_time - previous_time;
        heartbeat_timer += time_since_last_frame;
        if heartbeat_timer >= max_time_for_heartbeat {
            heartbeat_timer = Duration::new(0, 0);
            if let Some(client_rpc_sender) = reliable_client_rpc_sender.clone() {
                let _ = client_rpc_sender.unbounded_send(ReliableRpcClientMessage::Heartbeat);
            }
        }
        previous_time = current_time;
        // set lobby state to empty if connection lost
        if let Some(ref mut connection_finished_receiver) = connection_finished_receiver
            && let Ok(Some(_)) = connection_finished_receiver.try_recv()
        {
            log!("Connection dropped.");
            ui_game_state.reset();
            pending_inputs.clear();
            remote_snapshots.clear();
            next_input_sequence = 1;
            game_logic.setup_game();
        }
        // immediately pool the receiver even if there isn't a value there.
        while let Some(ref mut server_rpc_receiver) = reliable_server_rpc_receiver
            && let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten()
        {
            log!("{rpc_message:?}");
            match rpc_message {
                ReliableRpcServerMessage::Text(_text) => {
                    // TODO: Do something here I guess
                }
                ReliableRpcServerMessage::GameState {
                    state,
                    left_side_score,
                    right_side_score,
                } => {
                    if let Some(previous_state) = ui_game_state.current_game_state.as_ref()
                        && *previous_state == state
                    {
                        continue;
                    }
                    match &state {
                        RPSGameState::StartRound => {
                            // reset all game state on Start Round
                            ui_game_state.remote_left_input = None;
                            ui_game_state.remote_right_input = None;
                            local_player.set_position(Vec2::ZERO);
                            remote_player.set_position(Vec2::ZERO);
                            // remove previous players
                            game_logic.setup_game();
                            pending_inputs.clear();
                            remote_snapshots.clear();
                            next_input_sequence = 1;
                        }
                        RPSGameState::WaitingForLeftInput { right_input } => {
                            ui_game_state.remote_right_input = Some(*right_input);
                        }
                        RPSGameState::WaitingForRightInput { left_input } => {
                            ui_game_state.remote_left_input = Some(*left_input);
                        }
                        RPSGameState::Win {
                            state,
                            left_input,
                            right_input,
                        } => {
                            ui_game_state.win_state = Some(state.clone());
                            ui_game_state.remote_right_input = Some(*right_input);
                            ui_game_state.remote_left_input = Some(*left_input);
                        }
                    }
                    ui_game_state.remote_left_score = left_side_score;
                    ui_game_state.remote_right_score = right_side_score;
                    ui_game_state.current_game_state = Some(state);
                }
                ReliableRpcServerMessage::LobbyInit(side, user_id, lobby_id) => {
                    ui_game_state.user_id = Some(user_id);
                    ui_game_state.lobby_id = Some(lobby_id);
                    ui_game_state.player_side = Some(side);
                    // Ensure local simulation entities exist as soon as we know our side.
                    game_logic.setup_game();
                    pending_inputs.clear();
                    remote_snapshots.clear();
                    next_input_sequence = 1;
                }
                ReliableRpcServerMessage::LobbyState(state) => {
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

        // unreliable message
        while let Some(ref mut server_rpc_receiver) = unreliable_server_rpc_receiver
            && let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten()
        {
            match rpc_message {
                UnreliableRpcServerMessage::MoveGameState(game_state) => {
                    if let Some(local_side) = ui_game_state.player_side {
                        let (local_position, remote_position, acknowledged_sequence) =
                            match local_side {
                                PlayerSide::Left => (
                                    game_state.left_position,
                                    game_state.right_position,
                                    game_state.left_last_processed_input,
                                ),
                                PlayerSide::Right => (
                                    game_state.right_position,
                                    game_state.left_position,
                                    game_state.right_last_processed_input,
                                ),
                            };

                        game_logic.update_position_with_vec(local_side, local_position);

                        while let Some(pending) = pending_inputs.front() {
                            if pending.sequence <= acknowledged_sequence {
                                let _ = pending_inputs.pop_front();
                            } else {
                                break;
                            }
                        }

                        for pending in pending_inputs.iter() {
                            game_logic.update_position_with_input(local_side, &pending.input);
                        }

                        remote_snapshots.push_back(TimedRemoteSnapshot {
                            received_at: current_time,
                            position: remote_position,
                        });
                        while remote_snapshots.len() > MAX_REMOTE_SNAPSHOTS {
                            let _ = remote_snapshots.pop_front();
                        }
                    }
                }
            }
        }

        // the way OS's poll key inputs mean that there's a frame of waiting before sending in the next key input
        // see: https://stereopsis.com/keyrepeat/
        for event in window.events().iter() {
            match event.value {
                WindowEvent::Key(Key::Space, Action::Press, _) => {
                    log!("Space pressed");
                }
                WindowEvent::Key(Key::Left, Action::Press, _) => {
                    input.left = true;
                }
                WindowEvent::Key(Key::Right, Action::Press, _) => {
                    input.right = true;
                }
                WindowEvent::Key(Key::Up, Action::Press, _) => {
                    input.up = true;
                }
                WindowEvent::Key(Key::Down, Action::Press, _) => {
                    input.down = true;
                }
                WindowEvent::Key(Key::Left, Action::Release, _) => {
                    input.left = false;
                }
                WindowEvent::Key(Key::Right, Action::Release, _) => {
                    input.right = false;
                }
                WindowEvent::Key(Key::Up, Action::Release, _) => {
                    input.up = false;
                }
                WindowEvent::Key(Key::Down, Action::Release, _) => {
                    input.down = false;
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

        // run actual game logic if we've hit a tick (drain accumulated frames)
        game_time_step_timer += time_since_last_frame;
        while game_time_step_timer >= GAME_TIME_STEP {
            game_time_step_timer -= GAME_TIME_STEP;
            if let Some(side) = ui_game_state.player_side {
                let sequence = next_input_sequence;
                next_input_sequence = next_input_sequence.wrapping_add(1);

                pending_inputs.push_back(PendingMoveInput { input, sequence });
                while pending_inputs.len() > MAX_PENDING_INPUTS {
                    let _ = pending_inputs.pop_front();
                }

                game_logic.update_position_with_input(side, &input);
                if let Some(rpc_sender) = unreliable_client_rpc_sender.as_ref() {
                    let _ = rpc_sender.unbounded_send(UnreliableRpcClientMessage::MoveInput {input, sequence});
                }
            }
        }

        // make remote player whatever the other side is
        if let Some(side) = ui_game_state.player_side {
            let remote_side = match side {
                PlayerSide::Left => PlayerSide::Right,
                PlayerSide::Right => PlayerSide::Left,
            };
            let target_time = current_time - REMOTE_INTERPOLATION_DELAY;
            if let Some(interpolated_position) =
                interpolate_remote_position(&remote_snapshots, target_time)
            {
                game_logic.update_position_with_vec(remote_side, interpolated_position);
            }
        }

        let render_state = game_logic.get_state_to_send_to_client();
        match ui_game_state.player_side {
            Some(PlayerSide::Left) => {
                local_player.set_position(render_state.left_position);
                remote_player.set_position(render_state.right_position);
            }
            Some(PlayerSide::Right) => {
                local_player.set_position(render_state.right_position);
                remote_player.set_position(render_state.left_position);
            }
            None => {
                local_player.set_position(Vec2::ZERO);
                remote_player.set_position(Vec2::ZERO);
            }
        }

        // Draw UI
        window.draw_ui(|ctx| {
            egui::Window::new("Kiss3d egui Example")
                .default_width(300.0)
                .show(ctx, |ui| {
                    ui.label(format!("Current Frame Time {}", time_since_last_frame));
                    ui.label(format!(
                        "Current player position {}",
                        local_player.position()
                    ));
                    ui.label(format!("{ui_game_state:#?}"));

                    ui.separator();

                    match ui_game_state.lobby_state {
                        LobbyState::Empty => {
                            ui.label("Server List");
                            for server_address in &client_config.servers {
                                if ui.button(server_address).clicked() {
                                    // reset the connection
                                    let parts =
                                        get_connection_receivers(server_address.to_string());
                                    reliable_client_rpc_sender = Some(parts.0);
                                    unreliable_client_rpc_sender = Some(parts.1);
                                    reliable_server_rpc_receiver = Some(parts.2);
                                    unreliable_server_rpc_receiver = Some(parts.3);
                                    connection_finished_receiver = Some(parts.4);
                                    let _ = reliable_client_rpc_sender
                                        .clone()
                                        .expect("should be set")
                                        .unbounded_send(ReliableRpcClientMessage::JoinLobby);
                                }
                            }
                            let response =
                                ui.add(egui::TextEdit::singleline(&mut direct_connect_addr));
                            if response.changed() {
                                log!("Edit response {}", &direct_connect_addr);
                            }
                            if ui.button("Direct Connect").clicked() {
                                // reset the connection
                                let parts =
                                    get_connection_receivers(direct_connect_addr.to_string());
                                reliable_client_rpc_sender = Some(parts.0);
                                unreliable_client_rpc_sender = Some(parts.1);
                                reliable_server_rpc_receiver = Some(parts.2);
                                unreliable_server_rpc_receiver = Some(parts.3);
                                connection_finished_receiver = Some(parts.4);
                                let _ = reliable_client_rpc_sender
                                    .clone()
                                    .expect("should be set")
                                    .unbounded_send(ReliableRpcClientMessage::JoinLobby);
                            }
                        }
                        LobbyState::Waiting => {}
                        LobbyState::Running => {
                            let client_rpc_sender = reliable_client_rpc_sender
                                .clone()
                                .expect("should be setup by this point since the lobby is running");
                            if let Some(game_state) = &ui_game_state.current_game_state
                                && let Some(side) = ui_game_state.player_side
                            {
                                let round_start = matches!(game_state, RPSGameState::StartRound);
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
                                            ReliableRpcClientMessage::TurnInput(TurnInput::Rock),
                                        );
                                    }
                                    if ui.button("Paper").clicked() {
                                        let _ = client_rpc_sender.unbounded_send(
                                            ReliableRpcClientMessage::TurnInput(TurnInput::Paper)
                                        );
                                    }
                                    if ui.button("Scissors").clicked() {
                                        let _ = client_rpc_sender.unbounded_send(
                                            ReliableRpcClientMessage::TurnInput(TurnInput::Scissors)
                                        );
                                    }
                                }
                            }
                        }
                        LobbyState::Finished => {
                            let client_rpc_sender = reliable_client_rpc_sender.clone().expect(
                                "should be setup by this point since the lobby is finished",
                            );
                            if !is_input_selected {
                                if ui.button("Yes").clicked() {
                                    let _ = client_rpc_sender.unbounded_send(
                                        ReliableRpcClientMessage::ContinueRound(YesOrNo::Yes),
                                    );
                                    is_input_selected = true;
                                }
                                if ui.button("No").clicked() {
                                    let _ = client_rpc_sender.unbounded_send(
                                        ReliableRpcClientMessage::ContinueRound(YesOrNo::No),
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
