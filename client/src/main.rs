use crate::connection::{
    ConnectionFinishedReceiver, ReliableClientRpcSender, ReliableServerRpcReceiver,
    UnreliableClientRpcSender, UnreliableServerRpcReceiver,
};
use futures_channel::oneshot;
use futures_util::{FutureExt, StreamExt};
use include_dir::{Dir, include_dir};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use kiss3d::{egui, prelude::*};
use ringbuffer::{AllocRingBuffer, RingBuffer};
use rpc::{
    GAME_TIME_STEP, GameLogic, InputSequence, LobbyId, LobbyState, MoveGameState, PlayerSide,
    RPSGameState, RPSWinState, ReliableRpcClientMessage, ReliableRpcServerMessage, ScoreSize,
    TurnInput, UnreliableRpcClientMessage, UnreliableRpcServerMessage, UserId, YesOrNo,
    encode_message,
};
use std::collections::{BTreeMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
#[cfg(not(target_arch = "wasm32"))]
use std::thread;
use time::{Duration, OffsetDateTime};

mod connection;

static ASSET_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/assets");

fn reset_connection_and_join_lobby(
    server_address: String,
    reliable_client_rpc_sender: &mut Option<ReliableClientRpcSender>,
    unreliable_client_rpc_sender: &mut Option<UnreliableClientRpcSender>,
    reliable_server_rpc_receiver: &mut Option<ReliableServerRpcReceiver>,
    unreliable_server_rpc_receiver: &mut Option<UnreliableServerRpcReceiver>,
    connection_finished_receiver: &mut Option<ConnectionFinishedReceiver>,
) {
    let (
        new_reliable_client_rpc_sender,
        reliable_client_rpc_receiver,
        new_unreliable_client_rpc_sender,
        unreliable_client_rpc_receiver,
        reliable_server_rpc_sender,
        new_reliable_server_rpc_receiver,
        unreliable_server_rpc_sender,
        new_unreliable_server_rpc_receiver,
    ) = connection::make_channels();
    let (connection_finished_sender, new_connection_finished_receiver) = oneshot::channel::<()>();

    #[cfg(target_arch = "wasm32")]
    {
        spawn_local(async move {
            crate::connection::connect_to_webtransport_server_wasm(
                server_address,
                reliable_client_rpc_receiver,
                unreliable_client_rpc_receiver,
                reliable_server_rpc_sender,
                unreliable_server_rpc_sender,
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

    *reliable_client_rpc_sender = Some(new_reliable_client_rpc_sender);
    *unreliable_client_rpc_sender = Some(new_unreliable_client_rpc_sender);
    *reliable_server_rpc_receiver = Some(new_reliable_server_rpc_receiver);
    *unreliable_server_rpc_receiver = Some(new_unreliable_server_rpc_receiver);
    *connection_finished_receiver = Some(new_connection_finished_receiver);

    let _ = reliable_client_rpc_sender
        .as_ref()
        .expect("should be set")
        .unbounded_send(ReliableRpcClientMessage::JoinServer);
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

const MAX_PENDING_INPUTS: usize = 256;
const MAX_REMOTE_SNAPSHOTS: InputSequence = 64;
// this should be based on our delay to the server
const REMOTE_INTERPOLATION_DELAY: Duration = Duration::milliseconds(330);
// TODO: make a proper calculation based on acknowledgement + actual network delay
// We assume both client and server run on 30 fps
const REMOTE_ACK_DELAY_FRAMES: InputSequence = 11;
const REMOTE_SNAPSHOT_HISTORY: Duration = Duration::milliseconds(500);

fn hash_move_state(state: &MoveGameState) -> u64 {
    let bytes = encode_message(state).expect("Should be able to serialize");
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn prune_acknowledged_inputs(
    pending_inputs: &mut VecDeque<PendingMoveInput>,
    acknowledged_sequence: InputSequence,
) {
    pending_inputs.retain(|input| input.sequence > acknowledged_sequence);
    while pending_inputs.len() > MAX_PENDING_INPUTS {
        pending_inputs.pop_front();
    }
}

// client side replication taken from: https://www.gabrielgambetta.com/client-side-prediction-live-demo.html
// (right click and inspect webpage to see the actual javascript)
fn interpolate_remote_position(
    snapshots: &mut BTreeMap<InputSequence, Vec2>,
    render_delay_ticks: InputSequence,
) -> Option<Vec2> {
    let latest_known_tick = *snapshots.keys().next_back()?;
    let target_time = latest_known_tick.saturating_sub(render_delay_ticks);

    let oldest_allowed = latest_known_tick.saturating_sub(MAX_REMOTE_SNAPSHOTS);
    snapshots.retain(|k, _| *k >= oldest_allowed);

    let before = snapshots.range(..=target_time).next_back();
    let after = snapshots.range(target_time..).next();

    match (before, after) {
        (Some((t0, p0)), Some((t1, p1))) => {
            if t0 == t1 {
                Some(*p0)
            } else {
                let frac = (target_time - t0) as f32 / (t1 - t0) as f32;
                Some(*p0 + (*p1 - *p0) * frac) // lerp by actual gap, not flat average
            }
        }
        (Some((_, p)), None) => Some(*p),
        (None, Some((_, p))) => Some(*p),
        (None, None) => None,
    }
}
struct ClientGameLogic {
    is_input_selected: bool,
    // Ensure move prediction starts from a clean baseline when a round begins.
    pending_inputs: VecDeque<PendingMoveInput>,
    remote_snapshots: BTreeMap<InputSequence, Vec2>,
    next_input_sequence: InputSequence,
    last_acknowledged_sequence: InputSequence,
    game_logic: GameLogic,
}

impl ClientGameLogic {
    fn new() -> Self {
        Self {
            is_input_selected: false,
            pending_inputs: VecDeque::new(),
            remote_snapshots: BTreeMap::new(),
            next_input_sequence: 0,
            last_acknowledged_sequence: 0,
            game_logic: GameLogic::new(),
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[kiss3d::main]
async fn main() {
    #[cfg(target_arch = "wasm32")]
    {
        console_error_panic_hook::set_once();
    }
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
    // deltarune runs on 30 TPS
    let mut game_time_step_timer = Duration::new(0, 0);
    // max time accumulated between frames should be 0.25 seconds
    let max_time_between_frames = Duration::milliseconds(250);

    // input state
    let mut input = rpc::MoveInputState::default();

    // game state
    let mut game_logic = ClientGameLogic::new();

    let mut remote_state_hash = 0;
    let mut predicted_state: MoveGameState;

    // Client config
    let client_config = include_str!("../client_config.toml");
    let client_config: ClientConfig =
        toml::from_str(client_config).expect("Should be able to convert");
    let mut direct_connect_addr = String::new();
    while window.render_2d(&mut scene, &mut camera).await {
        let current_time = OffsetDateTime::now_utc();
        let time_since_last_frame = {
            let frame_time = current_time - previous_time;
            if frame_time > max_time_between_frames {
                max_time_between_frames
            } else {
                frame_time
            }
        };
        previous_time = current_time;
        // set lobby state to empty if connection lost
        if let Some(ref mut connection_finished_receiver) = connection_finished_receiver
            && let Ok(Some(_)) = connection_finished_receiver.try_recv()
        {
            log!("Connection dropped.");
        }
        // immediately pool the receiver even if there isn't a value there.
        while let Some(ref mut server_rpc_receiver) = reliable_server_rpc_receiver
            && let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten()
        {
            //log!("{rpc_message:?}");
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
                            // Ensure local simulation entities exist as soon as we know our side.
                            local_player.set_position(Vec2::ZERO);
                            remote_player.set_position(Vec2::ZERO);
                            game_logic.reset();
                            local_player.set_position(Vec2::ZERO);
                            remote_player.set_position(Vec2::ZERO);
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
                ReliableRpcServerMessage::ConnectionAuthentication(oauth_url) => {
                    if webbrowser::open(&oauth_url.0).is_err() {
                        log!("{:?}", oauth_url);
                    }
                }
                ReliableRpcServerMessage::ConnectionInit(user_id, init_message) => {
                    ui_game_state.reset();
                    ui_game_state.user_id = Some(user_id);
                    log!("Connection init message: {init_message:?}");
                }
                ReliableRpcServerMessage::LobbyInit(side, lobby_id) => {
                    ui_game_state.lobby_id = Some(lobby_id);
                    ui_game_state.player_side = Some(side);
                }
                ReliableRpcServerMessage::LobbyState(state) => {
                    // unless lobby state is finished, we really shouldn't have a win state
                    // also don't updadte if the same lobby state has already been set
                    if ui_game_state.lobby_state != state {
                        match state {
                            LobbyState::Finished => {}
                            LobbyState::Running => {
                                ui_game_state.win_state = None;
                                is_input_selected = false;
                                // Ensure move prediction starts from a clean baseline when a round begins.
                                game_logic.reset();
                            }
                            LobbyState::Empty => {}
                            LobbyState::Waiting => {}
                        }
                        ui_game_state.lobby_state = state;
                    }
                }
            }
        }

        // unreliable message
        while let Some(ref mut server_rpc_receiver) = unreliable_server_rpc_receiver
            && let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten()
        {
            match rpc_message {
                UnreliableRpcServerMessage::GameState {
                    state,
                    acknowledged_sequence,
                    tick,
                } => {
                    // hash state
                    remote_state_hash = hash_move_state(&state);

                    if let Some(local_side) = ui_game_state.player_side {
                        let (local_position, remote_position) = match local_side {
                            PlayerSide::Left => (state.left_position, state.right_position),
                            PlayerSide::Right => (state.right_position, state.left_position),
                        };

                        if acknowledged_sequence < game_logic.last_acknowledged_sequence {
                            continue;
                        }
                        game_logic.last_acknowledged_sequence = acknowledged_sequence;

                        prune_acknowledged_inputs(
                            &mut game_logic.pending_inputs,
                            acknowledged_sequence,
                        );

                        game_logic
                            .game_logic
                            .update_position_with_vec(local_side, local_position);

                        for pending in game_logic.pending_inputs.iter() {
                            game_logic
                                .game_logic
                                .update_position_with_input(local_side, &pending.input);
                        }

                        game_logic.remote_snapshots.insert(tick, remote_position);
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
            // make sure LobbyState is running, because building up inputs before network syncing is bad
            if let Some(side) = ui_game_state.player_side
                && ui_game_state.lobby_state == LobbyState::Running
            {
                let sequence = game_logic.next_input_sequence;
                game_logic.next_input_sequence = game_logic.next_input_sequence.wrapping_add(1);

                game_logic
                    .pending_inputs
                    .push_back(PendingMoveInput { input, sequence });

                game_logic
                    .game_logic
                    .update_position_with_input(side, &input);
                if let Some(rpc_sender) = unreliable_client_rpc_sender.as_ref() {
                    let _ = rpc_sender
                        .unbounded_send(UnreliableRpcClientMessage::Input { input, sequence });
                }
            }
            game_logic.game_logic.step_physics();
        }

        // Hash the simulation state before applying any render-time interpolation.
        // predicted state is just for rendering, it can't be real because the server has the real state.
        predicted_state = game_logic.game_logic.get_state_to_send_to_client();
        if let Some(side) = ui_game_state.player_side {
            if let Some(interpolated_position) = interpolate_remote_position(
                &mut game_logic.remote_snapshots,
                REMOTE_ACK_DELAY_FRAMES,
            ) {
                match side {
                    PlayerSide::Left => {
                        predicted_state.right_position = interpolated_position;
                    }
                    PlayerSide::Right => {
                        predicted_state.left_position = interpolated_position;
                    }
                }
            }
        }
        let predicted_state_hash = hash_move_state(&predicted_state);
        match ui_game_state.player_side {
            Some(PlayerSide::Left) => {
                local_player.set_position(predicted_state.left_position);
                remote_player.set_position(predicted_state.right_position);
            }
            Some(PlayerSide::Right) => {
                local_player.set_position(predicted_state.right_position);
                remote_player.set_position(predicted_state.left_position);
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
                    ui.label(format!("Current remote state hash: {}", remote_state_hash));
                    ui.label(format!(
                        "Current predicted state hash: {}",
                        predicted_state_hash
                    ));
                    ui.label(format!("{ui_game_state:#?}"));

                    ui.separator();

                    match ui_game_state.lobby_state {
                        LobbyState::Empty => {
                            ui.label("Server List");
                            for server_address in &client_config.servers {
                                if ui.button(server_address).clicked() {
                                    reset_connection_and_join_lobby(
                                        server_address.to_string(),
                                        &mut reliable_client_rpc_sender,
                                        &mut unreliable_client_rpc_sender,
                                        &mut reliable_server_rpc_receiver,
                                        &mut unreliable_server_rpc_receiver,
                                        &mut connection_finished_receiver,
                                    );
                                }
                            }
                            let response =
                                ui.add(egui::TextEdit::singleline(&mut direct_connect_addr));
                            if response.changed() {
                                log!("Edit response {}", &direct_connect_addr);
                            }
                            if ui.button("Direct Connect").clicked() {
                                reset_connection_and_join_lobby(
                                    direct_connect_addr.to_string(),
                                    &mut reliable_client_rpc_sender,
                                    &mut unreliable_client_rpc_sender,
                                    &mut reliable_server_rpc_receiver,
                                    &mut unreliable_server_rpc_receiver,
                                    &mut connection_finished_receiver,
                                );
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
                                            ReliableRpcClientMessage::TurnInput(TurnInput::Paper),
                                        );
                                    }
                                    if ui.button("Scissors").clicked() {
                                        let _ = client_rpc_sender.unbounded_send(
                                            ReliableRpcClientMessage::TurnInput(
                                                TurnInput::Scissors,
                                            ),
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
