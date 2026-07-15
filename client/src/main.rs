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
use shared::game::{
    BattleGameState, BattleStats, BattleWinner, ENEMY_POSITION, GAME_TIME_STEP, GameLogic,
    MoveGameState, PROJECTILE_SIZE, PlayerSide, TurnAction, get_current_bullet_count,
    get_data_for_projectile_from_index,
};
use shared::rpc::{
    InputSequence, LobbyId, LobbyState, PendingMoveInput, ReliableRpcClientMessage,
    ReliableRpcServerMessage, UnreliableRpcClientMessage, UnreliableRpcServerMessage, UserId,
    YesOrNo, encode_message,
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
enum UiState {
    NotConnected,
    Authenticating,
    Connected,
    LobbyWaiting,
    LobbyRunning,
    LobbyFinished,
}

#[derive(Debug)]
struct UiGameState {
    ui_state: UiState,
    lobby_state: LobbyState,
    lobby_id: Option<LobbyId>,
    current_game_state: Option<BattleGameState>,
    user_id: Option<UserId>,
}

impl UiGameState {
    fn new() -> Self {
        Self {
            ui_state: UiState::NotConnected,
            lobby_state: LobbyState::Empty,
            lobby_id: None,
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

const MAX_PENDING_INPUTS: usize = 256;
const MAX_INPUTS_TO_SEND: usize = 8;
const MAX_REMOTE_SNAPSHOTS: InputSequence = 64;
// We assume both client and server run on 30 fps
const DEFAULT_REMOTE_INTERPOLATION_DELAY_FRAMES: InputSequence = 2;
const RTT_EWMA_ALPHA: f32 = 0.125;
const RTT_EWMA_BETA: f32 = 0.10;

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

struct ClientGameLogic {
    // Ensure move prediction starts from a clean baseline when a round begins.
    pending_inputs: VecDeque<PendingMoveInput>,
    latest_remote_snapshot: Option<(InputSequence, MoveGameState)>,
    current_input_sequence: InputSequence,
    last_acknowledged_sequence: InputSequence,
    estimated_rtt: f32,
    jitter_rtt: f32,
    game_logic: GameLogic,
    player_side: Option<PlayerSide>,
    // How many bullets of the current dodge pattern we've spawned locally.
    // The pattern itself is deterministic and scheduled on a shared unix
    // timestamp, so this count is all the state prediction needs.
    amount_of_spawned_bullets: u32,
}

impl ClientGameLogic {
    fn new() -> Self {
        Self {
            pending_inputs: VecDeque::new(),
            latest_remote_snapshot: None,
            current_input_sequence: 0,
            last_acknowledged_sequence: 0,
            estimated_rtt: 0.0,
            jitter_rtt: 0.0,
            game_logic: GameLogic::new(),
            player_side: None,
            amount_of_spawned_bullets: 0,
        }
    }

    fn reset(&mut self) {
        // We keep the player side since this should be overridden when we (re)join a lobby
        let player_side = self.player_side;
        *self = Self::new();
        self.player_side = player_side;
    }

    fn update_snapshot(
        &mut self,
        acknowledged_sequence: InputSequence,
        remote_snapshot: MoveGameState,
    ) {
        let (remote_side, remote_position) = match self.player_side {
            Some(PlayerSide::Left) => (PlayerSide::Right, remote_snapshot.right_position),
            Some(PlayerSide::Right) => (PlayerSide::Left, remote_snapshot.left_position),
            None => return,
        };
        self.game_logic
            .update_position_with_vec(remote_side, remote_position);
        self.latest_remote_snapshot = Some((acknowledged_sequence, remote_snapshot));
    }

    // client side replication taken from: https://www.gabrielgambetta.com/client-side-prediction-live-demo.html
    // (right click and inspect webpage to see the actual javascript)
    // since this isn't PvP, we don't actually care all that much about hitting another player, we just care about the other enemies
    // Lerp between the last authoritative snapshot (t0 = its acknowledged sequence) and the current predicted state (t1 = current_input_sequence),
    fn interpolate_remote_position(&self, predicted_state: &mut MoveGameState) {
        let side = match self.player_side {
            Some(side) => side,
            None => return,
        };
        let (remote_sequence, snapshot) = match self.latest_remote_snapshot.as_ref() {
            Some(tuple) => tuple,
            None => return,
        };

        // give the position of the opposite side of the local player
        let remote_position = |state: &MoveGameState| match side {
            PlayerSide::Left => state.right_position,
            PlayerSide::Right => state.left_position,
        };
        let snapshot_position = remote_position(snapshot);
        let predicted_position = remote_position(predicted_state);

        // Sequences wrap, so measure the gap with wrapping_sub.
        let ticks_between_remote_and_local =
            self.current_input_sequence.wrapping_sub(*remote_sequence);
        // TODO: For now, lets just lerp by 0.5
        let lerped_position = if ticks_between_remote_and_local == 0 {
            snapshot_position
        } else {
            snapshot_position.lerp(predicted_position, 0.5)
        };
        match side {
            PlayerSide::Left => {
                predicted_state.right_position = lerped_position;
            }
            PlayerSide::Right => {
                predicted_state.left_position = lerped_position;
            }
        }
    }
}

fn draw_battle_stats(ui: &mut egui::Ui, stats: &BattleStats, side: PlayerSide) {
    let (own_health, partner_health) = match side {
        PlayerSide::Left => (stats.left_health, stats.right_health),
        PlayerSide::Right => (stats.right_health, stats.left_health),
    };
    ui.label(format!("Your HP: {own_health}"));
    ui.label(format!("Partner HP: {partner_health}"));
    ui.label(format!("Enemy HP: {}", stats.enemy_health));
}

fn win_message(winner: &BattleWinner) -> &'static str {
    match winner {
        BattleWinner::Players => "The enemy has fallen. You win!",
        BattleWinner::Enemy => "Both of you have fallen... the enemy wins.",
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
    let mut enemy_node = scene.add_rectangle(30.0, 30.0).set_color(GREEN);
    enemy_node.set_position(ENEMY_POSITION);

    // Fixed pool of nodes for predicted bullets; unused ones are parked far
    // off-screen since kiss3d nodes are cheapest to keep alive and move.
    let offscreen = Vec2::new(1_000_000.0, 1_000_000.0);
    const PROJECTILE_NODE_POOL: usize = 64;
    let mut projectile_nodes: Vec<_> = (0..PROJECTILE_NODE_POOL)
        .map(|_| {
            let mut node = scene
                .add_rectangle(PROJECTILE_SIZE, PROJECTILE_SIZE)
                .set_color(WHITE);
            node.set_position(offscreen);
            node
        })
        .collect();

    // UI state
    let mut ui_game_state = UiGameState::new();
    // timer stuff
    let mut previous_time = OffsetDateTime::now_utc();
    // deltarune runs on 30 TPS
    let mut game_time_step_timer = Duration::new(0, 0);
    // max time accumulated between frames should be 0.25 seconds
    let max_time_between_frames = Duration::milliseconds(250);

    // input state
    let mut input = shared::game::MoveInputState::default();

    // game state
    let mut game_logic = ClientGameLogic::new();

    let mut remote_state_hash = 0;

    // TODO: dynamically set and remove game rectangles, but for now we can just assume there's only a set number right now
    let rectangles = game_logic.game_logic.get_obstacle_rectangles();
    for rect in rectangles {
        let mut scene_rect = scene
            .add_rectangle(rect.width, rect.height)
            .set_color(YELLOW);
        // Draw at the collider's actual center (in pixel space) so the visible
        // box lines up with where collision really happens.
        scene_rect.set_position(rect.get_pixel_position());
    }

    // Client config
    let client_config = include_str!("../client_config.toml");
    let client_config: ClientConfig =
        toml::from_str(client_config).expect("Should be able to convert");
    let mut direct_connect_addr = String::new();
    while window.render_2d(&mut scene, &mut camera).await {
        let current_time = OffsetDateTime::now_utc();
        // current time in milliseconds
        let current_time_ms = (current_time.unix_timestamp_nanos() / 1_000_000) as i64;
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
            ui_game_state.ui_state = UiState::NotConnected;
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
                ReliableRpcServerMessage::GameState(state) => {
                    if let Some(previous_state) = ui_game_state.current_game_state.as_ref()
                        && *previous_state == state
                    {
                        continue;
                    }
                    // Leaving the dodge segment (next choice phase, or the battle ending)
                    // clears any predicted bullets still alive.
                    match &state {
                        BattleGameState::ChoosingActions { .. } | BattleGameState::Win { .. } => {
                            game_logic.game_logic.clear_projectiles();
                            game_logic.amount_of_spawned_bullets = 0;
                        }
                        BattleGameState::Dodging { .. } => {}
                    }
                    ui_game_state.current_game_state = Some(state);
                }
                ReliableRpcServerMessage::ConnectionAuthentication(oauth_url) => {
                    if webbrowser::open(&oauth_url.0).is_err() {
                        log!("{:?}", oauth_url);
                    }
                    ui_game_state.ui_state = UiState::Authenticating;
                }
                ReliableRpcServerMessage::ConnectionInit(user_id, init_message) => {
                    ui_game_state.reset();
                    ui_game_state.user_id = Some(user_id);
                    log!("Connection init message: {init_message:?}");
                    ui_game_state.ui_state = UiState::Connected;
                }
                ReliableRpcServerMessage::LobbyInit(side, lobby_id) => {
                    ui_game_state.lobby_id = Some(lobby_id);
                    game_logic.player_side = Some(side);
                    ui_game_state.ui_state = UiState::LobbyWaiting;
                }
                ReliableRpcServerMessage::LobbyState(state) => {
                    // unless lobby state is finished, we really shouldn't have a win state
                    // also don't update if the same lobby state has already been set
                    if ui_game_state.lobby_state != state {
                        match state {
                            LobbyState::Finished => {
                                ui_game_state.ui_state = UiState::LobbyFinished;
                            }
                            LobbyState::Running => {
                                ui_game_state.ui_state = UiState::LobbyRunning;
                                // Ensure move prediction starts from a clean baseline when a round begins.
                                game_logic.reset();
                            }
                            LobbyState::Empty => {}
                            LobbyState::Waiting => {
                                ui_game_state.ui_state = UiState::LobbyWaiting;
                            }
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
                    echo_client_time_ms,
                } => {
                    // hash state
                    remote_state_hash = hash_move_state(&state);

                    if let Some(local_side) = game_logic.player_side {
                        let (local_position, remote_position) = match local_side {
                            PlayerSide::Left => (state.left_position, state.right_position),
                            PlayerSide::Right => (state.right_position, state.left_position),
                        };

                        if acknowledged_sequence < game_logic.last_acknowledged_sequence {
                            continue;
                        }

                        let game_time_step = GAME_TIME_STEP.as_secs_f32();
                        // RTT estimate from wall-clock time: echo_client_time_ms is the
                        // client_send_time_ms of the most recent input packet the server had
                        // received, echoed back verbatim.
                        // Comparing it against "now" gives a real send-to-receive-echo latency sample
                        // A sentinel of 0 means the server hasn't received any input from us
                        // yet, so there's nothing valid to sample.
                        // RTT Calculation is based off of this:
                        // https://www.scs.stanford.edu/08sp-cs144/notes/l6.pdf
                        // specifically slide 14
                        if echo_client_time_ms > 0 {
                            let sample_rtt =
                                (current_time_ms - echo_client_time_ms).max(0) as f32 / 1000.0;
                            game_logic.estimated_rtt = ((1. - RTT_EWMA_ALPHA)
                                * game_logic.estimated_rtt)
                                + (RTT_EWMA_ALPHA * sample_rtt);
                            let abs_error = (game_logic.estimated_rtt - sample_rtt).abs();
                            // jitter is deviation of the estimated RTT
                            game_logic.jitter_rtt = ((1. - RTT_EWMA_BETA) * game_logic.jitter_rtt)
                                + (RTT_EWMA_BETA * abs_error);
                        }

                        game_logic.last_acknowledged_sequence = acknowledged_sequence;

                        prune_acknowledged_inputs(
                            &mut game_logic.pending_inputs,
                            acknowledged_sequence,
                        );

                        // update local position based on what the server sends up
                        // server is the authority, we can't override it or else desyncs will happen.
                        game_logic
                            .game_logic
                            .update_position_with_vec(local_side, local_position);

                        // however, we can use the inputs we stored to predict what the server SHOULD be.
                        for pending in game_logic.pending_inputs.iter() {
                            game_logic
                                .game_logic
                                .update_position_with_input(local_side, &pending.input);
                        }
                        // we want to give each remote snapshot a timestamp from the server itself
                        game_logic.update_snapshot(acknowledged_sequence, state);
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
            // TODO: for now we make all game logic only execute while we're inside a lobby
            // ideally we would LIKE to be able to do game logic when we're not connected to a server
            // for training mode or whatever.
            // TODO: Figure out how a "training mode" would work
            if let Some(side) = game_logic.player_side
                && ui_game_state.lobby_state == LobbyState::Running
            {
                let sequence = game_logic.current_input_sequence;
                game_logic.current_input_sequence =
                    game_logic.current_input_sequence.wrapping_add(1);

                game_logic
                    .pending_inputs
                    .push_back(PendingMoveInput { input, sequence });

                game_logic
                    .game_logic
                    .update_position_with_input(side, &input);
                // send all inputs that haven't been acknowledged yet
                // (there is a max amount of inputs we can send before the message gets too big)
                if let Some(rpc_sender) = unreliable_client_rpc_sender.as_ref() {
                    // only resend the newest MAX_INPUTS_TO_SEND unacked inputs per packet so
                    // message size stays bounded regardless of how deep the backlog gets
                    let len = game_logic.pending_inputs.len();
                    let start = len.saturating_sub(MAX_INPUTS_TO_SEND);

                    let pending_inputs_to_send: VecDeque<PendingMoveInput> =
                        game_logic.pending_inputs.range(start..).cloned().collect();
                    let _ = rpc_sender.unbounded_send(UnreliableRpcClientMessage::Input {
                        pending_inputs: pending_inputs_to_send,
                        client_send_time_ms: current_time_ms,
                    });
                }
                // Dodge phase prediction: the server told us when the pattern
                // starts (a shared unix timestamp), so spawn the same
                // deterministic bullets it does, on our own clock.
                if let Some(BattleGameState::Dodging {
                    pattern_start_time_ms,
                    ..
                }) = ui_game_state.current_game_state.as_ref()
                {
                    let scheduled_bullets =
                        get_current_bullet_count(*pattern_start_time_ms, current_time_ms);
                    while game_logic.amount_of_spawned_bullets < scheduled_bullets {
                        let (position, velocity) = get_data_for_projectile_from_index(
                            game_logic.amount_of_spawned_bullets,
                        );
                        game_logic.game_logic.spawn_projectile(position, velocity);
                        game_logic.amount_of_spawned_bullets += 1;
                    }
                }

                game_logic.game_logic.step_physics();

                // Mirror the server's despawn-on-contact so predicted bullets
                // visually disappear when they hit someone; the damage itself is
                // server-authoritative and arrives through the battle stats.
                if let Some(BattleGameState::Dodging { stats, .. }) =
                    ui_game_state.current_game_state.as_ref()
                {
                    let _ = game_logic
                        .game_logic
                        .detect_projectile_hits(stats.left_health > 0, stats.right_health > 0);
                }
            }
        }

        // Hash the simulation state before applying any render-time interpolation.
        // predicted state is just for rendering, it can't be real because the server has the real state.
        let mut predicted_state = game_logic.game_logic.get_state_to_send_to_client();
        let pre_predicted_state_hash = hash_move_state(&predicted_state);
        // actually do the prediction here, which will modify predicted state to fit the server
        game_logic.interpolate_remote_position(&mut predicted_state);
        let rendered_state_hash = hash_move_state(&predicted_state);
        let ms_per_tick = GAME_TIME_STEP.as_secs_f32() * 1000.0;
        let estimated_rtt_ms = game_logic.estimated_rtt * 1000.0;
        let jitter_rtt_ms = game_logic.jitter_rtt * 1000.0;
        match game_logic.player_side {
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

        // Predicted bullets: pool nodes take the simulated positions, the
        // rest wait off-screen.
        let projectile_positions = game_logic.game_logic.get_projectile_positions();
        for (i, node) in projectile_nodes.iter_mut().enumerate() {
            match projectile_positions.get(i) {
                Some(position) => {
                    node.set_position(*position);
                }
                None => {
                    node.set_position(offscreen);
                }
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
                        "Reconciled predicted state hash (pre-render interpolation): {}",
                        pre_predicted_state_hash
                    ));
                    ui.label(format!(
                        "Rendered predicted state hash (post-interpolation): {}",
                        rendered_state_hash
                    ));
                    ui.separator();
                    ui.label(format!("RTT estimate: ({:.1} ms)", estimated_rtt_ms));
                    ui.label(format!("Ping estimate: ({:.1} ms)", estimated_rtt_ms / 2.));
                    ui.label(format!("RTT jitter estimate: ({:.1} ms)", jitter_rtt_ms));
                    //ui.label(format!("{ui_game_state:#?}"));

                    ui.separator();

                    match ui_game_state.ui_state {
                        UiState::NotConnected => {
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
                        UiState::Authenticating => {
                            ui.label("Waiting for server authentication (please click accept on itch.io in your browser)");
                        }
                        UiState::Connected => {
                            ui.label("Successfully connected and authenticated to the server!");
                        }
                        UiState::LobbyWaiting => {
                            ui.label("Waiting for lobby to fill up...");
                        }
                        UiState::LobbyRunning => {
                            let client_rpc_sender = reliable_client_rpc_sender
                                .clone()
                                .expect("should be setup by this point since the lobby is running");
                            if let Some(game_state) = &ui_game_state.current_game_state
                                && let Some(side) = game_logic.player_side
                            {
                                let stats = match game_state {
                                    BattleGameState::ChoosingActions { stats, .. }
                                    | BattleGameState::Dodging { stats, .. }
                                    | BattleGameState::Win { stats, .. } => stats,
                                };
                                draw_battle_stats(ui, stats, side);
                                ui.separator();

                                match game_state {
                                    BattleGameState::ChoosingActions {
                                        left_ready,
                                        right_ready,
                                        ..
                                    } => {
                                        let we_are_ready = match side {
                                            PlayerSide::Left => *left_ready,
                                            PlayerSide::Right => *right_ready,
                                        };
                                        if stats.health_for(side) == 0 {
                                            ui.label(
                                                "You are down... your partner fights alone.",
                                            );
                                        } else if we_are_ready {
                                            ui.label("Waiting for your partner's choice...");
                                        } else {
                                            ui.label("Choose your action:");
                                            if ui.button("Attack").clicked() {
                                                let _ = client_rpc_sender.unbounded_send(
                                                    ReliableRpcClientMessage::TurnAction(
                                                        TurnAction::Attack,
                                                    ),
                                                );
                                            }
                                            if ui.button("Defend").clicked() {
                                                let _ = client_rpc_sender.unbounded_send(
                                                    ReliableRpcClientMessage::TurnAction(
                                                        TurnAction::Defend,
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                    BattleGameState::Dodging {
                                        ticks_remaining, ..
                                    } => {
                                        let seconds_remaining = *ticks_remaining as f32
                                            * GAME_TIME_STEP.as_secs_f32();
                                        ui.label(format!(
                                            "DODGE! {seconds_remaining:.1}s remaining"
                                        ));
                                    }
                                    BattleGameState::Win { winner, .. } => {
                                        ui.label(win_message(winner));
                                    }
                                }
                            }
                        }
                        UiState::LobbyFinished => {
                            let client_rpc_sender = reliable_client_rpc_sender.clone().expect(
                                "should be setup by this point since the lobby is finished",
                            );
                            if let Some(BattleGameState::Win { winner, stats }) =
                                &ui_game_state.current_game_state
                            {
                                ui.label(win_message(winner));
                                if let Some(side) = game_logic.player_side {
                                    draw_battle_stats(ui, stats, side);
                                }
                                ui.separator();
                            }
                            ui.label("Play again?");
                            if ui.button("Yes").clicked() {
                                let _ = client_rpc_sender.unbounded_send(
                                    ReliableRpcClientMessage::ContinueRound(YesOrNo::Yes),
                                );
                                // TODO: make it so that if you disconnect from the lobby you don't disconnect from the server
                                ui_game_state.ui_state = UiState::NotConnected;
                            }
                            if ui.button("No").clicked() {
                                let _ = client_rpc_sender.unbounded_send(
                                    ReliableRpcClientMessage::ContinueRound(YesOrNo::No),
                                );
                                ui_game_state.ui_state = UiState::NotConnected;
                            }
                        }
                    }
                });
        });
    }
}
