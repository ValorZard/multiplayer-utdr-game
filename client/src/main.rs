use futures_util::{FutureExt, StreamExt};
use include_dir::{Dir, include_dir};
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
use kiss3d::{egui, prelude::*};
use rpc::{GameInput, RPSGameState, RpcClientMessage, RpcServerMessage};
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
            crate::connection::connect_to_websocket_server_wasm(client_rpc_receiver, server_rpc_sender).await;
        });
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        let _connection_handle = thread::spawn(move || {
            crate::connection::connect_to_websocket_server_native(client_rpc_receiver, server_rpc_sender)
        });
    }

    let mut rect = scene
        .add_rectangle(
            image_texture.size.0 as f32 * 0.5,
            image_texture.size.1 as f32 * 0.5,
        )
        .set_lines_width(10.0, false)
        .set_lines_color(Some(WHITE))
        .set_texture(image_texture);
    rect.read_uvs(&mut |uv_vec| {
        println!("{:?}", uv_vec);
    });
    let mut circ = scene
        .add_circle(50.0)
        .translate(Vec2::new(200.0, 0.0))
        .set_color(BLUE);

    let rot_rect = 0.014;
    let rot_circ = -0.014;

    // UI state
    let mut rotation_speed = 0.014;
    let mut opacity = 1.0;
    let mut circle_color = [1.0, 0.0, 0.0];

    let mut current_game_state: Option<RPSGameState> = None;
    while window.render_2d(&mut scene, &mut camera).await {
        // immediately pool the receiver even if there isn't a value there.
        while let Some(rpc_message) = server_rpc_receiver.next().now_or_never().flatten() {
            log!("{rpc_message:?}");
            match rpc_message {
                RpcServerMessage::Text(text) => {
                    // TODO: Do something here I guess
                }
                RpcServerMessage::GameState(game_state) => {
                    current_game_state = Some(game_state);
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
        rect.append_rotation(rot_rect);
        circ.append_rotation(rot_circ);

        // set circle color
        circ.set_color(Color::new(
            circle_color[0],
            circle_color[1],
            circle_color[2],
            opacity,
        ));

        // Draw UI
        window.draw_ui(|ctx| {
            egui::Window::new("Kiss3d egui Example")
                .default_width(300.0)
                .show(ctx, |ui| {
                    // Rotation control
                    ui.label(format!("{current_game_state:?}"));

                    ui.separator();

                    if ui.button("Rock").clicked() {
                        let _ = client_rpc_sender.unbounded_send(RpcClientMessage::GameInput(GameInput::Rock));
                    }
                    if ui.button("Paper").clicked() {
                        let _ = client_rpc_sender.unbounded_send(RpcClientMessage::GameInput(GameInput::Paper));
                    }
                    if ui.button("Scissors").clicked() {
                        let _ = client_rpc_sender.unbounded_send(RpcClientMessage::GameInput(GameInput::Scissors));
                    }

                    // Opacity control
                    ui.label("Opacity:");
                    ui.add(egui::Slider::new(&mut opacity, 0.0..=1.0));

                    // Color picker
                    ui.label("Cube Color:");

                    ui.horizontal(|ui| {
                        ui.color_edit_button_rgb(&mut circle_color);
                    });
                });
        });
    }
}
