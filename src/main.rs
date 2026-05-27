use include_dir::{Dir, include_dir};
use kiss3d::prelude::*;
#[cfg(target_arch = "wasm32")]
use kiss3d::wasm_bindgen_futures::spawn_local;
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

    #[cfg(target_arch = "wasm32")]
    spawn_local(async {
        crate::connection::connect_to_websocket_server_wasm().await;
    });

    #[cfg(not(target_arch = "wasm32"))]
    let connection_thread_handle =
        thread::spawn(crate::connection::connect_to_websocket_server_native);

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
        .set_color(BLUE)
        .set_lines_width(5.0, false)
        .set_lines_color(Some(MAGENTA));

    let rot_rect = 0.014;
    let rot_circ = -0.014;

    while window.render_2d(&mut scene, &mut camera).await {
        rect.append_rotation(rot_rect);
        circ.append_rotation(rot_circ);
    }

    #[cfg(not(target_arch = "wasm32"))]
    let _ = connection_thread_handle.join();
}
