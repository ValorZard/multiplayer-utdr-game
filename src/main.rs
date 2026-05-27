use kiss3d::prelude::*;
use include_dir::{include_dir, Dir};

static ASSET_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR\\assets");

#[kiss3d::main]
async fn main() {
    let mut window = Window::new("Kiss3d: rectangle").await;
    let mut camera = PanZoomCamera2d::new(Vec2::ZERO, 2.0);
    let mut scene = SceneNode2d::empty();

    let image_buffer = ASSET_DIR.get_file("background_concept_2.png").unwrap();

    let image_texture = image::load_from_memory(image_buffer.contents()).unwrap();

    let mut rect = scene
        .add_rectangle(image_texture.width() as f32 * 0.5, image_texture.height() as f32 * 0.5)
        .set_lines_width(10.0, false)
        .set_lines_color(Some(WHITE))
        .set_texture_from_memory(image_buffer.contents(), "background_concept_2.png");
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
}