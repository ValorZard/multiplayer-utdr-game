use shared::{GameLogic, MoveInputState, PLAYER_GAME_RECTANGLE, PlayerSide};

fn main() {
    let mut logic = GameLogic::new();
    let rect = logic.get_obstacle_rectangles()[0];
    println!("rect pixel: {:?}", rect);
    println!("rect physics center: {:?}", rect.get_physics_position());
    println!(
        "rect physics half extents: {:?}",
        rect.get_half_extents_for_physics()
    );
    println!(
        "player half extents (physics): {:?}",
        PLAYER_GAME_RECTANGLE.get_half_extents_for_physics()
    );
    println!(
        "left player real spawn pos: {:?}",
        logic.get_position(PlayerSide::Left)
    );
    println!(
        "right player real spawn pos: {:?}",
        logic.get_position(PlayerSide::Right)
    );

    // start aligned with the box's y-center (pixel space) so the approach path actually crosses it
    logic.update_position_with_vec(PlayerSide::Left, glam::Vec2::new(-250.0, 30.0));
    logic.step_physics();

    let input = MoveInputState {
        up: false,
        down: false,
        left: false,
        right: true,
    };
    for i in 0..100 {
        logic.update_position_with_input(PlayerSide::Left, &input);
        logic.step_physics();
        println!(
            "tick {i}: left pos = {:?}",
            logic.get_position(PlayerSide::Left)
        );
    }
    println!("final left pos: {:?}", logic.get_position(PlayerSide::Left));
}
