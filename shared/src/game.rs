use crate::rpc::RemoteTimestamp;
use rapier2d::control::KinematicCharacterController;
use rapier2d::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;

pub type Health = u32;

pub const PLAYER_MAX_HEALTH: Health = 10;
pub const ENEMY_MAX_HEALTH: Health = 20;
pub const ATTACK_DAMAGE: Health = 1;
/// How much incoming damage a Defend action absorbs during the following dodge phase.
pub const DEFEND_BLOCK: Health = 1;
pub const PROJECTILE_HIT_DAMAGE: Health = 1;

/// Length of the bullet-hell dodge segment, in game ticks (30 TPS => ~10 seconds).
pub const DODGE_PHASE_TICKS: u32 = 300;
/// Wall-clock gap between bullets of a dodge pattern. Spawning is scheduled in
/// unix time (not ticks) so client and server can both derive the identical
/// schedule from the pattern start timestamp alone.
pub const PROJECTILE_SPAWN_INTERVAL_MS: RemoteTimestamp = 500;
/// How far in the future the server schedules a pattern start, so the reliable
/// message announcing it reaches clients before the first bullet spawns.
pub const PATTERN_START_LEAD_MS: RemoteTimestamp = 1000;
/// Projectile speed in game (pixel) space, i.e. pixels per second.
pub const PROJECTILE_SPEED: f32 = 150.0;
/// Projectiles render and collide as squares with this side length (px).
pub const PROJECTILE_SIZE: f32 = 6.0;
/// Where the enemy sits in game (pixel) space; projectiles spawn from here.
pub const ENEMY_POSITION: Vec2 = Vec2::new(0.0, 120.0);
/// Projectiles farther than this from the origin are despawned.
pub const PROJECTILE_DESPAWN_RADIUS: f32 = 500.0;

/// How many bullets of a pattern should exist by `now_ms`, given when the
/// pattern started. Both client and server run this against their own clocks
/// so they spawn the same bullets at (approximately) the same instant.
pub fn dodge_pattern_bullet_count(
    pattern_start_ms: RemoteTimestamp,
    now_ms: RemoteTimestamp,
) -> u32 {
    if now_ms < pattern_start_ms {
        return 0;
    }
    ((now_ms - pattern_start_ms) / PROJECTILE_SPAWN_INTERVAL_MS + 1) as u32
}

/// Deterministic dodge pattern: bullet `index` of a segment, as (spawn
/// position, velocity) in game (pixel) space. A golden-angle fan biased
/// downward gives spread-out coverage of the arena below the enemy without
/// any RNG, so both sides generate identical bullets from the index alone.
pub fn dodge_pattern_bullet(index: u32) -> (Vec2, Vec2) {
    const GOLDEN_ANGLE: f32 = 2.399_963;
    // wrap the fan into the lower half circle (pointing at the play area)
    let sweep = (index as f32 * GOLDEN_ANGLE) % std::f32::consts::PI;
    let angle = -std::f32::consts::PI + sweep;
    let direction = Vec2::new(angle.cos(), angle.sin());
    (ENEMY_POSITION, direction * PROJECTILE_SPEED)
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum TurnAction {
    Attack,
    Defend,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum BattleWinner {
    Players,
    Enemy,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct BattleStats {
    pub left_health: Health,
    pub right_health: Health,
    pub enemy_health: Health,
}

impl BattleStats {
    pub fn new() -> Self {
        Self {
            left_health: PLAYER_MAX_HEALTH,
            right_health: PLAYER_MAX_HEALTH,
            enemy_health: ENEMY_MAX_HEALTH,
        }
    }

    pub fn health_for(&self, side: PlayerSide) -> Health {
        match side {
            PlayerSide::Left => self.left_health,
            PlayerSide::Right => self.right_health,
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum BattleGameState {
    // Both players pick simultaneously; the flags only say who has locked in,
    // never what they picked, so choices stay hidden until the turn resolves.
    ChoosingActions {
        left_ready: bool,
        right_ready: bool,
        stats: BattleStats,
    },
    Dodging {
        ticks_remaining: u32,
        // unix ms timestamp at which the bullet pattern starts; clients
        // predict the pattern locally from this shared clock reference
        pattern_start_time_ms: RemoteTimestamp,
        stats: BattleStats,
    },
    Win {
        winner: BattleWinner,
        stats: BattleStats,
    },
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub enum PlayerSide {
    Left,
    Right,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // Derives can be passed through to the generated type:
    derive(Debug),
    compare(PartialEq)
)]
pub struct MoveGameState {
    pub left_position: Vec2,
    pub right_position: Vec2,
}

impl MoveGameState {
    pub fn new() -> Self {
        Self {
            left_position: Vec2::ZERO,
            right_position: Vec2::ZERO,
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct MoveInputState {
    pub up: bool,
    pub down: bool,
    pub left: bool,
    pub right: bool,
}
impl Default for MoveInputState {
    fn default() -> Self {
        Self {
            up: false,
            down: false,
            left: false,
            right: false,
        }
    }
}

impl MoveInputState {
    pub fn as_normalized_vec(&self) -> Vec2 {
        Vec2::new(
            (self.right as i8 - self.left as i8) as f32,
            (self.up as i8 - self.down as i8) as f32,
        )
        .normalize_or_zero()
    }
}

// deltarune runs on 30 TPS
pub const GAME_TIME_STEP: Duration = Duration::from_millis(33);
pub const GAME_TIME_DELTA: f32 = GAME_TIME_STEP.as_secs_f32();
/// Player movement speed in game (pixel) space, i.e. pixels per second.
pub const PLAYER_SPEED: f32 = 100.;
#[derive(Debug, PartialEq)]
pub enum LogicError {
    PlayerAlreadyExists(PlayerSide),
    PlayerDoesNotExist(PlayerSide),
}

impl std::fmt::Display for LogicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicError::PlayerAlreadyExists(side) => {
                write!(f, "Error! Player {side:?} already exists!")
            }
            LogicError::PlayerDoesNotExist(side) => {
                write!(f, "Error! Player {side:?} does not exist!")
            }
        }
    }
}

impl std::error::Error for LogicError {}

pub fn convert_vec2_pixel_to_physics(position: Vec2) -> Vec2 {
    Vec2 {
        x: position.x * PIXEL_TO_PHYSICS_SCALE,
        y: position.y * PIXEL_TO_PHYSICS_SCALE,
    }
}

pub fn convert_vec2_physics_to_pixel(position: Vec2) -> Vec2 {
    Vec2 {
        x: position.x * PHYSICS_TO_PIXEL_SCALE,
        y: position.y * PHYSICS_TO_PIXEL_SCALE,
    }
}

// Collision groups: players collide with obstacles but not with each other
pub const PLAYER_GROUP: Group = Group::GROUP_1;
pub const OBSTACLE_GROUP: Group = Group::GROUP_2;
pub const HITBOX_GROUP: Group = Group::GROUP_3;
pub const HURTBOX_GROUP: Group = Group::GROUP_4;
pub const INTERACTION_GROUP: Group = Group::GROUP_5;

pub struct Obstacle {}

/// ECS marker for enemy bullets during the dodge phase.
pub struct Projectile {}

// this is how the rectangle is displayed in the game world, not in physics coords
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct GameRectangle {
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

impl From<Aabb> for GameRectangle {
    fn from(aabb: Aabb) -> Self {
        let min = convert_vec2_physics_to_pixel(aabb.mins);
        let max = convert_vec2_physics_to_pixel(aabb.maxs);
        GameRectangle {
            x: min.x,
            y: min.y,
            width: max.x - min.x,
            height: max.y - min.y,
        }
    }
}

impl GameRectangle {
    pub const fn new(x: f32, y: f32, width: f32, height: f32) -> Self {
        GameRectangle {
            x,
            y,
            width,
            height,
        }
    }

    // both kiss3d and rapier put objects based on their position in the center of the object
    // so there's no need to adjust for width or height
    // see: https://docs.rs/kiss3d/latest/kiss3d/camera/enum.CoordinateSystem2d.html#variant.CenterUp
    pub const fn get_physics_position(&self) -> Vec2 {
        let center_x = self.x * PIXEL_TO_PHYSICS_SCALE;
        let center_y = self.y * PIXEL_TO_PHYSICS_SCALE;
        Vec2::new(center_x, center_y)
    }

    pub const fn get_pixel_position(&self) -> Vec2 {
        Vec2::new(self.x, self.y)
    }

    pub const fn get_half_extents_for_physics(&self) -> Vec2 {
        Vec2::new(
            (self.width * 0.5) * PIXEL_TO_PHYSICS_SCALE,
            (self.height * 0.5) * PIXEL_TO_PHYSICS_SCALE,
        )
    }
}

pub struct GameLogic {
    world: hecs::World,
    physics: PhysicsWorld,
    left_player: hecs::Entity,
    right_player: hecs::Entity,
}

pub const PHYSICS_TO_PIXEL_SCALE: f32 = 50.0; // 1 meter in physics engine equals 50 pixels
pub const PIXEL_TO_PHYSICS_SCALE: f32 = 1.0 / PHYSICS_TO_PIXEL_SCALE;
pub const PLAYER_PHYSICS_RADIUS: f32 = 1.0; // in physics scale
pub const PLAYER_GAME_RECTANGLE: GameRectangle = GameRectangle::new(0., 0., 10., 10.);
pub const PLAYER_STARTING_POSITION: Vec2 = Vec2::new(0., 0.);

impl GameLogic {
    pub fn new() -> Self {
        // TODO: for now, we hardcode left and right side players and require there to be only one of each
        let mut world = hecs::World::new();
        let mut physics = PhysicsWorld::new();
        // The physics default timestep is 1/60s but the game ticks at 30 TPS;
        // velocity-driven bodies (projectiles) would move at half speed if the
        // integration step didn't match the game tick.
        physics.integration_parameters.dt = GAME_TIME_DELTA;

        // Distinct starting points on opposite sides, clear of the obstacle (spawned
        // below) and of each other, so neither player starts already overlapping it.
        let left_player = Self::spawn_player(
            &mut world,
            &mut physics,
            PlayerSide::Left,
            PLAYER_STARTING_POSITION,
        );
        let right_player = Self::spawn_player(
            &mut world,
            &mut physics,
            PlayerSide::Right,
            PLAYER_STARTING_POSITION,
        );

        // spawn physics object for players to collide with
        // create a rectangle in the game world
        let rectangle = GameRectangle::new(40.0, 30.0, 20.0, 20.0);
        // GameRectangle's x/y is its center (matching kiss3d's center-based
        // set_position), so the physics collider sits at the same point.
        let physics_position = rectangle.get_physics_position();
        let rectangle_half_extents = rectangle.get_half_extents_for_physics();
        let collider = ColliderBuilder::cuboid(rectangle_half_extents.x, rectangle_half_extents.y)
            .collision_groups(InteractionGroups::new(
                OBSTACLE_GROUP,
                PLAYER_GROUP,
                InteractionTestMode::And,
            ))
            .position(Pose2 {
                rotation: Rot2::default(),
                translation: physics_position,
            })
            .build();
        let collider_handle = physics.colliders.insert(collider);
        world.spawn((rectangle, Obstacle {}, collider_handle));

        // The broad-phase spatial index used by scene queries (incl. the character
        // controller's move_shape) isn't populated until a step runs; without this,
        // the very first update_position_with_input call queries a stale/empty index
        // and misses newly-inserted colliders entirely.
        physics.step();

        Self {
            world,
            left_player,
            right_player,
            physics,
        }
    }

    fn spawn_player(
        world: &mut hecs::World,
        physics: &mut PhysicsWorld,
        side: PlayerSide,
        spawn_position: Vec2,
    ) -> hecs::Entity {
        let rigid_body = RigidBodyBuilder::kinematic_position_based()
            .translation(convert_vec2_pixel_to_physics(spawn_position))
            .build();

        let mut player_rect = PLAYER_GAME_RECTANGLE.clone();
        player_rect.x = spawn_position.x;
        player_rect.y = spawn_position.y;

        let half_extents = player_rect.get_half_extents_for_physics();

        let collider = ColliderBuilder::cuboid(half_extents.x, half_extents.y)
            .collision_groups(InteractionGroups::new(
                PLAYER_GROUP,
                OBSTACLE_GROUP,
                InteractionTestMode::And,
            ))
            .build();

        let (body_handle, collider_handle) = physics.insert(rigid_body, collider);

        // Top-down game, no floor/gravity, so ground-snapping doesn't apply here.
        let controller = KinematicCharacterController {
            snap_to_ground: None,
            ..Default::default()
        };

        world.spawn((side, body_handle, collider_handle, controller, player_rect))
    }

    pub fn get_obstacle_rectangles(&mut self) -> Vec<GameRectangle> {
        self.world
            .query_mut::<(&GameRectangle, &Obstacle)>()
            .into_iter()
            .map(|(rectangle, _)| rectangle)
            .copied()
            .collect()
    }

    fn character_handle_for(
        &mut self,
        player_side: PlayerSide,
    ) -> (&RigidBodyHandle, &KinematicCharacterController) {
        let entity = match player_side {
            PlayerSide::Left => self.left_player,
            PlayerSide::Right => self.right_player,
        };
        self.world
            .query_one_mut::<(&RigidBodyHandle, &KinematicCharacterController)>(entity)
            .expect("Player should exist here")
    }

    fn set_position_for_player_rect(&mut self, player_side: PlayerSide, new_position: Vec2) {
        let entity = match player_side {
            PlayerSide::Left => self.left_player,
            PlayerSide::Right => self.right_player,
        };
        let rect = self
            .world
            .query_one_mut::<&mut GameRectangle>(entity)
            .expect("Player should exist here");
        rect.x = new_position.x;
        rect.y = new_position.y;
    }

    pub fn setup_game(&mut self) {
        // Easier way to do this is to just totally reset all state
        *self = Self::new();
    }

    // returns position in game space not physics space
    pub fn update_position_with_input(
        &mut self,
        player_side: PlayerSide,
        input: &MoveInputState,
    ) -> Vec2 {
        let (handle, controller) = self.character_handle_for(player_side);
        let handle = *handle;
        let controller = *controller;
        // following code is based off of:
        // https://github.com/dimforge/rapier/blob/c13133ad293ee70c7f9cec9e498eac016c362169/examples2d/utils/character.rs#L125

        let character_body = &self.physics.bodies[handle];
        let character_collider = &self.physics.colliders[character_body.colliders()[0]];
        let character_rotation = character_collider.position().rotation;
        let character_shape = character_collider.shared_shape().clone();
        let character_mass = character_body.mass();
        // QueryFilter ignores each collider's own collision_groups unless told to check
        // them; without this, the query treats every collider (e.g. the other player) as
        // an obstacle, not just the ones this player's group is actually set to interact with.
        let character_groups = character_collider.collision_groups();

        // Reconciliation may apply multiple inputs before a physics step;
        // use the pending kinematic target so updates accumulate deterministically.
        let current_position = character_body.next_position().translation;
        // PLAYER_SPEED is px/s; move_shape works in physics space, so convert.
        let desired_movement =
            input.as_normalized_vec() * PLAYER_SPEED * PIXEL_TO_PHYSICS_SCALE * GAME_TIME_DELTA;

        let mut query_pipeline = self.physics.broad_phase.as_query_pipeline_mut(
            self.physics.narrow_phase.query_dispatcher(),
            &mut self.physics.bodies,
            &mut self.physics.colliders,
            QueryFilter::new()
                .exclude_rigid_body(handle)
                .groups(character_groups),
        );

        let mut collisions = vec![];
        let movement = controller.move_shape(
            GAME_TIME_DELTA,
            &query_pipeline.as_ref(),
            &*character_shape,
            &Pose2 {
                rotation: character_rotation,
                translation: current_position,
            },
            desired_movement,
            |c| collisions.push(c),
        );

        controller.solve_character_collision_impulses(
            GAME_TIME_DELTA,
            &mut query_pipeline,
            &*character_shape,
            character_mass,
            &collisions,
        );

        let new_pos = current_position + movement.translation;

        // Kinematic bodies don't move until you tell the physics step
        // where they're going next.
        self.physics.bodies[handle].set_next_kinematic_translation(new_pos);
        let new_pixel_position = Vec2::new(
            new_pos.x * PHYSICS_TO_PIXEL_SCALE,
            new_pos.y * PHYSICS_TO_PIXEL_SCALE,
        );
        self.set_position_for_player_rect(player_side, new_pixel_position);
        new_pixel_position
    }

    /// `new_position` is in game (pixel) space, like every other public position.
    pub fn update_position_with_vec(
        &mut self,
        player_side: PlayerSide,
        new_position: Vec2,
    ) -> Vec2 {
        let (handle, _) = self.character_handle_for(player_side);
        let handle = handle.clone();
        let body = &mut self.physics.bodies[handle];
        body.set_next_kinematic_translation(convert_vec2_pixel_to_physics(new_position));
        self.set_position_for_player_rect(player_side, new_position);
        new_position
    }

    /// Returns position in game (pixel) space, not physics space.
    pub fn get_position(&mut self, player_side: PlayerSide) -> Vec2 {
        let entity = match player_side {
            PlayerSide::Left => self.left_player,
            PlayerSide::Right => self.right_player,
        };
        self.world
            .query_one_mut::<&GameRectangle>(entity)
            .expect("Player should exist here")
            .get_pixel_position()
    }

    /// Advances the physics simulation. Call once per tick, after inputs
    /// have been applied via update_position_with_input/_vec.
    pub fn step_physics(&mut self) {
        self.physics.step();
    }

    /// Spawns an enemy bullet. Both position and velocity are in game (pixel)
    /// space. The bullet is a velocity-driven kinematic body whose sensor
    /// collider lives in HITBOX_GROUP, so it never physically pushes players
    /// or obstacles; hits are picked up by detect_projectile_hits instead.
    pub fn spawn_projectile(&mut self, position: Vec2, velocity: Vec2) -> hecs::Entity {
        let rigid_body = RigidBodyBuilder::kinematic_velocity_based()
            .translation(convert_vec2_pixel_to_physics(position))
            .linvel(convert_vec2_pixel_to_physics(velocity))
            .build();
        let half_extent = (PROJECTILE_SIZE * 0.5) * PIXEL_TO_PHYSICS_SCALE;
        let collider = ColliderBuilder::cuboid(half_extent, half_extent)
            .sensor(true)
            .collision_groups(InteractionGroups::new(
                HITBOX_GROUP,
                PLAYER_GROUP,
                InteractionTestMode::And,
            ))
            .build();
        let (body_handle, collider_handle) = self.physics.insert(rigid_body, collider);
        self.world
            .spawn((Projectile {}, body_handle, collider_handle))
    }

    /// Positions of all live projectiles in game (pixel) space, for rendering.
    pub fn get_projectile_positions(&mut self) -> Vec<Vec2> {
        let bodies = &self.physics.bodies;
        self.world
            .query_mut::<&RigidBodyHandle>()
            .with::<&Projectile>()
            .into_iter()
            .map(|handle| convert_vec2_physics_to_pixel(bodies[*handle].position().translation))
            .collect()
    }

    pub fn clear_projectiles(&mut self) {
        let entities: Vec<(hecs::Entity, RigidBodyHandle)> = self
            .world
            .query_mut::<(hecs::Entity, &RigidBodyHandle)>()
            .with::<&Projectile>()
            .into_iter()
            .map(|(entity, handle)| (entity, *handle))
            .collect();
        for (entity, body_handle) in entities {
            self.physics.remove_body(body_handle);
            let _ = self.world.despawn(entity);
        }
    }

    /// Finds projectiles overlapping each hittable player, despawns them
    /// (plus any that flew out of the arena), and returns how many hit each
    /// side. Call once per tick after step_physics. A projectile overlapping
    /// both players only counts against the left one, since it despawns on
    /// its first hit.
    pub fn detect_projectile_hits(
        &mut self,
        left_hittable: bool,
        right_hittable: bool,
    ) -> (u32, u32) {
        let mut hits_per_side: [HashSet<ColliderHandle>; 2] = [HashSet::new(), HashSet::new()];
        let [left_set, right_set] = &mut hits_per_side;
        for (side, hits) in [(PlayerSide::Left, left_set), (PlayerSide::Right, right_set)] {
            let hittable = match side {
                PlayerSide::Left => left_hittable,
                PlayerSide::Right => right_hittable,
            };
            if !hittable {
                continue;
            }
            let entity = match side {
                PlayerSide::Left => self.left_player,
                PlayerSide::Right => self.right_player,
            };
            let collider_handle = *self
                .world
                .query_one_mut::<&ColliderHandle>(entity)
                .expect("Player should exist here");
            let player_collider = &self.physics.colliders[collider_handle];
            let player_pose = *player_collider.position();
            let player_shape = player_collider.shared_shape().clone();

            // This query's groups pass the mutual And test only against
            // projectile colliders (HITBOX memberships, PLAYER filter), so
            // obstacles and the other player never show up here.
            let query_pipeline = self.physics.broad_phase.as_query_pipeline(
                self.physics.narrow_phase.query_dispatcher(),
                &self.physics.bodies,
                &self.physics.colliders,
                QueryFilter::new().groups(InteractionGroups::new(
                    PLAYER_GROUP,
                    HITBOX_GROUP,
                    InteractionTestMode::And,
                )),
            );
            hits.extend(
                query_pipeline
                    .intersect_shape(player_pose, &*player_shape)
                    .map(|(handle, _)| handle),
            );
        }

        let despawn_distance = PROJECTILE_DESPAWN_RADIUS * PIXEL_TO_PHYSICS_SCALE;
        let mut left_hits = 0;
        let mut right_hits = 0;
        let mut to_despawn: Vec<(hecs::Entity, RigidBodyHandle)> = Vec::new();
        for (entity, body_handle, collider_handle) in self
            .world
            .query_mut::<(hecs::Entity, &RigidBodyHandle, &ColliderHandle)>()
            .with::<&Projectile>()
        {
            if hits_per_side[0].contains(collider_handle) {
                left_hits += 1;
            } else if hits_per_side[1].contains(collider_handle) {
                right_hits += 1;
            } else if self.physics.bodies[*body_handle]
                .position()
                .translation
                .length()
                <= despawn_distance
            {
                continue;
            }
            to_despawn.push((entity, *body_handle));
        }
        for (entity, body_handle) in to_despawn {
            self.physics.remove_body(body_handle);
            let _ = self.world.despawn(entity);
        }

        (left_hits, right_hits)
    }

    pub fn get_state_to_send_to_client(&mut self) -> MoveGameState {
        let left_position = self.get_position(PlayerSide::Left);
        let right_position = self.get_position(PlayerSide::Right);

        MoveGameState {
            left_position,
            right_position,
        }
    }
}
