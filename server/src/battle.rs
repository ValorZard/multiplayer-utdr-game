use rapier2d::math::Vec2;
use shared::game::{
    ATTACK_DAMAGE, BattleGameState, BattleStats, BattleWinner, DEFEND_BLOCK, DODGE_PHASE_TICKS,
    GameLogic, Health, MoveGameState, MoveInputState, PROJECTILE_HIT_DAMAGE, PlayerSide,
    TurnAction, get_current_bullet_count, get_data_for_projectile_from_index,
};
use shared::rpc::{InputSequence, RemoteTimestamp};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq)]
pub enum GameError {
    // turn actions are only accepted while both players are choosing
    NotInChoicePhase,
    PlayerIsDead(PlayerSide),
}

impl std::fmt::Display for GameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GameError::NotInChoicePhase => {
                write!(f, "Turn actions can only be made during the choice phase!")
            }
            GameError::PlayerIsDead(side) => {
                write!(f, "Player {side:?} is dead and cannot act!")
            }
        }
    }
}

impl std::error::Error for GameError {}

// The battle turn cycle: both players secretly choose, the turn resolves
// (attacks land on the enemy), then everyone dodges the enemy's bullets
// until the timer runs out and the next choice phase begins.
#[derive(Debug, Clone, Copy, PartialEq)]
enum BattlePhase {
    Choosing {
        left_action: Option<TurnAction>,
        right_action: Option<TurnAction>,
    },
    Dodging {
        ticks_remaining: u32,
        // damage each side can still absorb this segment (from Defend)
        left_block: Health,
        right_block: Health,
        // bullets of the pattern spawned into the simulation so far
        spawned_bullets: u32,
    },
    Won(BattleWinner),
}

pub struct GameSession {
    phase: BattlePhase,
    left_health: Health,
    right_health: Health,
    enemy_health: Health,
    game_logic: GameLogic,
    // the latest ack the server received
    right_remote_clock_ack: InputSequence,
    left_remote_clock_ack: InputSequence,
    left_pending_move_inputs: BTreeMap<InputSequence, MoveInputState>,
    right_pending_move_inputs: BTreeMap<InputSequence, MoveInputState>,
    // held/duplicated when nothing new arrived (dead reckoning)
    left_last_applied_input: MoveInputState,
    right_last_applied_input: MoveInputState,
    // this is to add timestamps to the snapshots we're sending to the client
    tick: InputSequence,
    // client_send_time_ms from the most recently received input message per side,
    // echoed back to the client for round-trip timing (see get_move_state callers)
    left_last_client_time_ms: RemoteTimestamp,
    right_last_client_time_ms: RemoteTimestamp,
}

impl GameSession {
    pub fn new() -> Self {
        let stats = BattleStats::new();
        Self {
            phase: BattlePhase::Choosing {
                left_action: None,
                right_action: None,
            },
            left_health: stats.left_health,
            right_health: stats.right_health,
            enemy_health: stats.enemy_health,
            game_logic: GameLogic::new(),
            left_remote_clock_ack: 0,
            right_remote_clock_ack: 0,
            left_pending_move_inputs: BTreeMap::new(),
            right_pending_move_inputs: BTreeMap::new(),
            left_last_applied_input: MoveInputState::default(),
            right_last_applied_input: MoveInputState::default(),
            tick: 0,
            left_last_client_time_ms: 0,
            right_last_client_time_ms: 0,
        }
    }

    fn stats(&self) -> BattleStats {
        BattleStats {
            left_health: self.left_health,
            right_health: self.right_health,
            enemy_health: self.enemy_health,
        }
    }

    pub fn compute_state(&self) -> BattleGameState {
        let stats = self.stats();
        match &self.phase {
            BattlePhase::Choosing {
                left_action,
                right_action,
            } => BattleGameState::ChoosingActions {
                // dead players don't get a vote, so they always read as ready
                left_ready: left_action.is_some() || self.left_health == 0,
                right_ready: right_action.is_some() || self.right_health == 0,
                stats,
            },
            BattlePhase::Dodging {
                ticks_remaining, ..
            } => BattleGameState::Dodging {
                ticks_remaining: *ticks_remaining,
                stats,
            },
            BattlePhase::Won(winner) => BattleGameState::Win {
                winner: *winner,
                stats,
            },
        }
    }

    // First action per side per turn wins; repeats are ignored so packet
    // retries can't change a locked-in choice.
    pub fn set_turn_action(
        &mut self,
        side: PlayerSide,
        action: TurnAction,
    ) -> Result<BattleGameState, GameError> {
        let BattlePhase::Choosing {
            left_action,
            right_action,
        } = &mut self.phase
        else {
            return Err(GameError::NotInChoicePhase);
        };

        let health = match side {
            PlayerSide::Left => self.left_health,
            PlayerSide::Right => self.right_health,
        };
        if health == 0 {
            return Err(GameError::PlayerIsDead(side));
        }

        match side {
            PlayerSide::Left => {
                if left_action.is_none() {
                    *left_action = Some(action);
                }
            }
            PlayerSide::Right => {
                if right_action.is_none() {
                    *right_action = Some(action);
                }
            }
        }

        let left_choice = *left_action;
        let right_choice = *right_action;
        let left_done = left_choice.is_some() || self.left_health == 0;
        let right_done = right_choice.is_some() || self.right_health == 0;
        if left_done && right_done {
            self.resolve_turn(left_choice, right_choice);
        }

        Ok(self.compute_state())
    }

    fn resolve_turn(&mut self, left_choice: Option<TurnAction>, right_choice: Option<TurnAction>) {
        let attackers = [left_choice, right_choice]
            .iter()
            .filter(|choice| matches!(choice, Some(TurnAction::Attack)))
            .count() as Health;
        self.enemy_health = self.enemy_health.saturating_sub(attackers * ATTACK_DAMAGE);

        // The enemy losing takes priority over everything else, so check it
        // before ever entering the dodge phase.
        if self.enemy_health == 0 {
            self.phase = BattlePhase::Won(BattleWinner::Players);
            return;
        }

        self.phase = BattlePhase::Dodging {
            ticks_remaining: DODGE_PHASE_TICKS,
            left_block: match left_choice {
                Some(TurnAction::Defend) => DEFEND_BLOCK,
                _ => 0,
            },
            right_block: match right_choice {
                Some(TurnAction::Defend) => DEFEND_BLOCK,
                _ => 0,
            },
            spawned_bullets: 0,
        };
    }

    pub fn get_left_remote_clock_ack(&self) -> InputSequence {
        self.left_remote_clock_ack
    }

    pub fn get_right_remote_clock_ack(&self) -> InputSequence {
        self.right_remote_clock_ack
    }

    pub fn set_left_move_input(
        &mut self,
        input: MoveInputState,
        sequence: InputSequence,
        client_send_time_ms: RemoteTimestamp,
    ) {
        // recorded unconditionally (not gated on sequence dedup) so the echoed
        // timestamp always reflects the most recent packet actually received
        self.left_last_client_time_ms = client_send_time_ms;
        // accept only strictly newer sequence ids than the latest applied
        if sequence > self.left_remote_clock_ack {
            self.left_pending_move_inputs
                .entry(sequence)
                .or_insert(input);
        }
    }

    pub fn set_right_move_input(
        &mut self,
        input: MoveInputState,
        sequence: InputSequence,
        client_send_time_ms: RemoteTimestamp,
    ) {
        self.right_last_client_time_ms = client_send_time_ms;
        // accept only strictly newer sequence ids than the latest applied
        if sequence > self.right_remote_clock_ack {
            self.right_pending_move_inputs
                .entry(sequence)
                .or_insert(input);
        }
    }

    pub fn get_left_last_client_time_ms(&self) -> RemoteTimestamp {
        self.left_last_client_time_ms
    }

    pub fn get_right_last_client_time_ms(&self) -> RemoteTimestamp {
        self.right_last_client_time_ms
    }

    pub fn get_move_state(&mut self) -> MoveGameState {
        self.game_logic.get_state_to_send_to_client()
    }

    pub fn get_tick(&self) -> InputSequence {
        self.tick
    }

    pub fn step(&mut self) {
        // Unreliable packets can be dropped permanently, so do not stall waiting
        // for contiguous sequences. Apply the next available received input.
        // TODO: Add server-side prediction maybe?
        let mut left_input = self
            .left_pending_move_inputs
            .pop_first()
            .map(|(seq, input)| {
                self.left_remote_clock_ack = seq;
                input
            })
            .unwrap_or(self.left_last_applied_input);
        // only apply input if player isn't dead
        if self.left_health <= 0 {
            left_input = MoveInputState::default();
        }
        let mut right_input = self
            .right_pending_move_inputs
            .pop_first()
            .map(|(seq, input)| {
                self.right_remote_clock_ack = seq;
                input
            })
            .unwrap_or(self.right_last_applied_input);
        // only apply input if player isn't dead
        if self.right_health <= 0 {
            right_input = MoveInputState::default();
        }

        self.left_last_applied_input = left_input;
        self.right_last_applied_input = right_input;

        self.game_logic
            .update_position_with_input(PlayerSide::Left, &left_input);
        self.game_logic
            .update_position_with_input(PlayerSide::Right, &right_input);
        self.game_logic.step_physics();

        self.tick = self.tick.wrapping_add(1);

        self.step_dodge_phase();
    }

    fn step_dodge_phase(&mut self) {
        let BattlePhase::Dodging {
            ticks_remaining,
            mut left_block,
            mut right_block,
            mut spawned_bullets,
        } = self.phase
        else {
            // players should be reset back to center of the box and should not be allowed to move until dodging phase has commenced.
            self.game_logic
                .update_position_with_vec(PlayerSide::Left, Vec2::ZERO);
            self.game_logic
                .update_position_with_vec(PlayerSide::Right, Vec2::ZERO);
            return;
        };
        let ticks_remaining = ticks_remaining.saturating_sub(1);

        // Spawn any bullets whose scheduled tick has passed.
        // The client runs this same schedule/pattern against its own phase tick clock,
        // which is what keeps the predicted bullets in sync with the server's.
        let elapsed_ticks = DODGE_PHASE_TICKS - ticks_remaining;
        while spawned_bullets < get_current_bullet_count(elapsed_ticks) {
            let (position, velocity) = get_data_for_projectile_from_index(spawned_bullets);
            self.game_logic.spawn_projectile(position, velocity);
            spawned_bullets += 1;
        }

        let (left_hits, right_hits) = self
            .game_logic
            .detect_projectile_hits(self.left_health > 0, self.right_health > 0);

        for _ in 0..left_hits {
            if left_block > 0 {
                left_block -= 1;
            } else {
                self.left_health = self.left_health.saturating_sub(PROJECTILE_HIT_DAMAGE);
            }
        }
        for _ in 0..right_hits {
            if right_block > 0 {
                right_block -= 1;
            } else {
                self.right_health = self.right_health.saturating_sub(PROJECTILE_HIT_DAMAGE);
            }
        }

        self.phase = if self.left_health == 0 && self.right_health == 0 {
            self.game_logic.clear_projectiles();
            BattlePhase::Won(BattleWinner::Enemy)
        } else if ticks_remaining == 0 {
            self.game_logic.clear_projectiles();
            BattlePhase::Choosing {
                left_action: None,
                right_action: None,
            }
        } else {
            BattlePhase::Dodging {
                ticks_remaining,
                left_block,
                right_block,
                spawned_bullets,
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapier2d::prelude::Vec2;
    use shared::game::{ENEMY_MAX_HEALTH, PATTERN_START_LEAD_TICKS, PLAYER_MAX_HEALTH};

    fn teleport(session: &mut GameSession, side: PlayerSide, position: Vec2) {
        session.game_logic.update_position_with_vec(side, position);
        session.game_logic.step_physics();
    }

    #[test]
    fn attacks_damage_enemy_and_choices_stay_hidden_until_resolved() {
        let mut session = GameSession::new();

        let state = session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("left should be able to act");
        // one side locked in, still waiting on the other, enemy untouched
        assert_eq!(
            state,
            BattleGameState::ChoosingActions {
                left_ready: true,
                right_ready: false,
                stats: BattleStats::new(),
            }
        );

        let state = session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");
        let BattleGameState::Dodging {
            ticks_remaining,
            stats,
        } = state
        else {
            panic!("both players acted, dodge phase should begin: {state:?}");
        };
        assert_eq!(ticks_remaining, DODGE_PHASE_TICKS);
        assert_eq!(stats.enemy_health, ENEMY_MAX_HEALTH - 2 * ATTACK_DAMAGE);
    }

    #[test]
    fn repeated_actions_do_not_change_locked_choice() {
        let mut session = GameSession::new();

        session
            .set_turn_action(PlayerSide::Left, TurnAction::Defend)
            .expect("first action should lock in");
        session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("retries are accepted but ignored");
        session
            .set_turn_action(PlayerSide::Right, TurnAction::Defend)
            .expect("right should be able to act");

        // only defends were locked in, so the enemy took no damage
        let BattleGameState::Dodging { stats, .. } = session.compute_state() else {
            panic!("dodge phase should have begun");
        };
        assert_eq!(stats.enemy_health, ENEMY_MAX_HEALTH);
    }

    #[test]
    fn actions_rejected_during_dodge_phase() {
        let mut session = GameSession::new();
        session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("left should be able to act");
        session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");

        assert_eq!(
            session.set_turn_action(PlayerSide::Left, TurnAction::Attack),
            Err(GameError::NotInChoicePhase)
        );
    }

    #[test]
    fn players_win_when_enemy_health_reaches_zero() {
        let mut session = GameSession::new();
        session.enemy_health = 2;
        session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("left should be able to act");
        let state = session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");

        let BattleGameState::Win { winner, stats } = state else {
            panic!("enemy at 0 health should mean the players won: {state:?}");
        };
        assert_eq!(winner, BattleWinner::Players);
        assert_eq!(stats.enemy_health, 0);
    }

    #[test]
    fn enemy_wins_when_both_players_are_dead() {
        let mut session = GameSession::new();
        session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("left should be able to act");
        session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");

        session.left_health = 0;
        session.right_health = 0;
        session.step();

        let BattleGameState::Win { winner, .. } = session.compute_state() else {
            panic!("both players dead should mean the enemy won");
        };
        assert_eq!(winner, BattleWinner::Enemy);
    }

    /*
    #[test]
    fn dodge_phase_cycles_back_to_choosing() {
        let mut session = GameSession::new();
        session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("left should be able to act");
        session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");

        for _ in 0..DODGE_PHASE_TICKS {
            assert!(matches!(session.phase, BattlePhase::Dodging { .. }));
            session.step();
        }

        // teleport outside of the play area for the sake of this test
        teleport(&mut session, PlayerSide::Left, Vec2::new(-200., 0.));
        teleport(&mut session, PlayerSide::Right, Vec2::new(200., 0.));

        // survived the whole segment, so the next turn's choice phase begins
        assert_eq!(
            session.compute_state(),
            BattleGameState::ChoosingActions {
                left_ready: false,
                right_ready: false,
                stats: BattleStats {
                    enemy_health: ENEMY_MAX_HEALTH - 2 * ATTACK_DAMAGE,
                    ..BattleStats::new()
                },
            }
        );
    }
    */

    #[test]
    fn projectile_hits_damage_players_and_defend_blocks_one() {
        let mut session = GameSession::new();
        // separate the players so a bullet can only overlap its target
        teleport(&mut session, PlayerSide::Left, Vec2::new(-100.0, 0.0));
        teleport(&mut session, PlayerSide::Right, Vec2::new(100.0, 0.0));

        session
            .set_turn_action(PlayerSide::Left, TurnAction::Defend)
            .expect("left should be able to act");
        session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");

        // stationary bullet dropped right on each player; detected on the
        // next step once the physics broad phase has seen it
        session
            .game_logic
            .spawn_projectile(Vec2::new(-100.0, 0.0), Vec2::ZERO);
        session
            .game_logic
            .spawn_projectile(Vec2::new(100.0, 0.0), Vec2::ZERO);
        session.step();

        let stats = session.stats();
        // left's Defend absorbed the hit; right took it on the chin
        assert_eq!(stats.left_health, PLAYER_MAX_HEALTH);
        assert_eq!(
            stats.right_health,
            PLAYER_MAX_HEALTH - PROJECTILE_HIT_DAMAGE
        );

        // a second volley: left's block is spent now
        session
            .game_logic
            .spawn_projectile(Vec2::new(-100.0, 0.0), Vec2::ZERO);
        session.step();
        assert_eq!(
            session.stats().left_health,
            PLAYER_MAX_HEALTH - PROJECTILE_HIT_DAMAGE
        );
    }

    #[test]
    fn scheduled_pattern_spawns_projectiles_that_move() {
        let mut session = GameSession::new();
        session
            .set_turn_action(PlayerSide::Left, TurnAction::Attack)
            .expect("left should be able to act");
        session
            .set_turn_action(PlayerSide::Right, TurnAction::Attack)
            .expect("right should be able to act");

        // step the phase up to the first scheduled spawn tick: bullets should exist
        for _ in 0..PATTERN_START_LEAD_TICKS {
            session.step();
        }
        let initial_positions = session.game_logic.get_projectile_positions();
        assert!(
            !initial_positions.is_empty(),
            "pattern start tick passed, bullets should have spawned"
        );

        session.step();
        let moved_positions = session.game_logic.get_projectile_positions();
        assert_ne!(
            initial_positions[0], moved_positions[0],
            "projectiles should move under physics"
        );
    }
}
