use anyhow::{anyhow, bail};
use shared::game::{PlayerSide, RPSGameState};
use shared::rpc::{
    ConnectionInitMessage, LobbyId, PendingMoveInput, ReliableRpcClientMessage,
    ReliableRpcServerMessage, UnreliableRpcClientMessage, UserId, YesOrNo,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::lobby::LobbySessionHandle;

use crate::lobby::{UserReliableSender, UserUnreliableSender};
use shared::rpc::LobbyState;

#[derive(Debug, Clone)]
pub struct UserReliableRPCMessage {
    pub message: ReliableRpcClientMessage,
    pub send_addr: UserId,
}

#[derive(Debug, Clone)]
pub struct UserUnreliableRPCMessage {
    pub message: UnreliableRpcClientMessage,
    pub send_addr: UserId,
}

struct UserData {
    lobby_id: Option<LobbyId>,
    player_side: Option<PlayerSide>,
    reliable_sender: UserReliableSender,
    unreliable_sender: UserUnreliableSender,
}

struct FinishedLobbySession {
    lobby_session: LobbySessionHandle,
    left_side_continue: Option<YesOrNo>,
    right_side_continue: Option<YesOrNo>,
}

enum LobbyEntry {
    Waiting(LobbySessionHandle),
    Running(LobbySessionHandle),
    Finished(FinishedLobbySession),
}

impl LobbyEntry {
    #[allow(dead_code)]
    fn session(&self) -> &LobbySessionHandle {
        match self {
            LobbyEntry::Waiting(s) => s,
            LobbyEntry::Running(s) => s,
            LobbyEntry::Finished(f) => &f.lobby_session,
        }
    }

    #[allow(dead_code)]
    fn lobby_state(&self) -> LobbyState {
        match self {
            LobbyEntry::Waiting(_) => LobbyState::Waiting,
            LobbyEntry::Running(_) => LobbyState::Running,
            LobbyEntry::Finished(_) => LobbyState::Finished,
        }
    }
}

struct ServerStateInner {
    connected_user_list: HashMap<UserId, UserData>,
    lobby_list: HashMap<LobbyId, LobbyEntry>,
}

impl ServerStateInner {
    fn new() -> Self {
        Self {
            connected_user_list: HashMap::new(),
            lobby_list: HashMap::new(),
        }
    }

    fn connect_user(
        &mut self,
        addr: UserId,
        connection_init_message: ConnectionInitMessage,
        reliable_sender: UserReliableSender,
        unreliable_sender: UserUnreliableSender,
    ) -> anyhow::Result<()> {
        if let Some(_existing) = self.connected_user_list.get(&addr) {
            bail!("Can't double connect a user to the server");
        }

        reliable_sender.send(ReliableRpcServerMessage::ConnectionInit(
            addr,
            connection_init_message,
        ))?;

        self.connected_user_list.insert(
            addr,
            UserData {
                lobby_id: None,
                player_side: None,
                reliable_sender,
                unreliable_sender,
            },
        );

        info!("Connected user {addr} setup");

        Ok(())
    }

    // Note: We are assuming once we fill a lobby, we can't add any more users to it.
    async fn put_connected_user_in_lobby(
        &mut self,
        addr: UserId,
    ) -> anyhow::Result<(PlayerSide, LobbyId)> {
        let user_data = self
            .connected_user_list
            .get(&addr)
            .expect("If user is connected, they should be in connected list");

        // TODO: This is O(n), not O(log n)
        let waiting_lobby_id =
            self.lobby_list
                .iter()
                .find_map(|(lobby_id, lobby_entry)| match lobby_entry {
                    LobbyEntry::Waiting(_) => Some(*lobby_id),
                    _ => None,
                });

        let (player_side, lobby_id) = if let Some(lobby_id) = waiting_lobby_id
            && let Some(lobby_entry) = self.lobby_list.remove(&lobby_id)
        {
            let LobbyEntry::Waiting(lobby) = lobby_entry else {
                unreachable!(
                    "This should be waiting since we used find map to find something that matched what we want."
                );
            };
            lobby.reset_lobby().await;
            let (player_side, state) = lobby
                .insert_player((
                    addr,
                    user_data.reliable_sender.clone(),
                    user_data.unreliable_sender.clone(),
                ))
                .await?;
            self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
            info!("Lobby {lobby_id} should now be running: {state:?}");
            assert_eq!(state, LobbyState::Running);
            (player_side, lobby_id)
        } else {
            let lobby_id = Uuid::new_v4();
            let lobby = LobbySessionHandle::new(
                lobby_id,
                (
                    addr,
                    user_data.reliable_sender.clone(),
                    user_data.unreliable_sender.clone(),
                ),
            );
            info!(
                "Lobby {lobby_id} should now be waiting: {:?}",
                lobby.get_current_lobby_state().await
            );
            self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
            // Left is default when creating a new lobby
            (PlayerSide::Left, lobby_id)
        };

        if let Some(user) = self.connected_user_list.get_mut(&addr) {
            user.player_side = Some(player_side);
            user.lobby_id = Some(lobby_id);
        }

        Ok((player_side, lobby_id))
    }

    async fn disconnect_user(&mut self, addr: UserId) -> anyhow::Result<()> {
        let Some(user_data) = self.connected_user_list.get_mut(&addr) else {
            bail!("Cannot disconnect user {addr} from the server if it's not connected");
        };

        if let Some(lobby_id) = user_data.lobby_id {
            // removed player lobby is now empty
            // this can fail if the player totally disconnected
            let _ = user_data
                .reliable_sender
                .send(ReliableRpcServerMessage::LobbyState(LobbyState::Empty));
            user_data.lobby_id = None;
            user_data.player_side = None;

            // Pop lobby entry off, we can add it back in later
            let Some(lobby_entry) = self.lobby_list.remove(&lobby_id) else {
                bail!("Lobby {lobby_id} is invalid.");
            };

            match lobby_entry {
                LobbyEntry::Waiting(lobby) => {
                    let (_, state) = lobby.remove_player(addr).await?;

                    match state {
                        LobbyState::Empty => {
                            // delete lobby
                            info!("Waiting Lobby {lobby_id} is now destroyed, lobby was empty");
                        }
                        _ => unreachable!(
                            "Since we only have two players in lobby {lobby_id:?}, if we remove a player from a waiting lobby, its empty and can be deleted."
                        ),
                    }
                }

                LobbyEntry::Running(lobby) => {
                    let (_, state) = lobby.remove_player(addr).await?;

                    match state {
                        LobbyState::Waiting => {
                            self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
                        }
                        LobbyState::Empty => {
                            // delete lobby
                            info!("Running Lobby {lobby_id} is now destroyed, lobby was empty");
                        }
                        _ => unreachable!(),
                    }
                }

                LobbyEntry::Finished(finished) => {
                    let (_, state) = finished.lobby_session.remove_player(addr).await?;

                    match state {
                        LobbyState::Waiting => {
                            self.lobby_list
                                .insert(lobby_id, LobbyEntry::Waiting(finished.lobby_session));
                        }
                        LobbyState::Empty => {
                            // delete lobby
                            info!(
                                "Lobby {lobby_id} is now destroyed, both players rejected continuing to play"
                            );
                        }
                        LobbyState::Finished => {
                            unreachable!(
                                "If we remove a player, the lobby {lobby_id:?} can't be finished"
                            );
                        }
                        LobbyState::Running => unreachable!(
                            "If we remove a player, the lobby {lobby_id:?} can't be running"
                        ),
                    }
                }
            }
        }

        let Some(_user_data) = self.connected_user_list.remove(&addr) else {
            return Ok(());
        };

        Ok(())
    }

    #[allow(dead_code)]
    fn check_if_lobby_exists(&self, lobby_id: LobbyId) -> bool {
        self.lobby_list.contains_key(&lobby_id)
    }

    fn remove_connected_user_from_lobby(&mut self, addr: UserId) -> anyhow::Result<()> {
        if let Some(user) = self.connected_user_list.get_mut(&addr) {
            user.lobby_id = None;
            user.player_side = None;
            user.reliable_sender
                .send(ReliableRpcServerMessage::LobbyState(LobbyState::Empty))?;
            Ok(())
        } else {
            bail!("User {addr} is not actually connected")
        }
    }

    async fn handle_user_reliable_rpc(
        &mut self,
        user_rpc_message: UserReliableRPCMessage,
    ) -> anyhow::Result<()> {
        let user = self
            .connected_user_list
            .get(&user_rpc_message.send_addr)
            .ok_or_else(|| anyhow!("user not found"))?;

        let lobby_id = user.lobby_id;
        /*
        info!(
            "handle_user_rpc: addr={} lobby_id={:?} message={:?}",
            user_rpc_message.send_addr, lobby_id, user_rpc_message.message
        );
         */
        if let Some(lobby_id) = lobby_id
            && let Some(lobby_entry) = self.lobby_list.remove(&lobby_id)
        {
            match lobby_entry {
                LobbyEntry::Waiting(lobby) => {
                    // ignore most messages while waiting
                    /*
                    info!(
                        "lobby {lobby_id}: waiting; ignoring message from {}",
                        user_rpc_message.send_addr
                    );
                    */
                    self.lobby_list.insert(lobby_id, LobbyEntry::Waiting(lobby));
                }

                LobbyEntry::Running(lobby) => match user_rpc_message.message {
                    ReliableRpcClientMessage::TurnInput(input) => {
                        info!(
                            "Lobby input: {input:?} sent from {:?}",
                            user_rpc_message.send_addr
                        );
                        info!("Lobby session: {lobby:?}");

                        let current_state = lobby
                            .send_rps_input(user_rpc_message.send_addr, input)
                            .await;
                        if let RPSGameState::Win { state: _, .. } = current_state.clone() {
                            self.lobby_list.insert(
                                lobby_id,
                                LobbyEntry::Finished(FinishedLobbySession {
                                    lobby_session: lobby,
                                    left_side_continue: None,
                                    right_side_continue: None,
                                }),
                            );
                        } else {
                            self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
                        }
                    }
                    _ => {
                        info!(
                            "lobby {lobby_id}: running; non-game input from {} ignored",
                            user_rpc_message.send_addr
                        );
                        self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
                    }
                },

                LobbyEntry::Finished(mut finished) => {
                    match user_rpc_message.message {
                        ReliableRpcClientMessage::ContinueRound(vote) => {
                            let side = finished
                                .lobby_session
                                .get_player_side(user_rpc_message.send_addr)
                                .await?;

                            match side {
                                PlayerSide::Left => finished.left_side_continue = Some(vote),
                                PlayerSide::Right => finished.right_side_continue = Some(vote),
                            }

                            info!("Finished lobby {lobby_id:?}");

                            match (
                                finished.left_side_continue.clone(),
                                finished.right_side_continue.clone(),
                            ) {
                                (Some(YesOrNo::Yes), Some(YesOrNo::Yes)) => {
                                    finished.lobby_session.reset_lobby().await;
                                    self.lobby_list.insert(
                                        lobby_id,
                                        LobbyEntry::Running(finished.lobby_session),
                                    );
                                }

                                (Some(YesOrNo::No), Some(YesOrNo::No)) => {
                                    self.remove_connected_user_from_lobby(
                                        finished
                                            .lobby_session
                                            .get_left()
                                            .await
                                            .expect("Should have both players"),
                                    )?;
                                    self.remove_connected_user_from_lobby(
                                        finished
                                            .lobby_session
                                            .get_right()
                                            .await
                                            .expect("Should have both players"),
                                    )?;
                                    // delete lobby entirely
                                    info!(
                                        "Finished Lobby {lobby_id} is now destroyed, both players rejected continuing to play"
                                    );
                                }

                                (Some(YesOrNo::No), Some(YesOrNo::Yes)) => {
                                    let leaving = finished
                                        .lobby_session
                                        .get_left()
                                        .await
                                        .expect("Should have both players in here");
                                    let (_, state) =
                                        finished.lobby_session.remove_player(leaving).await?;
                                    assert_eq!(state, LobbyState::Waiting);

                                    self.remove_connected_user_from_lobby(leaving)?;

                                    finished.lobby_session.reset_lobby().await;
                                    self.lobby_list.insert(
                                        lobby_id,
                                        LobbyEntry::Waiting(finished.lobby_session),
                                    );
                                }

                                (Some(YesOrNo::Yes), Some(YesOrNo::No)) => {
                                    let leaving = finished
                                        .lobby_session
                                        .get_right()
                                        .await
                                        .expect("Should have both players in here");
                                    let (_, state) =
                                        finished.lobby_session.remove_player(leaving).await?;
                                    assert_eq!(state, LobbyState::Waiting);

                                    self.remove_connected_user_from_lobby(leaving)?;

                                    finished.lobby_session.reset_lobby().await;
                                    self.lobby_list.insert(
                                        lobby_id,
                                        LobbyEntry::Waiting(finished.lobby_session),
                                    );
                                }

                                _ => {
                                    self.lobby_list
                                        .insert(lobby_id, LobbyEntry::Finished(finished));
                                }
                            }
                        }
                        _ => {
                            // ignore other messages now that we're finished
                            info!(
                                "lobby {lobby_id}: finished; ignoring message from {}",
                                user_rpc_message.send_addr
                            );
                            self.lobby_list
                                .insert(lobby_id, LobbyEntry::Finished(finished));
                        }
                    }
                }
            }
        } else {
            if user_rpc_message.message == ReliableRpcClientMessage::JoinServer {
                info!(
                    "addr {} has no lobby yet; processing JoinServer",
                    user_rpc_message.send_addr
                );
                self.put_connected_user_in_lobby(user_rpc_message.send_addr)
                    .await?;
            } else {
                info!(
                    "addr {} has no lobby and sent non-JoinServer message: {:?}",
                    user_rpc_message.send_addr, user_rpc_message.message
                );
            }
        }
        Ok(())
    }

    async fn handle_user_unreliable_rpc(
        &mut self,
        user_rpc_message: UserUnreliableRPCMessage,
    ) -> anyhow::Result<()> {
        let user = self
            .connected_user_list
            .get(&user_rpc_message.send_addr)
            .ok_or_else(|| anyhow!("user not found"))?;

        let lobby_id = user.lobby_id;
        /*
        info!(
            "handle_user_rpc: addr={} lobby_id={:?} message={:?}",
            user_rpc_message.send_addr, lobby_id, user_rpc_message.message
        );
         */
        if let Some(lobby_id) = lobby_id
            && let Some(lobby_entry) = self.lobby_list.remove(&lobby_id)
        {
            match lobby_entry {
                LobbyEntry::Running(lobby) => {
                    match user_rpc_message.message {
                        UnreliableRpcClientMessage::Input {
                            pending_inputs,
                            client_send_time_ms,
                        } => {
                            /*
                            info!(
                                "Lobby input: {input:?} sent from {:?}",
                                user_rpc_message.send_addr
                            );
                            info!("Lobby session: {lobby:?}");
                            */
                            for PendingMoveInput { input, sequence } in pending_inputs {
                                lobby
                                    .send_move_input(
                                        user_rpc_message.send_addr,
                                        input,
                                        sequence,
                                        client_send_time_ms,
                                    )
                                    .await;
                            }
                        }
                    }
                    self.lobby_list.insert(lobby_id, LobbyEntry::Running(lobby));
                }
                _ => {
                    self.lobby_list.insert(lobby_id, lobby_entry);
                }
            }
        } else {
            warn!("Sending unreliable input to invalid lobby");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerState {
    server_state: Arc<Mutex<ServerStateInner>>,
}

impl ServerState {
    pub fn new() -> ServerState {
        Self {
            server_state: Arc::new(Mutex::new(ServerStateInner::new())),
        }
    }

    pub async fn connect_user(
        &self,
        addr: UserId,
        connection_init_message: ConnectionInitMessage,
        reliable_sender: UserReliableSender,
        unreliable_sender: UserUnreliableSender,
    ) -> anyhow::Result<()> {
        self.server_state.lock().await.connect_user(
            addr,
            connection_init_message,
            reliable_sender,
            unreliable_sender,
        )
    }

    pub async fn disconnect_user(&self, addr: UserId) -> anyhow::Result<()> {
        self.server_state.lock().await.disconnect_user(addr).await
    }

    pub async fn handle_user_reliable_rpc(
        &self,
        user_rpc_message: UserReliableRPCMessage,
    ) -> anyhow::Result<()> {
        self.server_state
            .lock()
            .await
            .handle_user_reliable_rpc(user_rpc_message)
            .await
    }

    #[allow(dead_code)]
    pub async fn check_if_lobby_exists(&self, lobby_id: LobbyId) -> bool {
        self.server_state
            .lock()
            .await
            .check_if_lobby_exists(lobby_id)
    }

    pub async fn handle_user_unreliable_rpc(
        &self,
        user_rpc_message: UserUnreliableRPCMessage,
    ) -> anyhow::Result<()> {
        self.server_state
            .lock()
            .await
            .handle_user_unreliable_rpc(user_rpc_message)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::time::timeout;

    async fn check_if_matches_lobby_state(
        reliable_receiver: &mut UnboundedReceiver<ReliableRpcServerMessage>,
        lobby_state_to_check: LobbyState,
        secs: u64,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);

        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }

            let remaining = deadline - now;
            match timeout(remaining, reliable_receiver.recv()).await {
                Ok(Some(ReliableRpcServerMessage::LobbyState(state))) => {
                    if state == lobby_state_to_check {
                        return true;
                    }
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_elapsed) => break,
            }
        }
        false
    }

    async fn check_if_connected_to_lobby(
        user_addr: UserId,
        player_side: PlayerSide,
        reliable_receiver: &mut UnboundedReceiver<ReliableRpcServerMessage>,
    ) -> anyhow::Result<LobbyId> {
        let connection_init = timeout(Duration::from_secs(1), reliable_receiver.recv()).await?;
        match connection_init {
            Some(ReliableRpcServerMessage::ConnectionInit(user_id, _)) => {
                if user_addr != user_id {
                    bail!("unexpected left user id init: {user_id:?}")
                }
            }
            other => bail!("unexpected left connection init: {other:?}"),
        }

        let lobby_init = timeout(Duration::from_secs(1), reliable_receiver.recv()).await?;
        match lobby_init {
            Some(ReliableRpcServerMessage::LobbyInit(recv_player_side, lobby_id)) => {
                if recv_player_side != player_side {
                    bail!("unexpected player side: {recv_player_side:?}, lobby_id: {lobby_id:?}")
                }
                anyhow::Ok(lobby_id)
            }
            other => bail!("unexpected left lobby init: {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_state_reuses_waiting_lobby_after_disconnect() -> anyhow::Result<()> {
        let server_state = ServerState::new();

        let left_addr = 3001;
        let right_addr = 3002;
        let replacement_left_addr = 3003;
        let replacement_right_addr = 3004;

        let (left_reliable_sender, mut left_reliable_receiver) = mpsc::unbounded_channel();
        let (left_unreliable_sender, left_unreliable_receiver) = mpsc::unbounded_channel();

        // by default the lobby gets created with the left side already filled
        server_state
            .connect_user(
                left_addr,
                ConnectionInitMessage::FirstTime,
                left_reliable_sender,
                left_unreliable_sender,
            )
            .await?;

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinServer,
                send_addr: left_addr,
            })
            .await?;

        let lobby_id =
            check_if_connected_to_lobby(left_addr, PlayerSide::Left, &mut left_reliable_receiver)
                .await?;

        let (right_reliable_sender, mut right_reliable_receiver) = mpsc::unbounded_channel();
        let (right_unreliable_sender, right_unreliable_receiver) = mpsc::unbounded_channel();
        server_state
            .connect_user(
                right_addr,
                ConnectionInitMessage::FirstTime,
                right_reliable_sender,
                right_unreliable_sender,
            )
            .await?;

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinServer,
                send_addr: right_addr,
            })
            .await?;

        assert_eq!(
            lobby_id,
            check_if_connected_to_lobby(
                right_addr,
                PlayerSide::Right,
                &mut right_reliable_receiver
            )
            .await?
        );

        // lobby should now be full
        assert!(
            check_if_matches_lobby_state(&mut left_reliable_receiver, LobbyState::Running, 3).await
        );

        // disconnect left user so now lobby has to fill in left user

        server_state.disconnect_user(left_addr).await?;

        // drop old left receivers
        drop(left_reliable_receiver);
        drop(left_unreliable_receiver);

        // check if right side saw that the lobby is now waiting
        assert!(
            check_if_matches_lobby_state(&mut right_reliable_receiver, LobbyState::Waiting, 1)
                .await
        );

        let (replacement_left_reliable_sender, mut replacement_left_reliable_receiver) =
            mpsc::unbounded_channel();
        let (replacement_left_unreliable_sender, _replacement_left_unreliable_receiver) =
            mpsc::unbounded_channel();
        server_state
            .connect_user(
                replacement_left_addr,
                ConnectionInitMessage::FirstTime,
                replacement_left_reliable_sender,
                replacement_left_unreliable_sender,
            )
            .await?;

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinServer,
                send_addr: replacement_left_addr,
            })
            .await?;

        assert_eq!(
            lobby_id,
            check_if_connected_to_lobby(
                replacement_left_addr,
                PlayerSide::Left,
                &mut replacement_left_reliable_receiver
            )
            .await?
        );

        // disconnect right user so now lobby has to fill in right user

        server_state.disconnect_user(right_addr).await?;

        // drop old right receivers
        drop(right_reliable_receiver);
        drop(right_unreliable_receiver);

        // check if left side saw that the lobby is now waiting
        assert!(
            check_if_matches_lobby_state(
                &mut replacement_left_reliable_receiver,
                LobbyState::Waiting,
                1
            )
            .await
        );

        let (replacement_right_reliable_sender, mut replacement_right_reliable_receiver) =
            mpsc::unbounded_channel();
        let (replacement_right_unreliable_sender, _replacement_right_unreliable_receiver) =
            mpsc::unbounded_channel();
        server_state
            .connect_user(
                replacement_right_addr,
                ConnectionInitMessage::FirstTime,
                replacement_right_reliable_sender,
                replacement_right_unreliable_sender,
            )
            .await?;

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinServer,
                send_addr: replacement_right_addr,
            })
            .await?;

        assert_eq!(
            lobby_id,
            check_if_connected_to_lobby(
                replacement_right_addr,
                PlayerSide::Right,
                &mut replacement_right_reliable_receiver
            )
            .await?
        );

        // disconnect both clients now and check that lobby is now gone
        server_state.disconnect_user(replacement_left_addr).await?;
        server_state.disconnect_user(replacement_right_addr).await?;

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(server_state.check_if_lobby_exists(lobby_id).await, false);

        Ok(())
    }
}
