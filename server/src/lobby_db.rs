use anyhow::{anyhow, bail};
use rpc::{
    LobbyId, PlayerSide, RPSGameState, ReliableRpcClientMessage, ReliableRpcServerMessage,
    UnreliableRpcClientMessage, UserId, YesOrNo,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::lobby::LobbySessionHandle;

use crate::lobby::{UserReliableSender, UserUnreliableSender};
use rpc::LobbyState;

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
    fn session(&self) -> &LobbySessionHandle {
        match self {
            LobbyEntry::Waiting(s) => s,
            LobbyEntry::Running(s) => s,
            LobbyEntry::Finished(f) => &f.lobby_session,
        }
    }

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
        reliable_sender: UserReliableSender,
        unreliable_sender: UserUnreliableSender,
    ) -> anyhow::Result<()> {
        if let Some(_existing) = self.connected_user_list.get(&addr) {
            bail!("Can't double connect a user to the server");
        }

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
            if user_rpc_message.message == ReliableRpcClientMessage::JoinLobby {
                info!(
                    "addr {} has no lobby yet; processing JoinLobby",
                    user_rpc_message.send_addr
                );
                self.put_connected_user_in_lobby(user_rpc_message.send_addr)
                    .await?;
            } else {
                info!(
                    "addr {} has no lobby and sent non-JoinLobby message: {:?}",
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
                        UnreliableRpcClientMessage::MoveInput { input, sequence } => {
                            /*
                            info!(
                                "Lobby input: {input:?} sent from {:?}",
                                user_rpc_message.send_addr
                            );
                            info!("Lobby session: {lobby:?}");
                            */
                            lobby
                                .send_move_input(user_rpc_message.send_addr, input, sequence)
                                .await;
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
        reliable_sender: UserReliableSender,
        unreliable_sender: UserUnreliableSender,
    ) -> anyhow::Result<()> {
        self.server_state
            .lock()
            .await
            .connect_user(addr, reliable_sender, unreliable_sender)
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
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    #[tokio::test]
    async fn server_state_reuses_waiting_lobby_after_disconnect() {
        let server_state = ServerState::new();

        let left_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3001);
        let right_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3002);
        let replacement_left_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3003);
        let replacement_right_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3004);

        let (left_reliable_sender, mut left_reliable_receiver) = mpsc::unbounded_channel();
        let (left_unreliable_sender, left_unreliable_receiver) = mpsc::unbounded_channel();

        // by default the lobby gets created with the left side already filled
        server_state
            .connect_user(left_addr, left_reliable_sender, left_unreliable_sender)
            .await
            .expect("left connect should succeed");

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinLobby,
                send_addr: left_addr,
            })
            .await
            .expect("left join should succeed");

        let left_init = timeout(Duration::from_secs(1), left_reliable_receiver.recv())
            .await
            .expect("left init should arrive in time");
        let lobby_id = match left_init {
            Some(ReliableRpcServerMessage::LobbyInit(PlayerSide::Left, user_id, lobby_id))
                if user_id == left_addr =>
            {
                lobby_id
            }
            other => panic!("unexpected left lobby init: {other:?}"),
        };

        let (right_reliable_sender, mut right_reliable_receiver) = mpsc::unbounded_channel();
        let (right_unreliable_sender, right_unreliable_receiver) = mpsc::unbounded_channel();
        server_state
            .connect_user(right_addr, right_reliable_sender, right_unreliable_sender)
            .await
            .expect("right connect should succeed");

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinLobby,
                send_addr: right_addr,
            })
            .await
            .expect("right join should succeed");

        assert!(matches!(
            timeout(Duration::from_secs(1), right_reliable_receiver.recv()).await,
            Ok(Some(ReliableRpcServerMessage::LobbyInit(
                PlayerSide::Right,
                user_id,
                received_lobby_id,
            ))) if user_id == right_addr && received_lobby_id == lobby_id
        ));

        // disconnect left user so now lobby has to fill in left user

        server_state
            .disconnect_user(left_addr)
            .await
            .expect("right disconnect should succeed");

        // drop old left receivers
        drop(left_reliable_receiver);
        drop(left_unreliable_receiver);

        // check if right side saw that the lobby is now waiting

        let mut saw_waiting = false;
        for _ in 0..8 {
            match timeout(Duration::from_secs(1), right_reliable_receiver.recv()).await {
                Ok(Some(ReliableRpcServerMessage::LobbyState(LobbyState::Waiting))) => {
                    saw_waiting = true
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_elapsed) => continue,
            }
        }
        assert!(saw_waiting);

        let (replacement_left_reliable_sender, mut replacement_left_reliable_receiver) =
            mpsc::unbounded_channel();
        let (replacement_left_unreliable_sender, replacement_left_unreliable_receiver) =
            mpsc::unbounded_channel();
        server_state
            .connect_user(
                replacement_left_addr,
                replacement_left_reliable_sender,
                replacement_left_unreliable_sender,
            )
            .await
            .expect("replacement connect should succeed");

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinLobby,
                send_addr: replacement_left_addr,
            })
            .await
            .expect("replacement join should succeed");

        assert!(matches!(
            timeout(Duration::from_secs(1), replacement_left_reliable_receiver.recv()).await,
            Ok(Some(ReliableRpcServerMessage::LobbyInit(
                PlayerSide::Left,
                user_id,
                received_lobby_id,
            ))) if user_id == replacement_left_addr && received_lobby_id == lobby_id
        ));

        // disconnect right user so now lobby has to fill in right user

        server_state
            .disconnect_user(right_addr)
            .await
            .expect("right disconnect should succeed");

        // drop old right receivers
        drop(right_reliable_receiver);
        drop(right_unreliable_receiver);

        // check if left side saw that the lobby is now waiting

        let mut saw_waiting = false;
        for _ in 0..8 {
            match timeout(
                Duration::from_secs(1),
                replacement_left_reliable_receiver.recv(),
            )
            .await
            {
                Ok(Some(ReliableRpcServerMessage::LobbyState(LobbyState::Waiting))) => {
                    saw_waiting = true
                }
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(_elapsed) => continue,
            }
        }
        assert!(saw_waiting);

        let (replacement_right_reliable_sender, mut replacement_right_reliable_receiver) =
            mpsc::unbounded_channel();
        let (replacement_right_unreliable_sender, replacement_right_unreliable_receiver) =
            mpsc::unbounded_channel();
        server_state
            .connect_user(
                replacement_right_addr,
                replacement_right_reliable_sender,
                replacement_right_unreliable_sender,
            )
            .await
            .expect("replacement connect should succeed");

        server_state
            .handle_user_reliable_rpc(UserReliableRPCMessage {
                message: ReliableRpcClientMessage::JoinLobby,
                send_addr: replacement_right_addr,
            })
            .await
            .expect("replacement join should succeed");

        assert!(matches!(
            timeout(Duration::from_secs(1), replacement_right_reliable_receiver.recv()).await,
            Ok(Some(ReliableRpcServerMessage::LobbyInit(
                PlayerSide::Right,
                user_id,
                received_lobby_id,
            ))) if user_id == replacement_right_addr && received_lobby_id == lobby_id
        ));

        // disconnect both clients now and check that lobby is now gone
        server_state
            .disconnect_user(replacement_left_addr)
            .await
            .expect("left replacement disconnect should succeed");
        server_state
            .disconnect_user(replacement_right_addr)
            .await
            .expect("right disconnect should succeed");

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(server_state.check_if_lobby_exists(lobby_id).await, false);
    }
}
