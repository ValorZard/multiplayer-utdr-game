# Architecture of the Server

The server is a Tokio-based async rust based server.

When a user connects to server, one of two things will happen.
1. The server will search for an empty lobby for the user to join. (It will find the first available lobby and return it)
2. If no empty lobby can be found, the server will generate one.

The server will wait for the lobby to be full before starting the game proper.

When the lobby is created, it has a internal async Rust "actor" that takes in messages from outside.

There are also network RPC messages, with two different versions for Client and Server, as well as two different reliability modes, Unreliable and Reliable.

This results in a combination of four different RPC messages: ReliableClient, UnreliableClient, ReliableServer, UnreliableServer.

Reliable messages HAVE to be sent and received, but there is no such requirement for unreliable messages. 
In fact, if we are missing an unreliable message, we could just interpolate/predict it until we get a new message from the client.

When the lobby is full, it starts the game, which is really just one big sans-io state machine.
The lobby actor might be an async task, but the actual game logic SHOULD NOT BE.

(This holds true on the client side as well, the client should only use async code for networking.)

When a game session/round is finished, the lobby will ask the clients if they want to continue.

If both the players say yes, the game gets restarted with both players.

If only one of the players says yes, the other player leaves, while the connected player waits for a new person to join the lobby.

If both players leave, the lobby gets torn down and a new lobby will need to be created later.