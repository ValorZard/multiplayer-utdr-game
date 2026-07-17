This project isn't strictly deterministic, but we do still try to be as deterministic as possible for generating bullet hell segments.

Since we are streaming game state from the server, the clients should be eventually consistent with the server game state, 
but we still want the client and server to be spawning bullets at the same time.

As such, we periodically have the server send the current tick length of it's bullet sequence, and the clients have to try to catch up to that.

It's not an exact science, since the clients also need to factor in the latency of the connection in how far ahead the server is, but it works well enough for now.

Since this is meant to be PvE and NOT PvP, we shouldn't need to worry about syncing things too closely.

If this becomes a problem in a future, there are a few strategies we could do to solve this problem (namely adopting fixed-point numbers.)