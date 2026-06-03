-- Add up migration script here
CREATE TABLE IF NOT EXISTS lobbies
(
    id          BIGSERIAL PRIMARY KEY,
    -- we make these big int instead of big serial to allow these to be nullable
    left_player BIGINT references users(id),
    right_player BIGINT references users(id)
);