-- Add up migration script here
CREATE TABLE IF NOT EXISTS users
(
    id          BIGSERIAL PRIMARY KEY,
    ip          INET    NOT NULL
);