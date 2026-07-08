-- Add up migration script here
CREATE TABLE IF NOT EXISTS users
(
    id          BIGINT PRIMARY KEY,
    username          TEXT    NOT NULL
);