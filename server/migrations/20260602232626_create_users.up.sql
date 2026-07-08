-- Add up migration script here
CREATE TABLE IF NOT EXISTS users
(
    id          INTEGER PRIMARY KEY,
    username    TEXT NOT NULL
) STRICT;