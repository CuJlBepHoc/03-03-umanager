BEGIN;

CREATE TABLE IF NOT EXISTS users
(
    id         UUID NOT NULL,
    username   TEXT NOT NULL,
    password   TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT pk_users_idx PRIMARY KEY (id),
    CONSTRAINT users_username_uniq_idx UNIQUE (username)
);

END;