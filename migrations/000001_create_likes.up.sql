CREATE TABLE likes (
    post_id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (post_id, user_id)
);