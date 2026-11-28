-- +goose Up
CREATE TABLE follower_blocks(target TEXT NOT NULL, actor TEXT NOT NULL, PRIMARY KEY(target,actor));

-- +goose Down
DROP TABLE follower_blocks;
