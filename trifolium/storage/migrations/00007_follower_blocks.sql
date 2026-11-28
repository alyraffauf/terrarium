-- +goose Up
CREATE TABLE follower_blocks(target TEXT NOT NULL, actor TEXT NOT NULL, PRIMARY KEY(target,actor));
CREATE VIEW qualifying_follows AS SELECT e.* FROM edges e WHERE NOT EXISTS (SELECT 1 FROM follower_blocks b WHERE b.target=e.target AND b.actor=e.actor);

-- +goose Down
DROP VIEW qualifying_follows;
DROP TABLE follower_blocks;
