-- +migrate Up
ALTER TABLE topic_rooms ADD COLUMN participant_count INT NOT NULL DEFAULT 1 AFTER reply_count;

UPDATE topic_rooms tr
SET participant_count = GREATEST(1, (
  SELECT COUNT(1)
  FROM topic_room_members tm
  WHERE tm.room_id = tr.room_id
));

-- +migrate Down
ALTER TABLE topic_rooms DROP COLUMN participant_count;
