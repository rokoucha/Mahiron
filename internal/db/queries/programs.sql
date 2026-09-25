-- name: GetProgram :one
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, stream_id, event
FROM programs WHERE id = ?;

-- name: ListProgramsByIDs :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, stream_id, event
FROM programs
WHERE id IN (sqlc.slice('ids'))
ORDER BY start_at, id;

-- name: ListProgramsByServiceFrom :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, stream_id, event
FROM programs
WHERE network_id = ? AND service_id = ? AND start_at >= ?
ORDER BY start_at, id;

-- name: UpsertProgram :exec
INSERT INTO programs (id, event_id, service_id, network_id, stream_id, start_at, duration, is_free,
                      name, description, event)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  event_id=excluded.event_id,
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  stream_id=excluded.stream_id,
  start_at=excluded.start_at,
  duration=excluded.duration,
  is_free=excluded.is_free,
  name=excluded.name,
  description=excluded.description,
  event=excluded.event;

-- name: DeleteProgramsByServiceFrom :exec
DELETE FROM programs WHERE network_id = ? AND service_id = ? AND start_at + duration >= ?;

-- name: DeleteEndedAtBefore :exec
DELETE FROM programs WHERE start_at + duration < ?;

-- name: ListEndedProgramIDsBefore :many
SELECT id FROM programs WHERE start_at + duration < ? ORDER BY id;

-- name: CountPrograms :one
SELECT COUNT(*) FROM programs;
