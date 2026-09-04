-- name: GetProgram :one
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, genres, video, audios, extended, related_items, series
FROM programs WHERE id = ?;

-- name: ListPrograms :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, genres, video, audios, extended, related_items, series
FROM programs
WHERE (sqlc.arg(id) IS NULL OR id = sqlc.arg(id))
  AND (sqlc.arg(network_id) IS NULL OR network_id = sqlc.arg(network_id))
  AND (sqlc.arg(service_id) IS NULL OR service_id = sqlc.arg(service_id))
  AND (sqlc.arg(event_id) IS NULL OR event_id = sqlc.arg(event_id))
  AND (sqlc.arg(start_at) IS NULL OR start_at + duration >= sqlc.arg(start_at))
  AND (sqlc.arg(end_at) IS NULL OR start_at <= sqlc.arg(end_at))
ORDER BY start_at, id;

-- name: ListProgramsByIDs :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, genres, video, audios, extended, related_items, series
FROM programs
WHERE id IN (sqlc.slice('ids'))
ORDER BY start_at, id;

-- name: ListProgramsByServiceFrom :many
SELECT id, event_id, service_id, network_id, start_at, duration, is_free,
       name, description, genres, video, audios, extended, related_items, series
FROM programs
WHERE network_id = ? AND service_id = ? AND start_at >= ?
ORDER BY start_at, id;

-- name: UpsertProgram :exec
INSERT INTO programs (id, event_id, service_id, network_id, start_at, duration, is_free,
                      name, description, genres, video, audios, extended, related_items, series)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  event_id=excluded.event_id,
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  start_at=excluded.start_at,
  duration=excluded.duration,
  is_free=excluded.is_free,
  name=COALESCE(excluded.name, programs.name),
  description=COALESCE(excluded.description, programs.description),
  genres=COALESCE(excluded.genres, programs.genres),
  video=COALESCE(excluded.video, programs.video),
  audios=COALESCE(excluded.audios, programs.audios),
  extended=COALESCE(excluded.extended, programs.extended),
  related_items=COALESCE(excluded.related_items, programs.related_items),
  series=COALESCE(excluded.series, programs.series);

-- name: DeleteProgramsByServiceFrom :exec
DELETE FROM programs WHERE network_id = ? AND service_id = ? AND start_at + duration >= ?;

-- name: DeleteEndedAtBefore :exec
DELETE FROM programs WHERE start_at + duration < ?;

-- name: ListEndedProgramIDsBefore :many
SELECT id FROM programs WHERE start_at + duration < ? ORDER BY id;

-- name: CountPrograms :one
SELECT COUNT(*) FROM programs;
