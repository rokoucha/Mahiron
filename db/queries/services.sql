-- name: ListServices :many
SELECT * FROM services;

-- name: GetServiceByID :one
SELECT * FROM services WHERE id = ?;

-- name: GetServicesByChannel :many
SELECT * FROM services WHERE channel_type = ? AND channel_id = ?;

-- name: DeleteServicesByChannel :exec
DELETE FROM services WHERE channel_type = ? AND channel_id = ?;

-- name: UpsertService :exec
INSERT INTO services (id, service_id, network_id, transport_stream_id, name, type, remote_control_key_id, channel_type, channel_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  transport_stream_id=excluded.transport_stream_id,
  name=excluded.name,
  type=excluded.type,
  remote_control_key_id=excluded.remote_control_key_id,
  channel_type=excluded.channel_type,
  channel_id=excluded.channel_id;

-- name: SetEPGAttempt :exec
INSERT INTO epg_service_status (network_id, service_id, last_attempt_at, last_error)
VALUES (?, ?, ?, ?)
ON CONFLICT(network_id, service_id) DO UPDATE SET
  last_attempt_at=excluded.last_attempt_at,
  last_error=excluded.last_error;

-- name: SetEPGSuccess :exec
INSERT INTO epg_service_status (network_id, service_id, last_attempt_at, last_success_at, last_error)
VALUES (?, ?, ?, ?, NULL)
ON CONFLICT(network_id, service_id) DO UPDATE SET
  last_attempt_at=excluded.last_attempt_at,
  last_success_at=excluded.last_success_at,
  last_error=NULL;

-- name: ListEPGStatuses :many
SELECT network_id, service_id, last_attempt_at, last_success_at, last_error
FROM epg_service_status;
