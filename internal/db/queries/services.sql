-- name: ListServices :many
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id;

-- name: GetServiceByID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.id = ?;

-- name: GetServiceByItemID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.network_id * 100000 + s.service_id = ?;

-- name: GetServiceByNetworkServiceID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.network_id = ? AND s.service_id = ?;

-- name: GetServicesByChannel :many
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.channel_type = ? AND s.channel_id = ?;

-- name: GetServiceByChannelAndID :one
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.channel_type = sqlc.arg(channel_type)
  AND s.channel_id = sqlc.arg(channel_id)
  AND s.id = sqlc.arg(id)
UNION ALL
SELECT s.id, s.service_id, s.network_id, s.transport_stream_id, s.name, s.type,
       s.logo_id, s.logo_version, s.logo_download_data_id, EXISTS (
         SELECT 1 FROM service_logos l
         WHERE l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
           AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
       ) AS has_logo_data,
       s.remote_control_key_id, s.channel_type, s.channel_id,
       epg.last_attempt_at, epg.last_success_at, epg.last_error
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id
WHERE s.channel_type = sqlc.arg(channel_type)
  AND s.channel_id = sqlc.arg(channel_id)
  AND s.id != sqlc.arg(id)
  AND s.network_id * 100000 + s.service_id = sqlc.arg(item_id)
LIMIT 1;

-- name: CountServices :one
SELECT COUNT(*) FROM services;

-- name: GetEPGSummary :one
SELECT COUNT(CASE
         WHEN epg.last_success_at IS NULL
           OR sqlc.arg(now) - epg.last_success_at > sqlc.arg(stale_after)
         THEN 1
       END) AS stale,
       COUNT(CASE
         WHEN epg.last_error IS NOT NULL AND epg.last_error != ''
         THEN 1
       END) AS failed,
       MAX(epg.last_success_at) AS last_success_at
FROM services s
LEFT JOIN epg_service_status epg
  ON epg.network_id = s.network_id AND epg.service_id = s.service_id;

-- name: DeleteServicesByChannel :exec
DELETE FROM services WHERE channel_type = ? AND channel_id = ?;

-- name: UpsertService :exec
INSERT INTO services (id, service_id, network_id, transport_stream_id, name, type, logo_id, logo_version, logo_download_data_id, remote_control_key_id, channel_type, channel_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  service_id=excluded.service_id,
  network_id=excluded.network_id,
  transport_stream_id=excluded.transport_stream_id,
  name=excluded.name,
  type=excluded.type,
  logo_id=excluded.logo_id,
  logo_version=excluded.logo_version,
  logo_download_data_id=excluded.logo_download_data_id,
  remote_control_key_id=excluded.remote_control_key_id,
  channel_type=excluded.channel_type,
  channel_id=excluded.channel_id;

-- name: UpsertServiceLogo :exec
INSERT INTO service_logos (network_id, service_id, logo_id, logo_type, logo_version, download_data_id, data, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(network_id, service_id, logo_id, logo_type) DO UPDATE SET
  logo_version=excluded.logo_version,
  download_data_id=excluded.download_data_id,
  data=excluded.data,
  updated_at=excluded.updated_at;

-- name: DeleteServiceLogo :exec
DELETE FROM service_logos
WHERE network_id = ?
  AND service_id = ?
  AND logo_id = ?
  AND logo_type = ?
  AND logo_version = ?
  AND download_data_id = ?;

-- name: DeleteStaleServiceLogosForService :exec
DELETE FROM service_logos
WHERE network_id = ?
  AND service_id = ?
  AND (logo_id != ? OR logo_version != ? OR download_data_id != ?);

-- name: DeleteServiceLogosForService :exec
DELETE FROM service_logos
WHERE network_id = ? AND service_id = ?;

-- name: GetLogoByServiceItemID :one
SELECT l.data
FROM services s
JOIN service_logos l
  ON l.network_id = s.network_id AND l.service_id = s.service_id AND l.logo_id = s.logo_id
  AND l.logo_version = s.logo_version AND l.download_data_id = s.logo_download_data_id
WHERE s.network_id * 100000 + s.service_id = ?
ORDER BY CASE l.logo_type
  WHEN 5 THEN 0
  WHEN 3 THEN 1
  WHEN 4 THEN 2
  WHEN 2 THEN 3
  WHEN 0 THEN 4
  WHEN 1 THEN 5
  ELSE 6
END, l.logo_type
LIMIT 1;

-- name: KnownLogoTargets :many
SELECT s.network_id, s.service_id, s.transport_stream_id, s.channel_type, s.channel_id, s.logo_id, s.logo_version, s.logo_download_data_id
FROM services s
WHERE s.logo_id IS NOT NULL AND s.logo_id >= 0
  AND s.logo_version IS NOT NULL
  AND s.logo_download_data_id IS NOT NULL
ORDER BY s.channel_type, s.channel_id, s.network_id, s.service_id;

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
