-- name: GetModule :one
SELECT info, data FROM data_broadcast_modules WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? AND size=?;
-- name: GetVersionModules :many
SELECT size, info, data FROM data_broadcast_modules WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=?;
-- name: TouchModule :exec
UPDATE data_broadcast_modules SET last_accessed=? WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? AND size=?;
-- name: SetAllLastAccessed :exec
UPDATE data_broadcast_modules SET last_accessed=?;
-- name: TotalStoredBytes :one
SELECT CAST(COALESCE(SUM(stored_bytes), 0) AS INTEGER) FROM data_broadcast_modules;
-- name: ModuleExists :one
SELECT 1 FROM data_broadcast_modules WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? AND size=?;
-- name: GetStoredBytes :one
SELECT stored_bytes FROM data_broadcast_modules WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? AND size=?;
-- name: WasEvicted :one
SELECT 1 FROM data_broadcast_module_tombstones WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=?;
-- name: GetResources :many
SELECT size, resource_id, content_location, content_type, data FROM data_broadcast_resources WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? ORDER BY CAST(resource_id AS INTEGER), resource_id;
-- name: UpsertModule :exec
INSERT OR REPLACE INTO data_broadcast_modules(channel_type,channel_id,service_id,component_tag,download_id,module_id,version,size,info,data,last_accessed,stored_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
-- name: DeleteResources :exec
DELETE FROM data_broadcast_resources WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? AND size=?;
-- name: InsertResource :exec
INSERT INTO data_broadcast_resources(channel_type,channel_id,service_id,component_tag,download_id,module_id,version,size,resource_id,content_location,content_type,data) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
-- name: DeleteTombstone :exec
DELETE FROM data_broadcast_module_tombstones WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=?;
-- name: OldestExpiredModule :one
SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size FROM data_broadcast_modules WHERE last_accessed < ? ORDER BY last_accessed, channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size LIMIT 1;
-- name: OldestModule :one
SELECT channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size FROM data_broadcast_modules ORDER BY last_accessed, channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size LIMIT 1;
-- name: UpsertTombstone :exec
INSERT OR REPLACE INTO data_broadcast_module_tombstones(channel_type,channel_id,service_id,component_tag,download_id,module_id,version,evicted_at) VALUES(?,?,?,?,?,?,?,?);
-- name: DeleteModule :exec
DELETE FROM data_broadcast_modules WHERE channel_type=? AND channel_id=? AND service_id=? AND component_tag=? AND download_id=? AND module_id=? AND version=? AND size=?;
-- name: TrimTombstones :exec
DELETE FROM data_broadcast_module_tombstones WHERE (channel_type,channel_id,service_id,component_tag,download_id,module_id,version) IN (SELECT channel_type,channel_id,service_id,component_tag,download_id,module_id,version FROM data_broadcast_module_tombstones ORDER BY evicted_at DESC,channel_type,channel_id,service_id,component_tag,download_id,module_id,version LIMIT -1 OFFSET ?);
