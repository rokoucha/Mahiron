DROP INDEX data_broadcast_modules_last_accessed;
CREATE INDEX data_broadcast_modules_prune ON data_broadcast_modules (
    last_accessed, channel_type, channel_id, service_id, component_tag,
    download_id, module_id, version, size, stored_bytes
);
