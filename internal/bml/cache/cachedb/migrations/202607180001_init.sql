CREATE TABLE data_broadcast_modules (
    channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
    component_tag INTEGER NOT NULL, download_id INTEGER NOT NULL, module_id INTEGER NOT NULL,
    version INTEGER NOT NULL, size INTEGER NOT NULL, info BLOB NOT NULL, data BLOB NOT NULL,
    last_accessed INTEGER NOT NULL, stored_bytes INTEGER NOT NULL,
    PRIMARY KEY (channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size)
);
CREATE INDEX data_broadcast_modules_last_accessed ON data_broadcast_modules(last_accessed);
CREATE TABLE data_broadcast_module_tombstones (
    channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
    component_tag INTEGER NOT NULL, download_id INTEGER NOT NULL, module_id INTEGER NOT NULL,
    version INTEGER NOT NULL, evicted_at INTEGER NOT NULL,
    PRIMARY KEY (channel_type, channel_id, service_id, component_tag, download_id, module_id, version)
);
CREATE TABLE data_broadcast_resources (
    channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
    component_tag INTEGER NOT NULL, download_id INTEGER NOT NULL, module_id INTEGER NOT NULL,
    version INTEGER NOT NULL, size INTEGER NOT NULL, resource_id TEXT NOT NULL,
    content_location TEXT, content_type TEXT NOT NULL, data BLOB NOT NULL,
    PRIMARY KEY (channel_type, channel_id, service_id, component_tag, download_id, module_id, version, size, resource_id)
);
