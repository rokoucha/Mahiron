CREATE TABLE data_broadcast_snapshots (
    channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
    pmt_section BLOB NOT NULL, stored_at INTEGER NOT NULL,
    PRIMARY KEY (channel_type, channel_id, service_id)
);
CREATE INDEX data_broadcast_snapshots_stored_at ON data_broadcast_snapshots(stored_at);
CREATE TABLE data_broadcast_snapshot_carousels (
    channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, service_id INTEGER NOT NULL,
    component_tag INTEGER NOT NULL, pid INTEGER NOT NULL, dii_section BLOB NOT NULL,
    stored_at INTEGER NOT NULL,
    PRIMARY KEY (channel_type, channel_id, service_id, component_tag)
);
