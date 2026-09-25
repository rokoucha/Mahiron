-- Services keep the broadcast service (model.Service): the SDT provider
-- name, running status and free_CA_mode, and the simple logo gain columns,
-- and a service without a remote control key stores NULL instead of 0.
-- SQLite cannot drop NOT NULL in place, so the table is rebuilt. Existing
-- rows carry no provider name, running status, free_CA_mode or simple logo,
-- which stay at their defaults until the next scan; the negative logo_id
-- sentinel becomes NULL.
CREATE TABLE services_new (
    id TEXT PRIMARY KEY,
    service_id INTEGER NOT NULL,
    network_id INTEGER NOT NULL,
    transport_stream_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    provider_name TEXT NOT NULL DEFAULT '',
    type INTEGER NOT NULL,
    running_status INTEGER NOT NULL DEFAULT 0,
    free_ca INTEGER NOT NULL DEFAULT 0,
    eit_schedule_flag INTEGER NOT NULL DEFAULT 1,
    eit_present_following INTEGER NOT NULL DEFAULT 1,
    logo_id INTEGER,
    logo_version INTEGER,
    logo_download_data_id INTEGER,
    simple_logo TEXT,
    remote_control_key_id INTEGER,
    channel_type TEXT NOT NULL,
    channel_id TEXT NOT NULL
);

INSERT INTO services_new (
    id, service_id, network_id, transport_stream_id, name, type,
    eit_schedule_flag, eit_present_following,
    logo_id, logo_version, logo_download_data_id,
    remote_control_key_id, channel_type, channel_id
)
SELECT
    id, service_id, network_id, transport_stream_id, name, type,
    eit_schedule_flag, eit_present_following,
    CASE WHEN logo_id < 0 THEN NULL ELSE logo_id END,
    CASE WHEN logo_id < 0 THEN NULL ELSE logo_version END,
    CASE WHEN logo_id < 0 THEN NULL ELSE logo_download_data_id END,
    NULLIF(remote_control_key_id, 0), channel_type, channel_id
FROM services;

DROP TABLE services;
ALTER TABLE services_new RENAME TO services;
CREATE INDEX idx_services_channel ON services(channel_type, channel_id);
