ALTER TABLE services ADD COLUMN logo_id INTEGER;
ALTER TABLE services ADD COLUMN logo_version INTEGER;
ALTER TABLE services ADD COLUMN logo_download_data_id INTEGER;

CREATE TABLE IF NOT EXISTS service_logos (
    network_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    logo_id INTEGER NOT NULL,
    logo_type INTEGER NOT NULL,
    logo_version INTEGER NOT NULL,
    download_data_id INTEGER NOT NULL,
    data BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (network_id, service_id, logo_id, logo_type)
);
