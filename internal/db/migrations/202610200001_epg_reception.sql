ALTER TABLE epg_service_status ADD COLUMN basic_updated_at INTEGER;
ALTER TABLE epg_service_status ADD COLUMN extended_probe_at INTEGER;
ALTER TABLE epg_service_status ADD COLUMN extended_observed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE epg_service_status ADD COLUMN partial_at INTEGER;
