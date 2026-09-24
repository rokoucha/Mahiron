-- Programs keep the broadcast event (model.Event) instead of the
-- Mirakurun-shaped JSON columns: the descriptor details move into one JSON
-- column, event, and the service key gains its stream ID. Existing rows are
-- rewritten from the old columns; the old rows carry no stream ID, extended
-- language or audio codec, which stay absent.
ALTER TABLE programs ADD COLUMN stream_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE programs ADD COLUMN event TEXT;

UPDATE programs SET event = json_patch('{}', json_object(
    'genres', CASE WHEN genres IS NULL THEN NULL ELSE (
        SELECT json_group_array(json_object(
            'lv1', json_extract(g.value, '$.Lv1'),
            'lv2', json_extract(g.value, '$.Lv2'),
            'un1', json_extract(g.value, '$.Un1'),
            'un2', json_extract(g.value, '$.Un2')))
        FROM json_each(programs.genres) AS g) END,
    'videos', CASE WHEN video IS NULL THEN NULL ELSE (
        SELECT json_array(json_object(
            'codec', CASE v.stream_content WHEN 1 THEN 'mpeg2' WHEN 5 THEN 'h264' WHEN 9 THEN 'h265' ELSE '' END,
            'resolution', coalesce(v.resolution, ''),
            'aspect', CASE WHEN v.resolution IS NULL THEN ''
                ELSE CASE v.component_type & 15
                    WHEN 1 THEN '4:3' WHEN 2 THEN '16:9-pan-vector'
                    WHEN 3 THEN '16:9-no-pan-vector' WHEN 4 THEN '16:9-over' ELSE '' END END,
            'progressive', CASE WHEN v.resolution IN ('480p', '720p', '1080p', '2160p', '4320p')
                THEN json('true') ELSE json('false') END))
        FROM (SELECT
                json_extract(programs.video, '$.StreamContent') AS stream_content,
                json_extract(programs.video, '$.ComponentType') AS component_type,
                CASE
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 1 AND 4 THEN '480i'
                    WHEN json_extract(programs.video, '$.ComponentType') = 131 THEN '4320p'
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 145 AND 148 THEN '2160p'
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 161 AND 164 THEN '480p'
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 177 AND 180 THEN '1080i'
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 193 AND 196 THEN '720p'
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 209 AND 212 THEN '240p'
                    WHEN json_extract(programs.video, '$.ComponentType') BETWEEN 225 AND 228 THEN '1080p'
                END AS resolution) AS v) END,
    'audios', CASE WHEN audios IS NULL THEN NULL ELSE (
        SELECT json_group_array(json_object(
            'tag', coalesce(json_extract(a.value, '$.ComponentTag'), 0),
            'componentType', coalesce(json_extract(a.value, '$.ComponentType'), 0),
            'main', CASE WHEN json_extract(a.value, '$.IsMain') THEN json('true') ELSE json('false') END,
            'samplingHz', coalesce(json_extract(a.value, '$.SamplingRate'), 0),
            'languages', coalesce(json_extract(a.value, '$.Langs'), json('[]'))))
        FROM json_each(programs.audios) AS a) END,
    'extended', CASE WHEN extended IS NULL THEN NULL ELSE json_array(json_object(
        'items', (
            SELECT json_group_array(json_object('name', e.key, 'text', e.value))
            FROM json_each(programs.extended) AS e))) END,
    'related', CASE WHEN related_items IS NULL THEN NULL ELSE (
        SELECT json_group_array(json_object(
            'groupType', CASE json_extract(r.value, '$.Type')
                WHEN 'shared' THEN 1 WHEN 'relay' THEN 2 WHEN 'movement' THEN 3 ELSE 0 END,
            'networkId', coalesce(json_extract(r.value, '$.NetworkID'), 0),
            'streamId', coalesce(json_extract(r.value, '$.TransportStreamID'), 0),
            'serviceId', json_extract(r.value, '$.ServiceID'),
            'eventId', json_extract(r.value, '$.EventID')))
        FROM json_each(programs.related_items) AS r) END,
    'series', CASE WHEN series IS NULL THEN NULL ELSE json_object(
        'id', json_extract(series, '$.ID'),
        'repeat', json_extract(series, '$.Repeat'),
        'pattern', CASE WHEN json_extract(series, '$.Pattern') < 0 THEN NULL ELSE json_extract(series, '$.Pattern') END,
        'expiresAt', json_extract(series, '$.ExpiresAt'),
        'episode', json_extract(series, '$.Episode'),
        'lastEpisode', json_extract(series, '$.LastEpisode'),
        'name', json_extract(series, '$.Name')) END
));

ALTER TABLE programs DROP COLUMN genres;
ALTER TABLE programs DROP COLUMN video;
ALTER TABLE programs DROP COLUMN audios;
ALTER TABLE programs DROP COLUMN extended;
ALTER TABLE programs DROP COLUMN related_items;
ALTER TABLE programs DROP COLUMN series;
