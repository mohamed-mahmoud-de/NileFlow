-- Seed the monitored corridors
SET search_path TO nileflow, public;

INSERT INTO corridors (corridor_id, name, city, start_lat, start_lon, end_lat, end_lon, distance_km)
VALUES
    ('ring_road',       'Ring Road',               'Cairo',      30.0561, 31.3467, 30.0131, 31.2089, 18.5),
    ('corniche_cairo',  'Corniche El Nil',         'Cairo',      30.0459, 31.2243, 30.0029, 31.2297, 5.2),
    ('october_bridge',  '6th of October Bridge',   'Cairo',      30.0554, 31.2235, 30.0434, 31.2015, 3.8),
    ('salah_salem',     'Salah Salem Road',        'Cairo',      30.0724, 31.2834, 30.0281, 31.2611, 6.1),
    ('july26',          '26th of July Corridor',   'Cairo',      30.0609, 31.2003, 30.0764, 31.1177, 9.7),
    ('alex_corniche',   'Alexandria Corniche',     'Alexandria', 31.2135, 29.8854, 31.2017, 29.9533, 7.3)
ON CONFLICT (corridor_id) DO NOTHING;
