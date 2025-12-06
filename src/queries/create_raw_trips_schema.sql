-- Connect to your database
CREATE EXTENSION IF NOT EXISTS postgis;

-- Verify installation
SELECT PostGIS_Version();

CREATE TABLE taxi_zones (
    objectid INTEGER PRIMARY KEY,
    shape_area NUMERIC,
    shape_leng NUMERIC,
    zone VARCHAR(255),
    locationid INTEGER,
    borough VARCHAR(100),
    -- PostGIS geometry column for MultiPolygon
    the_geom GEOMETRY(MultiPolygon, 4326),  -- 4326 is WGS84 (lat/lon)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index for better query performance
CREATE INDEX idx_taxi_zones_geom ON taxi_zones USING GIST(the_geom);
