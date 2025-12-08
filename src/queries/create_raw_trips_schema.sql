-- Connect to your database
CREATE EXTENSION IF NOT EXISTS postgis;

-- Verify installation
SELECT PostGIS_Version();

CREATE TABLE IF NOT EXISTS  taxi_zones (
    objectid INTEGER PRIMARY KEY,
    shape_area NUMERIC,
    shape_leng NUMERIC,
    zone VARCHAR(255),
    locationid INTEGER UNIQUE,
    borough VARCHAR(100),
    -- PostGIS geometry column for MultiPolygon
    the_geom GEOMETRY(MultiPolygon, 4326),  -- 4326 is WGS84 (lat/lon)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index for better query performance
CREATE INDEX IF NOT EXISTS idx_taxi_zones_geom ON taxi_zones USING GIST(the_geom);



-- Create taxi trips table
CREATE TABLE IF NOT EXISTS taxi_trips (
    -- System fields from Socrata
    id VARCHAR(80) PRIMARY KEY,
    socrata_created_at TIMESTAMP,
    socrata_updated_at TIMESTAMP,
    version VARCHAR(50),
    
    -- License and base information
    hvfhs_license_num VARCHAR(10),
    dispatching_base_num VARCHAR(10),
    originating_base_num VARCHAR(10),
    
    -- Datetime fields
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    
    -- Location IDs with foreign keys
    pulocationid INTEGER REFERENCES taxi_zones(locationid),
    dolocationid INTEGER REFERENCES taxi_zones(locationid),
    
    -- Trip metrics
    trip_miles NUMERIC(10, 3),
    trip_time INTEGER,  -- in seconds
    
    -- Fare breakdown
    base_passenger_fare NUMERIC(10, 3),
    tolls NUMERIC(10, 3),
    bcf NUMERIC(10, 3),  -- Black Car Fund
    sales_tax NUMERIC(10, 3),
    congestion_surcharge NUMERIC(10, 3),
    airport_fee NUMERIC(10, 3),
    tips NUMERIC(10, 3),
    driver_pay NUMERIC(10, 3),
    
    -- Flags
    shared_request_flag VARCHAR(10),
    shared_match_flag VARCHAR(10),
    wav_request_flag VARCHAR(10),
    wav_match_flag BOOLEAN,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_datetime ON taxi_trips(pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_taxi_trips_dropoff_datetime ON taxi_trips(dropoff_datetime);
CREATE INDEX IF NOT EXISTS idx_taxi_trips_pulocationid ON taxi_trips(pulocationid);
CREATE INDEX IF NOT EXISTS idx_taxi_trips_dolocationid ON taxi_trips(dolocationid);
