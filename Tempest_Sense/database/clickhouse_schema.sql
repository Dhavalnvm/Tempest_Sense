-- ClickHouse Schema for Cyclone Tracking System
-- Author: Infra-Pulse Team
-- Purpose: Real-time cyclone tracking and historical analysis

-- Create database
CREATE DATABASE IF NOT EXISTS cyclones;

USE cyclones;

-- Main cyclone positions table (real-time updates)
CREATE TABLE IF NOT EXISTS cyclone_positions (
    id String,
    name String,
    basin String,
    classification String,
    intensity String,
    latitude Float64,
    longitude Float64,
    movement_speed Float32,
    movement_direction Float32,
    central_pressure Float32,
    max_sustained_wind Float32,
    timestamp DateTime,
    data_source String DEFAULT 'NOAA',
    ingestion_time DateTime DEFAULT now(),
    INDEX idx_id id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_timestamp timestamp TYPE minmax GRANULARITY 3
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Cyclone forecast data
CREATE TABLE IF NOT EXISTS cyclone_forecasts (
    id String,
    name String,
    forecast_hour Int32,
    forecast_timestamp DateTime,
    latitude Float64,
    longitude Float64,
    max_wind Float32,
    min_pressure Float32,
    forecast_type String,
    issued_at DateTime,
    ingestion_time DateTime DEFAULT now(),
    INDEX idx_id id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_forecast_timestamp forecast_timestamp TYPE minmax GRANULARITY 3
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(issued_at)
ORDER BY (id, forecast_hour, issued_at)
TTL issued_at + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Cyclone metadata and lifecycle
CREATE TABLE IF NOT EXISTS cyclone_metadata (
    id String,
    name String,
    basin String,
    formation_date DateTime,
    dissipation_date Nullable(DateTime),
    peak_intensity String,
    peak_wind Float32,
    min_pressure Float32,
    total_advisories Int32,
    is_active Bool DEFAULT 1,
    last_updated DateTime DEFAULT now(),
    INDEX idx_id id TYPE bloom_filter GRANULARITY 1
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY id
SETTINGS index_granularity = 8192;

-- Cyclone track history (aggregated view)
CREATE TABLE IF NOT EXISTS cyclone_tracks (
    id String,
    name String,
    positions Array(Tuple(Float64, Float64, DateTime)),
    total_positions Int32,
    track_start DateTime,
    track_end DateTime,
    max_intensity Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (id, track_start)
SETTINGS index_granularity = 8192;

-- Real-time statistics materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS cyclone_stats_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (basin, hour)
AS SELECT
    basin,
    toStartOfHour(timestamp) as hour,
    count() as total_observations,
    uniq(id) as active_cyclones,
    avg(max_sustained_wind) as avg_wind_speed,
    max(max_sustained_wind) as max_wind_speed,
    avg(central_pressure) as avg_pressure
FROM cyclone_positions
GROUP BY basin, hour;

-- Cyclone intensity changes (for trend analysis)
CREATE TABLE IF NOT EXISTS cyclone_intensity_changes (
    id String,
    name String,
    timestamp DateTime,
    previous_wind Float32,
    current_wind Float32,
    wind_change Float32,
    previous_pressure Float32,
    current_pressure Float32,
    pressure_change Float32,
    change_type String,
    ingestion_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, timestamp)
TTL timestamp + INTERVAL 60 DAY
SETTINGS index_granularity = 8192;

-- Create a dictionary for quick lookups
CREATE DICTIONARY IF NOT EXISTS active_cyclones_dict (
    id String,
    name String,
    latitude Float64,
    longitude Float64,
    max_sustained_wind Float32,
    last_update DateTime
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'admin'
    PASSWORD 'admin123'
    DB 'cyclones'
    TABLE 'cyclone_positions'
))
LAYOUT(FLAT())
LIFETIME(60);

-- Indexes for faster queries
-- Already defined in table creation with INDEX clauses

-- Sample queries for testing
-- SELECT * FROM cyclone_positions WHERE id = 'AL012025' ORDER BY timestamp DESC LIMIT 10;
-- SELECT * FROM cyclone_forecasts WHERE id = 'AL012025' AND forecast_hour <= 48 ORDER BY forecast_hour;
-- SELECT basin, active_cyclones, max_wind_speed FROM cyclone_stats_mv WHERE hour >= now() - INTERVAL 24 HOUR;