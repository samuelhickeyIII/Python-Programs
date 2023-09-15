-- Create the TIME_DIMENSION table
CREATE TABLE TIME_DIMENSION (
    time_hash VARCHAR(64),
    current_utc_timestamp TIMESTAMP,
    game_start_timestamp TIMESTAMP,
    game_end_timestamp TIMESTAMP,
    row_created_at TIMESTAMP,
    row_updated_at TIMESTAMP
) ENGINE=columnstore;
