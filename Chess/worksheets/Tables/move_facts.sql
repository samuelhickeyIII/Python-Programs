-- Create the MOVE_FACTS table with foreign keys to USER_DIMENSION, GAME_DIMENSION, and TIME_DIMENSION
CREATE TABLE MOVE_FACTS (
    user_hash VARCHAR(64),
    game_hash VARCHAR(64),
    time_hash VARCHAR(64),
    piece VARCHAR(8),
    current_position VARCHAR(255),
    mainline_move VARCHAR(128),
    clock VARCHAR(16)
) ENGINE=columnstore;
