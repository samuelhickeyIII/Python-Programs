-- Create the GAME_DIMENSION table
CREATE TABLE D_GAME (
    game_hash varchar(64),
    `event` VARCHAR(255),
    `site` VARCHAR(255),
    `round` varchar(16),
    result VARCHAR(8),
    eco VARCHAR(64),
    eco_url VARCHAR(255),
    termination VARCHAR(128),
    link VARCHAR(255),
    mainline TEXT,
    tournament VARCHAR(255),
    variant VARCHAR(255),
    FEN VARCHAR(255),
    setup VARCHAR(255),
    `match` VARCHAR(255)
) ENGINE=columnstore;