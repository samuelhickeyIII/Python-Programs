-- Create the USER_DIMENSION table
CREATE TABLE D_USER (
    user_hash VARCHAR(64),
    `name` VARCHAR(255),
    elo INT
) ENGINE=columnstore;
