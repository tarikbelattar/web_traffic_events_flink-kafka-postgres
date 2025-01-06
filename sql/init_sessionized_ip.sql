-- Create sessionized_events table
CREATE TABLE IF NOT EXISTS sessionized_events (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    urls VARCHAR
);