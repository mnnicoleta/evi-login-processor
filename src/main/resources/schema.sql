-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS login_tracker_db;

-- Create table login_tracking_result in that schema
CREATE TABLE IF NOT EXISTS login_tracker_db.login_tracking_result (
    message_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    username VARCHAR(255) NOT NULL,
    client VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    customer_ip VARCHAR(50),
    request_result VARCHAR(50) NOT NULL
);