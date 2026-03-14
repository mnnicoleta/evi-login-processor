-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS login_tracker_db;

-- Create table login_tracking_result in that schema
CREATE TABLE IF NOT EXISTS login_tracker_db.login_tracking_result (
    login_result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,
    username VARCHAR(255),
    client VARCHAR(255),
    timestamp TIMESTAMP,
    message_id UUID,
    customer_ip VARCHAR(50),
    request_result VARCHAR(50)
);