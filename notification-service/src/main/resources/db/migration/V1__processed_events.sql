-- Kafka delivers at least once; this table makes email sending effectively once per event.
CREATE TABLE processed_events (
    event_key    VARCHAR(200) PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
