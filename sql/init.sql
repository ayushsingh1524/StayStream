-- StayStream: initialisation DDL
-- Applied automatically by Docker on first run.

CREATE TABLE IF NOT EXISTS booking_metrics_per_minute (
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    city         VARCHAR(100) NOT NULL,
    bookings     INT NOT NULL,
    revenue      NUMERIC(14, 2) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, city)
);

CREATE TABLE IF NOT EXISTS booking_events_dlq (
    id          BIGSERIAL PRIMARY KEY,
    raw_value   TEXT,
    reason      VARCHAR(200),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

