CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS bookings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(100) NOT NULL,
    passenger_name VARCHAR(100) NOT NULL,
    passenger_email VARCHAR(255) NOT NULL,
    flight_id UUID NOT NULL,
    seat_count INT NOT NULL,
    total_price NUMERIC(12,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'CONFIRMED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_booking_seat_count CHECK (seat_count > 0),
    CONSTRAINT chk_booking_total_price CHECK (total_price > 0),
    CONSTRAINT chk_booking_status CHECK (status IN ('CONFIRMED','CANCELLED'))
);

CREATE INDEX idx_bookings_user_id ON bookings(user_id);
CREATE INDEX idx_bookings_flight ON bookings(flight_id);
CREATE INDEX idx_bookings_status ON bookings(status);
CREATE INDEX idx_bookings_email ON bookings(passenger_email);