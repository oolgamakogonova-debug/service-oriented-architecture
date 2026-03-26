CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS flights (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_number VARCHAR(10) NOT NULL,
    airline VARCHAR(100) NOT NULL,
    origin_airport CHAR(3) NOT NULL,
    destination_airport CHAR(3) NOT NULL,
    departure_time TIMESTAMPTZ NOT NULL,
    departure_date DATE NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL,
    total_seats INT NOT NULL,
    available_seats INT NOT NULL,
    price_per_seat NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_total_seats_positive CHECK (total_seats > 0),
    CONSTRAINT chk_available_seats_non_negative CHECK (available_seats >= 0),
    CONSTRAINT chk_available_lte_total CHECK (available_seats <= total_seats),
    CONSTRAINT chk_price_positive CHECK (price_per_seat > 0),
    CONSTRAINT chk_departure_before_arrival CHECK (departure_time < arrival_time),
    CONSTRAINT chk_flight_status CHECK (status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED')),
    CONSTRAINT uq_flight_number_date UNIQUE (flight_number, departure_date)
);

CREATE TABLE IF NOT EXISTS seat_reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_id UUID NOT NULL REFERENCES flights(id),
    booking_id UUID NOT NULL UNIQUE,
    seat_count INT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_seat_count_positive CHECK (seat_count > 0),
    CONSTRAINT chk_reservation_status CHECK (status IN ('ACTIVE','RELEASED','EXPIRED'))
);

CREATE INDEX idx_flights_search ON flights(origin_airport, destination_airport, departure_time);
CREATE INDEX idx_reservations_flight ON seat_reservations(flight_id);
CREATE INDEX idx_reservations_booking ON seat_reservations(booking_id);

INSERT INTO flights (
    flight_number,
    airline,
    origin_airport,
    destination_airport,
    departure_time,
    departure_date,
    arrival_time,
    total_seats,
    available_seats,
    price_per_seat,
    status
)
VALUES
    ('SU1234', 'Aeroflot', 'VKO', 'LED', '2026-04-01 08:00:00+03', '2026-04-01', '2026-04-01 09:30:00+03', 180, 180, 5500.00, 'SCHEDULED'),
    ('SU1235', 'Aeroflot', 'LED', 'VKO', '2026-04-01 12:00:00+03', '2026-04-01', '2026-04-01 13:30:00+03', 180, 180, 5500.00, 'SCHEDULED'),
    ('S71001', 'S7 Airlines', 'DME', 'OVB', '2026-04-01 10:00:00+03', '2026-04-01', '2026-04-01 14:30:00+03', 160, 160, 12000.00, 'SCHEDULED'),
    ('DP405',  'Pobeda', 'VKO', 'AER', '2026-04-02 06:00:00+03', '2026-04-02', '2026-04-02 08:30:00+03', 189, 189, 3200.00, 'SCHEDULED'),
    ('SU1234', 'Aeroflot', 'VKO', 'LED', '2026-04-02 08:00:00+03', '2026-04-02', '2026-04-02 09:30:00+03', 180, 180, 5800.00, 'SCHEDULED');