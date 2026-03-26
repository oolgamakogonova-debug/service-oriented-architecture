// flight-service/internal/repository/flight_repository.go
package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"flight-booking/flight-service/internal/model"
)

type FlightRepository struct {
	pool *pgxpool.Pool
}

func NewFlightRepository(pool *pgxpool.Pool) *FlightRepository {
	return &FlightRepository{pool: pool}
}

func (r *FlightRepository) GetFlightByID(ctx context.Context, id string) (*model.Flight, error) {
	query := `
		SELECT id, flight_number, airline, origin_airport, destination_airport,
		       departure_time, arrival_time, total_seats, available_seats,
		       price_per_seat, status, created_at, updated_at
		FROM flights WHERE id = $1`

	f := &model.Flight{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&f.ID, &f.FlightNumber, &f.Airline, &f.OriginAirport, &f.DestinationAirport,
		&f.DepartureTime, &f.ArrivalTime, &f.TotalSeats, &f.AvailableSeats,
		&f.PricePerSeat, &f.Status, &f.CreatedAt, &f.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (r *FlightRepository) SearchFlights(ctx context.Context, origin, destination string, date time.Time) ([]*model.Flight, error) {
	query := `
		SELECT id, flight_number, airline, origin_airport, destination_airport,
		       departure_time, arrival_time, total_seats, available_seats,
		       price_per_seat, status, created_at, updated_at
		FROM flights
		WHERE origin_airport = $1
		  AND destination_airport = $2
		  AND departure_time::date = $3::date
		  AND status = 'SCHEDULED'
		ORDER BY departure_time`

	rows, err := r.pool.Query(ctx, query, origin, destination, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var flights []*model.Flight
	for rows.Next() {
		f := &model.Flight{}
		err := rows.Scan(
			&f.ID, &f.FlightNumber, &f.Airline, &f.OriginAirport, &f.DestinationAirport,
			&f.DepartureTime, &f.ArrivalTime, &f.TotalSeats, &f.AvailableSeats,
			&f.PricePerSeat, &f.Status, &f.CreatedAt, &f.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		flights = append(flights, f)
	}
	return flights, nil
}

// ReserveSeats - атомарно резервирует места (SELECT FOR UPDATE + транзакция)
func (r *FlightRepository) ReserveSeats(ctx context.Context, flightID, bookingID string, seatCount int32) (*model.SeatReservation, float64, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// SELECT FOR UPDATE чтобы заблокировать строку рейса
	var availableSeats int32
	var pricePerSeat float64
	var status string
	err = tx.QueryRow(ctx, `
		SELECT available_seats, price_per_seat, status
		FROM flights WHERE id = $1 FOR UPDATE`, flightID).Scan(&availableSeats, &pricePerSeat, &status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, 0, fmt.Errorf("flight_not_found")
		}
		return nil, 0, fmt.Errorf("select flight: %w", err)
	}

	if status != "SCHEDULED" {
		return nil, 0, fmt.Errorf("flight_not_available: status is %s", status)
	}

	if availableSeats < seatCount {
		return nil, 0, fmt.Errorf("not_enough_seats: available=%d, requested=%d", availableSeats, seatCount)
	}

	// Уменьшаем available_seats
	_, err = tx.Exec(ctx, `
		UPDATE flights SET available_seats = available_seats - $1, updated_at = now()
		WHERE id = $2`, seatCount, flightID)
	if err != nil {
		return nil, 0, fmt.Errorf("update seats: %w", err)
	}

	// Создаём резервацию
	reservation := &model.SeatReservation{}
	err = tx.QueryRow(ctx, `
		INSERT INTO seat_reservations (flight_id, booking_id, seat_count, status)
		VALUES ($1, $2, $3, 'ACTIVE')
		RETURNING id, flight_id, booking_id, seat_count, status, created_at, updated_at`,
		flightID, bookingID, seatCount).Scan(
		&reservation.ID, &reservation.FlightID, &reservation.BookingID,
		&reservation.SeatCount, &reservation.Status,
		&reservation.CreatedAt, &reservation.UpdatedAt,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("insert reservation: %w", err)
	}

	totalPrice := float64(seatCount) * pricePerSeat

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, fmt.Errorf("commit: %w", err)
	}

	return reservation, totalPrice, nil
}

// ReleaseReservation - возврат мест в одной транзакции
func (r *FlightRepository) ReleaseReservation(ctx context.Context, bookingID string) (*model.SeatReservation, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Находим резервацию
	var reservation model.SeatReservation
	err = tx.QueryRow(ctx, `
		SELECT id, flight_id, booking_id, seat_count, status, created_at, updated_at
		FROM seat_reservations WHERE booking_id = $1 FOR UPDATE`, bookingID).Scan(
		&reservation.ID, &reservation.FlightID, &reservation.BookingID,
		&reservation.SeatCount, &reservation.Status,
		&reservation.CreatedAt, &reservation.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("reservation_not_found")
		}
		return nil, fmt.Errorf("select reservation: %w", err)
	}

	if reservation.Status != "ACTIVE" {
		return nil, fmt.Errorf("reservation_not_active: status is %s", reservation.Status)
	}

	// Возвращаем места
	_, err = tx.Exec(ctx, `
		UPDATE flights SET available_seats = available_seats + $1, updated_at = now()
		WHERE id = $2`, reservation.SeatCount, reservation.FlightID)
	if err != nil {
		return nil, fmt.Errorf("update flight seats: %w", err)
	}

	// Обновляем статус резервации
	err = tx.QueryRow(ctx, `
		UPDATE seat_reservations SET status = 'RELEASED', updated_at = now()
		WHERE id = $1
		RETURNING id, flight_id, booking_id, seat_count, status, created_at, updated_at`,
		reservation.ID).Scan(
		&reservation.ID, &reservation.FlightID, &reservation.BookingID,
		&reservation.SeatCount, &reservation.Status,
		&reservation.CreatedAt, &reservation.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update reservation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return &reservation, nil
}