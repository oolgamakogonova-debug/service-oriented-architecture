package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"flight-booking/booking-service/internal/model"
)

type BookingRepository struct {
	db *pgxpool.Pool
}

func NewBookingRepository(db *pgxpool.Pool) *BookingRepository {
	return &BookingRepository{db: db}
}

func (r *BookingRepository) Create(ctx context.Context, b *model.Booking) error {
	query := `
		INSERT INTO bookings (
			id,
			user_id,
			flight_id,
			passenger_name,
			passenger_email,
			seat_count,
			total_price,
			status,
			created_at,
			updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`

	_, err := r.db.Exec(ctx, query,
		b.ID,
		b.UserID,
		b.FlightID,
		b.PassengerName,
		b.PassengerEmail,
		b.SeatCount,
		b.TotalPrice,
		b.Status,
		b.CreatedAt,
		b.UpdatedAt,
	)
	return err
}

func (r *BookingRepository) GetByID(ctx context.Context, id string) (*model.Booking, error) {
	query := `
		SELECT id, user_id, flight_id, passenger_name, passenger_email,
		       seat_count, total_price, status, created_at, updated_at
		FROM bookings
		WHERE id = $1
	`

	var b model.Booking
	err := r.db.QueryRow(ctx, query, id).Scan(
		&b.ID,
		&b.UserID,
		&b.FlightID,
		&b.PassengerName,
		&b.PassengerEmail,
		&b.SeatCount,
		&b.TotalPrice,
		&b.Status,
		&b.CreatedAt,
		&b.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &b, nil
}

func (r *BookingRepository) UpdateStatus(ctx context.Context, id string, status string) (*model.Booking, error) {
	query := `
		UPDATE bookings
		SET status = $2,
		    updated_at = NOW()
		WHERE id = $1
	`
	_, err := r.db.Exec(ctx, query, id, status)
	if err != nil {
		return nil, err
	}

	return r.GetByID(ctx, id)
}

func (r *BookingRepository) ListAll(ctx context.Context) ([]*model.Booking, error) {
	query := `
		SELECT id, user_id, flight_id, passenger_name, passenger_email,
		       seat_count, total_price, status, created_at, updated_at
		FROM bookings
		ORDER BY created_at DESC
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bookings []*model.Booking
	for rows.Next() {
		var b model.Booking
		if err := rows.Scan(
			&b.ID,
			&b.UserID,
			&b.FlightID,
			&b.PassengerName,
			&b.PassengerEmail,
			&b.SeatCount,
			&b.TotalPrice,
			&b.Status,
			&b.CreatedAt,
			&b.UpdatedAt,
		); err != nil {
			return nil, err
		}
		bookings = append(bookings, &b)
	}

	return bookings, rows.Err()
}

func (r *BookingRepository) ListByUserID(ctx context.Context, userID string) ([]*model.Booking, error) {
	query := `
		SELECT id, user_id, flight_id, passenger_name, passenger_email,
		       seat_count, total_price, status, created_at, updated_at
		FROM bookings
		WHERE user_id = $1
		ORDER BY created_at DESC
	`

	rows, err := r.db.Query(ctx, query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bookings []*model.Booking
	for rows.Next() {
		var b model.Booking
		if err := rows.Scan(
			&b.ID,
			&b.UserID,
			&b.FlightID,
			&b.PassengerName,
			&b.PassengerEmail,
			&b.SeatCount,
			&b.TotalPrice,
			&b.Status,
			&b.CreatedAt,
			&b.UpdatedAt,
		); err != nil {
			return nil, err
		}
		bookings = append(bookings, &b)
	}

	return bookings, rows.Err()
}