package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"flight-booking/booking-service/internal/grpcclient"
	"flight-booking/booking-service/internal/model"
	"flight-booking/booking-service/internal/repository"
)

type BookingService struct {
	repo         *repository.BookingRepository
	flightClient *grpcclient.FlightClient
}

func NewBookingService(repo *repository.BookingRepository, flightClient *grpcclient.FlightClient) *BookingService {
	return &BookingService{
		repo:         repo,
		flightClient: flightClient,
	}
}

func (s *BookingService) CreateBooking(ctx context.Context, req *model.CreateBookingRequest) (*model.Booking, error) {
	if req.UserID == "" {
		return nil, fmt.Errorf("validation: user_id is required")
	}
	if req.PassengerName == "" {
		return nil, fmt.Errorf("validation: passenger_name is required")
	}
	if req.PassengerEmail == "" {
		return nil, fmt.Errorf("validation: passenger_email is required")
	}
	if req.FlightID == "" {
		return nil, fmt.Errorf("validation: flight_id is required")
	}
	if req.SeatCount <= 0 {
		return nil, fmt.Errorf("validation: seat_count must be positive")
	}

	log.Printf("[BOOKING] Step 1: GetFlight id=%s", req.FlightID)
	_, err := s.flightClient.GetFlight(ctx, req.FlightID)
	if err != nil {
		log.Printf("[BOOKING] GetFlight failed: %v", err)
		return nil, fmt.Errorf("flight_error: %w", err)
	}

	bookingID := uuid.New().String()

	log.Printf("[BOOKING] Step 2: ReserveSeats flight=%s booking=%s seats=%d", req.FlightID, bookingID, req.SeatCount)
	totalPrice, err := s.flightClient.ReserveSeats(ctx, req.FlightID, bookingID, req.SeatCount)
	if err != nil {
		log.Printf("[BOOKING] ReserveSeats failed: %v", err)
		return nil, fmt.Errorf("reserve_error: %w", err)
	}

	log.Printf("[BOOKING] Step 3: Creating booking record, totalPrice=%.2f", totalPrice)

	now := time.Now().UTC()

	booking := &model.Booking{
		ID:             bookingID,
		UserID:         req.UserID,
		PassengerName:  req.PassengerName,
		PassengerEmail: req.PassengerEmail,
		FlightID:       req.FlightID,
		SeatCount:      req.SeatCount,
		TotalPrice:     totalPrice,
		Status:         "CONFIRMED",
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	err = s.repo.Create(ctx, booking)
	if err != nil {
		log.Printf("[BOOKING] DB create failed: %v, releasing reservation", err)
		if releaseErr := s.flightClient.ReleaseReservation(ctx, bookingID); releaseErr != nil {
			log.Printf("[BOOKING] CRITICAL: failed to release reservation after DB error: %v", releaseErr)
		}
		return nil, fmt.Errorf("booking_create_error: %w", err)
	}

	created, err := s.repo.GetByID(ctx, booking.ID)
	if err != nil {
		return nil, fmt.Errorf("booking_created_but_fetch_failed: %w", err)
	}

	log.Printf("[BOOKING] Booking created successfully: id=%s", booking.ID)
	return created, nil
}

func (s *BookingService) GetBooking(ctx context.Context, id string) (*model.Booking, error) {
	booking, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if booking == nil {
		return nil, fmt.Errorf("not_found: booking %s", id)
	}
	return booking, nil
}

func (s *BookingService) CancelBooking(ctx context.Context, id string) (*model.Booking, error) {
	booking, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if booking == nil {
		return nil, fmt.Errorf("not_found: booking %s", id)
	}
	if booking.Status == "CANCELLED" {
		return nil, fmt.Errorf("already_cancelled: booking %s", id)
	}

	log.Printf("[BOOKING] Cancelling booking %s: releasing reservation", id)
	err = s.flightClient.ReleaseReservation(ctx, id)
	if err != nil {
		log.Printf("[BOOKING] ReleaseReservation failed: %v", err)
		return nil, fmt.Errorf("release_error: %w", err)
	}

	updated, err := s.repo.UpdateStatus(ctx, id, "CANCELLED")
	if err != nil {
		log.Printf("[BOOKING] CRITICAL: reservation released but booking status update failed: %v", err)
		return nil, fmt.Errorf("update_error: %w", err)
	}

	log.Printf("[BOOKING] Booking cancelled successfully: id=%s", id)
	return updated, nil
}

func (s *BookingService) ListBookings(ctx context.Context) ([]*model.Booking, error) {
	return s.repo.ListAll(ctx)
}

func (s *BookingService) SearchFlights(ctx context.Context, origin, destination, dateStr string) ([]*model.FlightInfo, error) {
	if origin == "" || destination == "" || dateStr == "" {
		return nil, fmt.Errorf("validation: origin, destination and date are required")
	}

	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return nil, fmt.Errorf("validation: invalid date format, use YYYY-MM-DD")
	}

	return s.flightClient.SearchFlights(ctx, origin, destination, date)
}