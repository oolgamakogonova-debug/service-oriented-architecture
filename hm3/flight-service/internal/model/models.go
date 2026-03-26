// flight-service/internal/model/models.go
package model

import (
	"time"
)

type Flight struct {
	ID                   string
	FlightNumber         string
	Airline              string
	OriginAirport        string
	DestinationAirport   string
	DepartureTime        time.Time
	ArrivalTime          time.Time
	TotalSeats           int32
	AvailableSeats       int32
	PricePerSeat         float64
	Status               string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type SeatReservation struct {
	ID        string
	FlightID  string
	BookingID string
	SeatCount int32
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}