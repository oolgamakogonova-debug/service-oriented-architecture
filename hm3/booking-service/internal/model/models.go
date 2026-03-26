package model

import "time"

type Booking struct {
	ID             string    `json:"id"`
	UserID         string    `json:"user_id"`
	PassengerName  string    `json:"passenger_name"`
	PassengerEmail string    `json:"passenger_email"`
	FlightID       string    `json:"flight_id"`
	SeatCount      int32     `json:"seat_count"`
	TotalPrice     float64   `json:"total_price"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CreateBookingRequest struct {
	UserID         string `json:"user_id"`
	PassengerName  string `json:"passenger_name"`
	PassengerEmail string `json:"passenger_email"`
	FlightID       string `json:"flight_id"`
	SeatCount      int32  `json:"seat_count"`
}

type FlightInfo struct {
	ID                 string    `json:"id"`
	FlightNumber       string    `json:"flight_number"`
	Airline            string    `json:"airline"`
	OriginAirport      string    `json:"origin_airport"`
	DestinationAirport string    `json:"destination_airport"`
	DepartureTime      time.Time `json:"departure_time"`
	ArrivalTime        time.Time `json:"arrival_time"`
	TotalSeats         int32     `json:"total_seats"`
	AvailableSeats     int32     `json:"available_seats"`
	PricePerSeat       float64   `json:"price_per_seat"`
	Status             string    `json:"status"`
}

type SearchFlightsRequest struct {
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
	Date        string `json:"date"`
}