// flight-service/internal/server/grpc_server.go
package server

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jackc/pgx/v5"

	pb "flight-booking/gen/proto/flight"
	"flight-booking/flight-service/internal/model"
	"flight-booking/flight-service/internal/service"
)

type FlightGRPCServer struct {
	pb.UnimplementedFlightServiceServer
	svc *service.FlightService
}

func NewFlightGRPCServer(svc *service.FlightService) *FlightGRPCServer {
	return &FlightGRPCServer{svc: svc}
}

func (s *FlightGRPCServer) GetFlight(ctx context.Context, req *pb.GetFlightRequest) (*pb.GetFlightResponse, error) {
	if req.FlightId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "flight_id is required")
	}

	flight, err := s.svc.GetFlight(ctx, req.FlightId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, status.Errorf(codes.NotFound, "flight not found: %s", req.FlightId)
		}
		return nil, status.Errorf(codes.Internal, "internal error: %v", err)
	}

	return &pb.GetFlightResponse{
		Flight: flightToProto(flight),
	}, nil
}

func (s *FlightGRPCServer) SearchFlights(ctx context.Context, req *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	if req.OriginAirport == "" || req.DestinationAirport == "" {
		return nil, status.Errorf(codes.InvalidArgument, "origin and destination airports are required")
	}
	if req.DepartureDate == nil {
		return nil, status.Errorf(codes.InvalidArgument, "departure_date is required")
	}

	date := req.DepartureDate.AsTime()
	flights, err := s.svc.SearchFlights(ctx, req.OriginAirport, req.DestinationAirport, date)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "search error: %v", err)
	}

	var protoFlights []*pb.FlightInfo
	for _, f := range flights {
		protoFlights = append(protoFlights, flightToProto(f))
	}

	return &pb.SearchFlightsResponse{
		Flights: protoFlights,
	}, nil
}

func (s *FlightGRPCServer) ReserveSeats(ctx context.Context, req *pb.ReserveSeatsRequest) (*pb.ReserveSeatsResponse, error) {
	if req.FlightId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "flight_id is required")
	}
	if req.BookingId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "booking_id is required")
	}
	if req.SeatCount <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "seat_count must be positive")
	}

	reservation, totalPrice, err := s.svc.ReserveSeats(ctx, req.FlightId, req.BookingId, req.SeatCount)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "flight_not_found") {
			return nil, status.Errorf(codes.NotFound, "flight not found")
		}
		if strings.Contains(errMsg, "not_enough_seats") {
			return nil, status.Errorf(codes.ResourceExhausted, "not enough seats available")
		}
		if strings.Contains(errMsg, "flight_not_available") {
			return nil, status.Errorf(codes.FailedPrecondition, "flight is not available for booking")
		}
		// Duplicate booking_id
		if strings.Contains(errMsg, "duplicate key") || strings.Contains(errMsg, "unique") {
			return nil, status.Errorf(codes.AlreadyExists, "reservation already exists for this booking")
		}
		return nil, status.Errorf(codes.Internal, "reserve error: %v", err)
	}

	return &pb.ReserveSeatsResponse{
		Reservation: reservationToProto(reservation),
		TotalPrice:  totalPrice,
	}, nil
}

func (s *FlightGRPCServer) ReleaseReservation(ctx context.Context, req *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
	if req.BookingId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "booking_id is required")
	}

	reservation, err := s.svc.ReleaseReservation(ctx, req.BookingId)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "reservation_not_found") {
			return nil, status.Errorf(codes.NotFound, "reservation not found for booking")
		}
		if strings.Contains(errMsg, "reservation_not_active") {
			return nil, status.Errorf(codes.FailedPrecondition, "reservation is not active")
		}
		return nil, status.Errorf(codes.Internal, "release error: %v", err)
	}

	return &pb.ReleaseReservationResponse{
		Reservation: reservationToProto(reservation),
	}, nil
}

func flightToProto(f *model.Flight) *pb.FlightInfo {
	return &pb.FlightInfo{
		Id:                 f.ID,
		FlightNumber:       f.FlightNumber,
		Airline:            f.Airline,
		OriginAirport:      strings.TrimSpace(f.OriginAirport),
		DestinationAirport: strings.TrimSpace(f.DestinationAirport),
		DepartureTime:      timestamppb.New(f.DepartureTime),
		ArrivalTime:        timestamppb.New(f.ArrivalTime),
		TotalSeats:         f.TotalSeats,
		AvailableSeats:     f.AvailableSeats,
		PricePerSeat:       f.PricePerSeat,
		Status:             statusToProto(f.Status),
		CreatedAt:          timestamppb.New(f.CreatedAt),
		UpdatedAt:          timestamppb.New(f.UpdatedAt),
	}
}

func reservationToProto(r *model.SeatReservation) *pb.SeatReservationInfo {
	return &pb.SeatReservationInfo{
		Id:        r.ID,
		FlightId:  r.FlightID,
		BookingId: r.BookingID,
		SeatCount: r.SeatCount,
		Status:    reservationStatusToProto(r.Status),
		CreatedAt: timestamppb.New(r.CreatedAt),
		UpdatedAt: timestamppb.New(r.UpdatedAt),
	}
}

func statusToProto(s string) pb.FlightStatus {
	switch strings.TrimSpace(s) {
	case "SCHEDULED":
		return pb.FlightStatus_FLIGHT_STATUS_SCHEDULED
	case "DEPARTED":
		return pb.FlightStatus_FLIGHT_STATUS_DEPARTED
	case "CANCELLED":
		return pb.FlightStatus_FLIGHT_STATUS_CANCELLED
	case "COMPLETED":
		return pb.FlightStatus_FLIGHT_STATUS_COMPLETED
	default:
		return pb.FlightStatus_FLIGHT_STATUS_UNSPECIFIED
	}
}

func reservationStatusToProto(s string) pb.ReservationStatus {
	switch strings.TrimSpace(s) {
	case "ACTIVE":
		return pb.ReservationStatus_RESERVATION_STATUS_ACTIVE
	case "RELEASED":
		return pb.ReservationStatus_RESERVATION_STATUS_RELEASED
	case "EXPIRED":
		return pb.ReservationStatus_RESERVATION_STATUS_EXPIRED
	default:
		return pb.ReservationStatus_RESERVATION_STATUS_UNSPECIFIED
	}
}