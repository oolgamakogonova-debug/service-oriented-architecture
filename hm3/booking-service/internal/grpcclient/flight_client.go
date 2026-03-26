// booking-service/internal/grpcclient/flight_client.go
package grpcclient

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "flight-booking/gen/proto/flight"
	"flight-booking/booking-service/internal/model"
)

type FlightClient struct {
	client pb.FlightServiceClient
	conn   *grpc.ClientConn
	apiKey string
}

func NewFlightClient(addr, apiKey string) (*FlightClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &FlightClient{
		client: pb.NewFlightServiceClient(conn),
		conn:   conn,
		apiKey: apiKey,
	}, nil
}

func (c *FlightClient) Close() error {
	return c.conn.Close()
}

func (c *FlightClient) authCtx(ctx context.Context) context.Context {
	md := metadata.New(map[string]string{
		"x-api-key": c.apiKey,
	})
	return metadata.NewOutgoingContext(ctx, md)
}

func (c *FlightClient) GetFlight(ctx context.Context, flightID string) (*model.FlightInfo, error) {
	log.Printf("[GRPC CLIENT] GetFlight id=%s", flightID)

	resp, err := c.client.GetFlight(c.authCtx(ctx), &pb.GetFlightRequest{
		FlightId: flightID,
	})
	if err != nil {
		return nil, err
	}

	return protoToFlightInfo(resp.Flight), nil
}

func (c *FlightClient) SearchFlights(ctx context.Context, origin, destination string, date time.Time) ([]*model.FlightInfo, error) {
	log.Printf("[GRPC CLIENT] SearchFlights origin=%s dest=%s date=%s", origin, destination, date.Format("2006-01-02"))

	resp, err := c.client.SearchFlights(c.authCtx(ctx), &pb.SearchFlightsRequest{
		OriginAirport:      origin,
		DestinationAirport: destination,
		DepartureDate:      timestamppb.New(date),
	})
	if err != nil {
		return nil, err
	}

	var flights []*model.FlightInfo
	for _, f := range resp.Flights {
		flights = append(flights, protoToFlightInfo(f))
	}
	return flights, nil
}

func (c *FlightClient) ReserveSeats(ctx context.Context, flightID, bookingID string, seatCount int32) (float64, error) {
	log.Printf("[GRPC CLIENT] ReserveSeats flight=%s booking=%s seats=%d", flightID, bookingID, seatCount)

	resp, err := c.client.ReserveSeats(c.authCtx(ctx), &pb.ReserveSeatsRequest{
		FlightId:  flightID,
		BookingId: bookingID,
		SeatCount: seatCount,
	})
	if err != nil {
		return 0, err
	}

	return resp.TotalPrice, nil
}

func (c *FlightClient) ReleaseReservation(ctx context.Context, bookingID string) error {
	log.Printf("[GRPC CLIENT] ReleaseReservation booking=%s", bookingID)

	_, err := c.client.ReleaseReservation(c.authCtx(ctx), &pb.ReleaseReservationRequest{
		BookingId: bookingID,
	})
	return err
}

func protoToFlightInfo(f *pb.FlightInfo) *model.FlightInfo {
	return &model.FlightInfo{
		ID:                 f.Id,
		FlightNumber:       f.FlightNumber,
		Airline:            f.Airline,
		OriginAirport:      f.OriginAirport,
		DestinationAirport: f.DestinationAirport,
		DepartureTime:      f.DepartureTime.AsTime(),
		ArrivalTime:        f.ArrivalTime.AsTime(),
		TotalSeats:         f.TotalSeats,
		AvailableSeats:     f.AvailableSeats,
		PricePerSeat:       f.PricePerSeat,
		Status:             f.Status.String(),
	}
}