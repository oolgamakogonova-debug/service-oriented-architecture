// flight-service/internal/service/flight_service.go
package service

import (
	"context"
	"log"
	"time"

	"flight-booking/flight-service/internal/cache"
	"flight-booking/flight-service/internal/model"
	"flight-booking/flight-service/internal/repository"
)

type FlightService struct {
	repo  *repository.FlightRepository
	cache *cache.RedisCache
}

func NewFlightService(repo *repository.FlightRepository, cache *cache.RedisCache) *FlightService {
	return &FlightService{repo: repo, cache: cache}
}

func (s *FlightService) GetFlight(ctx context.Context, id string) (*model.Flight, error) {
	// Cache-Aside: check cache first
	if s.cache != nil {
		cached, err := s.cache.GetFlight(ctx, id)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	flight, err := s.repo.GetFlightByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Write to cache
	if s.cache != nil && flight != nil {
		if cacheErr := s.cache.SetFlight(ctx, flight); cacheErr != nil {
			log.Printf("[SERVICE] failed to cache flight %s: %v", id, cacheErr)
		}
	}

	return flight, nil
}

func (s *FlightService) SearchFlights(ctx context.Context, origin, destination string, date time.Time) ([]*model.Flight, error) {
	// Cache-Aside: check cache first
	if s.cache != nil {
		cached, err := s.cache.GetSearchResults(ctx, origin, destination, date)
		if err == nil && cached != nil {
			return cached, nil
		}
	}

	flights, err := s.repo.SearchFlights(ctx, origin, destination, date)
	if err != nil {
		return nil, err
	}

	// Write to cache
	if s.cache != nil {
		if cacheErr := s.cache.SetSearchResults(ctx, origin, destination, date, flights); cacheErr != nil {
			log.Printf("[SERVICE] failed to cache search results: %v", cacheErr)
		}
	}

	return flights, nil
}

func (s *FlightService) ReserveSeats(ctx context.Context, flightID, bookingID string, seatCount int32) (*model.SeatReservation, float64, error) {
	reservation, totalPrice, err := s.repo.ReserveSeats(ctx, flightID, bookingID, seatCount)
	if err != nil {
		return nil, 0, err
	}

	// Invalidate cache after mutation
	s.invalidateFlightCache(ctx, flightID)

	return reservation, totalPrice, nil
}

func (s *FlightService) ReleaseReservation(ctx context.Context, bookingID string) (*model.SeatReservation, error) {
	reservation, err := s.repo.ReleaseReservation(ctx, bookingID)
	if err != nil {
		return nil, err
	}

	// Invalidate cache after mutation
	s.invalidateFlightCache(ctx, reservation.FlightID)

	return reservation, nil
}

func (s *FlightService) invalidateFlightCache(ctx context.Context, flightID string) {
	if s.cache == nil {
		return
	}

	// Invalidate flight cache
	if err := s.cache.InvalidateFlight(ctx, flightID); err != nil {
		log.Printf("[SERVICE] failed to invalidate flight cache %s: %v", flightID, err)
	}

	// Also invalidate search cache for this flight's route
	flight, err := s.repo.GetFlightByID(ctx, flightID)
	if err == nil && flight != nil {
		if err := s.cache.InvalidateSearchByFlight(ctx, flight); err != nil {
			log.Printf("[SERVICE] failed to invalidate search cache for flight %s: %v", flightID, err)
		}
	}
}