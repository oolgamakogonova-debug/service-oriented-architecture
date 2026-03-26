package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"flight-booking/flight-service/internal/model"
)

const (
	flightKeyPrefix = "flight:"
	searchKeyPrefix = "search:"
	defaultTTL      = 7 * time.Minute
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr string) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisCache{client: client}
}

func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

// ==================== Flight Cache ====================

func (c *RedisCache) GetFlight(ctx context.Context, id string) (*model.Flight, error) {
	key := flightKeyPrefix + id

	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		log.Printf("[CACHE MISS] flight id=%s", id)
		return nil, nil
	}
	if err != nil {
		log.Printf("[CACHE ERROR] GetFlight id=%s: %v", id, err)
		return nil, err
	}

	var flight model.Flight
	if err := json.Unmarshal(data, &flight); err != nil {
		log.Printf("[CACHE ERROR] unmarshal flight id=%s: %v", id, err)
		return nil, err
	}

	log.Printf("[CACHE HIT] flight id=%s", id)
	return &flight, nil
}

func (c *RedisCache) SetFlight(ctx context.Context, flight *model.Flight) error {
	key := flightKeyPrefix + flight.ID

	data, err := json.Marshal(flight)
	if err != nil {
		return err
	}

	log.Printf("[CACHE SET] flight id=%s ttl=%v", flight.ID, defaultTTL)
	return c.client.Set(ctx, key, data, defaultTTL).Err()
}

func (c *RedisCache) InvalidateFlight(ctx context.Context, id string) error {
	key := flightKeyPrefix + id
	log.Printf("[CACHE INVALIDATE] flight id=%s", id)
	return c.client.Del(ctx, key).Err()
}

// ==================== Search Cache ====================

func (c *RedisCache) GetSearchResults(
	ctx context.Context,
	origin, destination string,
	date time.Time,
) ([]*model.Flight, error) {
	key := c.searchKey(origin, destination, date)

	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		log.Printf("[CACHE MISS] search key=%s", key)
		return nil, nil
	}
	if err != nil {
		log.Printf("[CACHE ERROR] GetSearchResults key=%s: %v", key, err)
		return nil, err
	}

	var flights []*model.Flight
	if err := json.Unmarshal(data, &flights); err != nil {
		log.Printf("[CACHE ERROR] unmarshal search key=%s: %v", key, err)
		return nil, err
	}

	log.Printf("[CACHE HIT] search key=%s count=%d", key, len(flights))
	return flights, nil
}

func (c *RedisCache) SetSearchResults(
	ctx context.Context,
	origin, destination string,
	date time.Time,
	flights []*model.Flight,
) error {
	key := c.searchKey(origin, destination, date)

	data, err := json.Marshal(flights)
	if err != nil {
		return err
	}

	log.Printf("[CACHE SET] search key=%s count=%d ttl=%v", key, len(flights), defaultTTL)
	return c.client.Set(ctx, key, data, defaultTTL).Err()
}

func (c *RedisCache) InvalidateSearchByFlight(ctx context.Context, flight *model.Flight) error {
	key := c.searchKey(flight.OriginAirport, flight.DestinationAirport, flight.DepartureTime)
	log.Printf("[CACHE INVALIDATE] search key=%s", key)
	return c.client.Del(ctx, key).Err()
}

func (c *RedisCache) searchKey(origin, destination string, date time.Time) string {
	return fmt.Sprintf("%s%s:%s:%s", searchKeyPrefix, origin, destination, date.Format("2006-01-02"))
}