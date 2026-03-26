// flight-service/cmd/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	pb "flight-booking/gen/proto/flight"
	"flight-booking/flight-service/internal/auth"
	"flight-booking/flight-service/internal/cache"
	"flight-booking/flight-service/internal/repository"
	"flight-booking/flight-service/internal/server"
	"flight-booking/flight-service/internal/service"
)

func main() {
	ctx := context.Background()

	// DB connection
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_NAME"))

	// Run migrations
	log.Println("Running migrations...")
	m, err := migrate.New("file:///migrations", dbURL)
	if err != nil {
		log.Fatalf("Failed to create migrate instance: %v", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("Failed to run migrations: %v", err)
	}
	log.Println("Migrations completed")

	// DB pool
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping DB: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	var redisCache *cache.RedisCache
	if redisAddr != "" {
		redisCache = cache.NewRedisCache(redisAddr)
		if err := redisCache.Ping(ctx); err != nil {
			log.Printf("WARNING: Redis not available: %v, continuing without cache", err)
			redisCache = nil
		} else {
			log.Println("Connected to Redis")
		}
	}

	// Service layer
	repo := repository.NewFlightRepository(pool)
	svc := service.NewFlightService(repo, redisCache)
	grpcServer := server.NewFlightGRPCServer(svc)

	// gRPC server with auth interceptor
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(auth.UnaryAuthInterceptor()),
	)
	pb.RegisterFlightServiceServer(s, grpcServer)

	log.Printf("Flight Service gRPC server listening on :%s", grpcPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}