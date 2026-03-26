// booking-service/cmd/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"

	"flight-booking/booking-service/internal/grpcclient"
	"flight-booking/booking-service/internal/handler"
	"flight-booking/booking-service/internal/repository"
	"flight-booking/booking-service/internal/service"
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

	// gRPC client to Flight Service
	flightAddr := os.Getenv("FLIGHT_SERVICE_ADDR")
	if flightAddr == "" {
		flightAddr = "localhost:50051"
	}
	apiKey := os.Getenv("FLIGHT_SERVICE_API_KEY")

	flightClient, err := grpcclient.NewFlightClient(flightAddr, apiKey)
	if err != nil {
		log.Fatalf("Failed to create flight client: %v", err)
	}
	defer flightClient.Close()
	log.Printf("Connected to Flight Service at %s", flightAddr)

	// Service layer
	repo := repository.NewBookingRepository(pool)
	svc := service.NewBookingService(repo, flightClient)
	h := handler.NewBookingHandler(svc)

	// HTTP server
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	log.Printf("Booking Service HTTP server listening on :%s", httpPort)
	if err := http.ListenAndServe(":"+httpPort, mux); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}