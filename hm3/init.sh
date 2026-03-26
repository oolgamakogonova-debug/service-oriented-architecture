#!/bin/bash
set -e

echo "=== Creating gen module ==="
mkdir -p gen/flight

go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

export PATH="$PATH:$(go env GOPATH)/bin"

protoc --go_out=gen --go_opt=paths=source_relative \
    --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
    proto/flight/flight.proto

cd gen
cat > go.mod << 'EOGEN'
module flight-booking/gen

go 1.22

require (
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.33.0
)
EOGEN
go mod tidy
cd ..

echo "=== Flight Service deps ==="
cd flight-service
go mod tidy
cd ..

echo "=== Booking Service deps ==="
cd booking-service
go mod tidy
cd ..

echo "=== Done! Run: docker-compose up --build ==="
