FROM golang:1.24-alpine AS builder

RUN apk add --no-cache protobuf protobuf-dev git make

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /app

COPY proto/ proto/
RUN mkdir -p gen/flight
RUN protoc --go_out=gen --go_opt=paths=source_relative \
    --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
    proto/flight/flight.proto

RUN cd gen && \
    go mod init flight-booking/gen && \
    go get google.golang.org/grpc@latest && \
    go get google.golang.org/protobuf@latest && \
    go mod tidy

COPY flight-service/go.mod flight-service/go.sum ./flight-service/
RUN cd flight-service && \
    sed -i 's|replace flight-booking/gen => ../gen|replace flight-booking/gen => /app/gen|g' go.mod && \
    go mod download

COPY flight-service/ flight-service/
RUN cd flight-service && \
    sed -i 's|replace flight-booking/gen => ../gen|replace flight-booking/gen => /app/gen|g' go.mod && \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -o /flight-service ./cmd/main.go

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /flight-service /flight-service
COPY --from=builder /app/flight-service/migrations /migrations
ENTRYPOINT ["/flight-service"]