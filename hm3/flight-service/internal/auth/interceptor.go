// flight-service/internal/auth/interceptor.go
package auth

import (
	"context"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func UnaryAuthInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		apiKey := os.Getenv("API_KEY")
		if apiKey == "" {
			log.Println("[AUTH] WARNING: API_KEY not set, skipping auth")
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			log.Printf("[AUTH] UNAUTHENTICATED: no metadata in request to %s", info.FullMethod)
			return nil, status.Errorf(codes.Unauthenticated, "no metadata provided")
		}

		keys := md.Get("x-api-key")
		if len(keys) == 0 {
			log.Printf("[AUTH] UNAUTHENTICATED: no x-api-key in metadata for %s", info.FullMethod)
			return nil, status.Errorf(codes.Unauthenticated, "no api key provided")
		}

		if keys[0] != apiKey {
			log.Printf("[AUTH] UNAUTHENTICATED: invalid api key for %s", info.FullMethod)
			return nil, status.Errorf(codes.Unauthenticated, "invalid api key")
		}

		log.Printf("[AUTH] authenticated request to %s", info.FullMethod)
		return handler(ctx, req)
	}
}