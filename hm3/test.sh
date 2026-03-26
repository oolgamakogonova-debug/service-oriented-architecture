#!/bin/bash

BASE_URL="http://localhost:8080"

echo "=== Health Check ==="
curl -s $BASE_URL/health | jq .

echo ""
echo "=== Search Flights VKO -> LED on 2026-04-01 ==="
curl -s "$BASE_URL/api/v1/flights/search?origin=VKO&destination=LED&date=2026-04-01" | jq .

echo ""
echo "=== List All Bookings (empty) ==="
curl -s $BASE_URL/api/v1/bookings | jq .

echo ""
echo "=== Create Booking ==="
FLIGHT_ID=$(curl -s "$BASE_URL/api/v1/flights/search?origin=VKO&destination=LED&date=2026-04-01" | jq -r '.[0].id')
echo "Flight ID: $FLIGHT_ID"

BOOKING_RESPONSE=$(curl -s -X POST $BASE_URL/api/v1/bookings \
  -H "Content-Type: application/json" \
  -d "{
    \"passenger_name\": \"Ivan Ivanov\",
    \"passenger_email\": \"ivan@example.com\",
    \"flight_id\": \"$FLIGHT_ID\",
    \"seat_count\": 2
  }")
echo $BOOKING_RESPONSE | jq .

BOOKING_ID=$(echo $BOOKING_RESPONSE | jq -r '.id')
echo "Booking ID: $BOOKING_ID"

echo ""
echo "=== Get Booking ==="
curl -s $BASE_URL/api/v1/bookings/$BOOKING_ID | jq .

echo ""
echo "=== Search Again (seats should decrease) ==="
curl -s "$BASE_URL/api/v1/flights/search?origin=VKO&destination=LED&date=2026-04-01" | jq '.[0] | {available_seats, total_seats}'

echo ""
echo "=== Cancel Booking ==="
curl -s -X DELETE $BASE_URL/api/v1/bookings/$BOOKING_ID | jq .

echo ""
echo "=== Get Cancelled Booking ==="
curl -s $BASE_URL/api/v1/bookings/$BOOKING_ID | jq .

echo ""
echo "=== Search Again (seats should be restored) ==="
curl -s "$BASE_URL/api/v1/flights/search?origin=VKO&destination=LED&date=2026-04-01" | jq '.[0] | {available_seats, total_seats}'

echo ""
echo "=== Try Cancel Again (should fail) ==="
curl -s -X DELETE $BASE_URL/api/v1/bookings/$BOOKING_ID | jq .

echo ""
echo "=== Create Booking on Non-existent Flight ==="
curl -s -X POST $BASE_URL/api/v1/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "passenger_name": "Test",
    "passenger_email": "test@test.com",
    "flight_id": "00000000-0000-0000-0000-000000000000",
    "seat_count": 1
  }' | jq .

echo ""
echo "=== All tests done ==="
