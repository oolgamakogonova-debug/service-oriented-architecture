// booking-service/internal/handler/booking_handler.go
package handler

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"flight-booking/booking-service/internal/model"
	"flight-booking/booking-service/internal/service"
)

type BookingHandler struct {
	svc *service.BookingService
}

func NewBookingHandler(svc *service.BookingService) *BookingHandler {
	return &BookingHandler{svc: svc}
}

func (h *BookingHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/bookings", h.handleBookings)
	mux.HandleFunc("/api/v1/bookings/", h.handleBookingByID)
	mux.HandleFunc("/api/v1/flights/search", h.handleSearchFlights)
	mux.HandleFunc("/health", h.handleHealth)
}

func (h *BookingHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *BookingHandler) handleBookings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createBooking(w, r)
	case http.MethodGet:
		h.listBookings(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *BookingHandler) handleBookingByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/bookings/")
	parts := strings.Split(path, "/")
	id := parts[0]

	if id == "" {
		writeError(w, http.StatusBadRequest, "booking id is required")
		return
	}

	// Check if this is a cancel request: DELETE /api/v1/bookings/{id}
	// or PATCH /api/v1/bookings/{id}/cancel
	if len(parts) > 1 && parts[1] == "cancel" && r.Method == http.MethodPatch {
		h.cancelBooking(w, r, id)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getBooking(w, r, id)
	case http.MethodDelete:
		h.cancelBooking(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *BookingHandler) createBooking(w http.ResponseWriter, r *http.Request) {
	var req model.CreateBookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	booking, err := h.svc.CreateBooking(r.Context(), &req)
	if err != nil {
		log.Printf("[HANDLER] CreateBooking error: %v", err)
		errMsg := err.Error()

		if strings.Contains(errMsg, "validation:") {
			writeError(w, http.StatusBadRequest, errMsg)
			return
		}
		if strings.Contains(errMsg, "NotFound") || strings.Contains(errMsg, "not found") {
			writeError(w, http.StatusNotFound, "flight not found")
			return
		}
		if strings.Contains(errMsg, "ResourceExhausted") || strings.Contains(errMsg, "not enough seats") {
			writeError(w, http.StatusConflict, "not enough seats available")
			return
		}
		if strings.Contains(errMsg, "FailedPrecondition") {
			writeError(w, http.StatusConflict, "flight is not available for booking")
			return
		}
		if strings.Contains(errMsg, "Unauthenticated") {
			writeError(w, http.StatusInternalServerError, "service authentication error")
			return
		}

		writeError(w, http.StatusInternalServerError, "failed to create booking")
		return
	}

	writeJSON(w, http.StatusCreated, booking)
}

func (h *BookingHandler) getBooking(w http.ResponseWriter, r *http.Request, id string) {
	booking, err := h.svc.GetBooking(r.Context(), id)
	if err != nil {
		if strings.Contains(err.Error(), "not_found") {
			writeError(w, http.StatusNotFound, "booking not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get booking")
		return
	}

	writeJSON(w, http.StatusOK, booking)
}

func (h *BookingHandler) cancelBooking(w http.ResponseWriter, r *http.Request, id string) {
	booking, err := h.svc.CancelBooking(r.Context(), id)
	if err != nil {
		log.Printf("[HANDLER] CancelBooking error: %v", err)
		errMsg := err.Error()

		if strings.Contains(errMsg, "not_found") {
			writeError(w, http.StatusNotFound, "booking not found")
			return
		}
		if strings.Contains(errMsg, "already_cancelled") {
			writeError(w, http.StatusConflict, "booking already cancelled")
			return
		}
		if strings.Contains(errMsg, "NotFound") {
			writeError(w, http.StatusNotFound, "reservation not found in flight service")
			return
		}

		writeError(w, http.StatusInternalServerError, "failed to cancel booking")
		return
	}

	writeJSON(w, http.StatusOK, booking)
}

func (h *BookingHandler) listBookings(w http.ResponseWriter, r *http.Request) {
	bookings, err := h.svc.ListBookings(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list bookings")
		return
	}

	if bookings == nil {
		bookings = []*model.Booking{}
	}

	writeJSON(w, http.StatusOK, bookings)
}

func (h *BookingHandler) handleSearchFlights(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	origin := r.URL.Query().Get("origin")
	destination := r.URL.Query().Get("destination")
	date := r.URL.Query().Get("date")

	flights, err := h.svc.SearchFlights(r.Context(), origin, destination, date)
	if err != nil {
		if strings.Contains(err.Error(), "validation:") {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to search flights")
		return
	}

	if flights == nil {
		flights = []*model.FlightInfo{}
	}

	writeJSON(w, http.StatusOK, flights)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}