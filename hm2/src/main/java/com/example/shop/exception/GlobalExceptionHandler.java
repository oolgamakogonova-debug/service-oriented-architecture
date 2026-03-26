package com.example.shop.exception;

import com.example.shop.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = new HashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(err ->
                fieldErrors.put(err.getField(), err.getDefaultMessage()));

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.builder()
                        .errorCode("VALIDATION_ERROR")
                        .message("Validation failed")
                        .details(fieldErrors)
                        .build());
    }

    @ExceptionHandler(ProductNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleProductNotFound(ProductNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.builder()
                        .errorCode("PRODUCT_NOT_FOUND")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(OrderNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleOrderNotFound(OrderNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ErrorResponse.builder()
                        .errorCode("ORDER_NOT_FOUND")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(OrderOwnershipViolationException.class)
    public ResponseEntity<ErrorResponse> handleOwnership(OrderOwnershipViolationException ex) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(ErrorResponse.builder()
                        .errorCode("ORDER_OWNERSHIP_VIOLATION")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(InvalidStateTransitionException.class)
    public ResponseEntity<ErrorResponse> handleInvalidState(InvalidStateTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.builder()
                        .errorCode("INVALID_STATE_TRANSITION")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(InsufficientStockException.class)
    public ResponseEntity<ErrorResponse> handleInsufficientStock(InsufficientStockException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.builder()
                        .errorCode("INSUFFICIENT_STOCK")
                        .message(ex.getMessage())
                        .details(ex.getInsufficientItems())
                        .build());
    }

    @ExceptionHandler(ProductNotActiveException.class)
    public ResponseEntity<ErrorResponse> handleProductInactive(ProductNotActiveException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.builder()
                        .errorCode("PRODUCT_INACTIVE")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(TooManyActiveOrdersException.class)
    public ResponseEntity<ErrorResponse> handleActiveOrders(TooManyActiveOrdersException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.builder()
                        .errorCode("ORDER_HAS_ACTIVE")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(OrderRateLimitException.class)
    public ResponseEntity<ErrorResponse> handleRateLimit(OrderRateLimitException ex) {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(ErrorResponse.builder()
                        .errorCode("ORDER_LIMIT_EXCEEDED")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(InvalidPromoCodeException.class)
    public ResponseEntity<ErrorResponse> handlePromoCode(InvalidPromoCodeException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(ErrorResponse.builder()
                        .errorCode(ex.getErrorCode())
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(UserAlreadyExistsException.class)
    public ResponseEntity<ErrorResponse> handleUserExists(UserAlreadyExistsException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(ErrorResponse.builder()
                        .errorCode("USER_ALREADY_EXISTS")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleAccessDenied(AccessDeniedException ex) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(ErrorResponse.builder()
                        .errorCode("ACCESS_DENIED")
                        .message(ex.getMessage())
                        .build());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneral(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.builder()
                        .errorCode("INTERNAL_ERROR")
                        .message("Internal server error: " + ex.getMessage())
                        .build());
    }
}