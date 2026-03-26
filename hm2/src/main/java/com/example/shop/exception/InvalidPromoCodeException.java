package com.example.shop.exception;

public class InvalidPromoCodeException extends RuntimeException {
    private final String errorCode;

    public InvalidPromoCodeException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}