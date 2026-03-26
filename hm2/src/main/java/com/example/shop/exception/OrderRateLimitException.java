package com.example.shop.exception;

public class OrderRateLimitException extends RuntimeException {
    public OrderRateLimitException() {
        super("Order creation/update rate limit exceeded");
    }
}