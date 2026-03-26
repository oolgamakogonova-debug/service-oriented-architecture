package com.example.shop.exception;

public class TooManyActiveOrdersException extends RuntimeException {
    public TooManyActiveOrdersException() {
        super("User already has an active order");
    }
}