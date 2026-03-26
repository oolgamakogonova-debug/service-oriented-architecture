package com.example.shop.exception;

public class OrderOwnershipViolationException extends RuntimeException {
    public OrderOwnershipViolationException() {
        super("Order belongs to another user");
    }
}