package com.example.shop.exception;

import java.util.List;
import java.util.Map;

public class InsufficientStockException extends RuntimeException {
    private final List<Map<String, Object>> insufficientItems;

    public InsufficientStockException(List<Map<String, Object>> insufficientItems) {
        super("Insufficient stock for some products");
        this.insufficientItems = insufficientItems;
    }

    public List<Map<String, Object>> getInsufficientItems() {
        return insufficientItems;
    }
}