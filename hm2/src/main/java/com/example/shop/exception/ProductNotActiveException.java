package com.example.shop.exception;

import java.util.UUID;

public class ProductNotActiveException extends RuntimeException {
    private final UUID productId;

    public ProductNotActiveException(UUID productId) {
        super("Product is not active: " + productId);
        this.productId = productId;
    }

    public UUID getProductId() {
        return productId;
    }
}