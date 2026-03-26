package com.example.shop.dto.product;

import lombok.*;
import java.time.Instant;
import java.util.UUID;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ProductResponse {
    private UUID id;
    private String name;
    private String description;
    private Double price;
    private Integer stock;
    private String category;
    private String status;
    private UUID sellerId;
    private Instant createdAt;
    private Instant updatedAt;
}