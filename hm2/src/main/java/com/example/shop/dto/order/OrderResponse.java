package com.example.shop.dto.order;

import lombok.*;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OrderResponse {
    private UUID id;
    private UUID userId;
    private String status;
    private List<OrderItemResponse> items;
    private Double totalPrice;
    private Double discountedPrice;
    private UUID promoCodeId;
    private Instant createdAt;
    private Instant updatedAt;
}