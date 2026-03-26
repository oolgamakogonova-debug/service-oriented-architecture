package com.example.shop.dto.order;

import lombok.*;
import java.util.UUID;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OrderItemResponse {
    private UUID productId;
    private String productName;
    private Integer quantity;
    private Double priceAtOrder;
}