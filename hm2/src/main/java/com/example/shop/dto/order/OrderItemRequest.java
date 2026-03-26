package com.example.shop.dto.order;

import jakarta.validation.constraints.*;
import lombok.*;
import java.util.UUID;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OrderItemRequest {

    @NotNull
    private UUID productId;

    @NotNull @Min(1) @Max(999)
    private Integer quantity;
}