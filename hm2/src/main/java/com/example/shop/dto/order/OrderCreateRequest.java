package com.example.shop.dto.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import lombok.*;
import java.util.List;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class OrderCreateRequest {

    @NotNull @Size(min = 1, max = 50)
    @Valid
    private List<OrderItemRequest> items;

    @Pattern(regexp = "^[A-Z0-9_]{4,20}$")
    private String promoCode;
}