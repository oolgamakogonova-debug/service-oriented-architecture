package com.example.shop.dto.product;

import jakarta.validation.constraints.*;
import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ProductUpdateRequest {

    @NotBlank @Size(min = 1, max = 255)
    private String name;

    @Size(max = 4000)
    private String description;

    @NotNull @DecimalMin(value = "0.01")
    private Double price;

    @NotNull @Min(0)
    private Integer stock;

    @NotBlank @Size(min = 1, max = 100)
    private String category;

    @NotBlank @Pattern(regexp = "ACTIVE|INACTIVE|ARCHIVED")
    private String status;
}