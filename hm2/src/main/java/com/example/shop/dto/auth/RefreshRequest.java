package com.example.shop.dto.auth;

import jakarta.validation.constraints.NotBlank;
import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class RefreshRequest {
    @NotBlank
    private String refreshToken;
}