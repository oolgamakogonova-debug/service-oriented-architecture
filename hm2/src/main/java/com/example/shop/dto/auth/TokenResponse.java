package com.example.shop.dto.auth;

import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class TokenResponse {
    private String accessToken;
    private String refreshToken;
    private long expiresIn;
}