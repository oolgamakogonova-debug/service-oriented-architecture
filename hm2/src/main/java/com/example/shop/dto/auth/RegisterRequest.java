package com.example.shop.dto.auth;

import jakarta.validation.constraints.*;
import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class RegisterRequest {
    @NotBlank @Email @Size(max = 255)
    private String email;

    @NotBlank @Size(min = 8, max = 100)
    private String password;

    @NotBlank @Size(min = 1, max = 255)
    private String name;

    @NotBlank @Pattern(regexp = "BUYER|SELLER")
    private String role;
}