package com.example.shop.service;

import com.example.shop.dto.auth.*;
import com.example.shop.entity.User;
import com.example.shop.entity.enums.UserRole;
import com.example.shop.exception.UserAlreadyExistsException;
import com.example.shop.repository.UserRepository;
import com.example.shop.security.JwtTokenProvider;
import io.jsonwebtoken.ExpiredJwtException;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider jwtTokenProvider;

    @Transactional
    public TokenResponse register(RegisterRequest request) {
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new UserAlreadyExistsException(request.getEmail());
        }

        User user = User.builder()
                .email(request.getEmail())
                .password(passwordEncoder.encode(request.getPassword()))
                .name(request.getName())
                .role(UserRole.valueOf(request.getRole()))
                .build();

        user = userRepository.save(user);
        return generateTokens(user);
    }

    public TokenResponse login(LoginRequest request) {
        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new RuntimeException("Invalid credentials"));

        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            throw new RuntimeException("Invalid credentials");
        }

        return generateTokens(user);
    }

    public TokenResponse refresh(RefreshRequest request) {
        String token = request.getRefreshToken();

        try {
            if (!jwtTokenProvider.validateToken(token)) {
                throw new RuntimeException("Invalid refresh token");
            }
        } catch (ExpiredJwtException e) {
            throw new RuntimeException("Refresh token expired");
        }

        String tokenType = jwtTokenProvider.getTokenType(token);
        if (!"refresh".equals(tokenType)) {
            throw new RuntimeException("Not a refresh token");
        }

        var userId = jwtTokenProvider.getUserIdFromToken(token);
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("User not found"));

        return generateTokens(user);
    }

    private TokenResponse generateTokens(User user) {
        return TokenResponse.builder()
                .accessToken(jwtTokenProvider.generateAccessToken(user))
                .refreshToken(jwtTokenProvider.generateRefreshToken(user))
                .expiresIn(jwtTokenProvider.getAccessTokenExpirationMs() / 1000)
                .build();
    }
}