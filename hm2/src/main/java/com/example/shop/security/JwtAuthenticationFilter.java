package com.example.shop.security;

import com.example.shop.dto.ErrorResponse;
import com.example.shop.entity.User;
import com.example.shop.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.ExpiredJwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private final JwtTokenProvider jwtTokenProvider;
    private final UserRepository userRepository;
    private final ObjectMapper objectMapper;

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();

        // Skip auth endpoints
        if (path.startsWith("/api/v1/auth/")) {
            filterChain.doFilter(request, response);
            return;
        }

        // Allow GET /products without auth
        if (request.getMethod().equals("GET") &&
                (path.equals("/api/v1/products") || path.matches("/api/v1/products/[^/]+"))) {
            // try to authenticate if token present, but don't require it
            String token = extractToken(request);
            if (token != null) {
                tryAuthenticate(token, request);
            }
            filterChain.doFilter(request, response);
            return;
        }

        String token = extractToken(request);
        if (token == null) {
            sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "TOKEN_INVALID", "Missing authorization token");
            return;
        }

        try {
            if (!jwtTokenProvider.validateToken(token)) {
                sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "TOKEN_INVALID", "Invalid token");
                return;
            }

            String tokenType = jwtTokenProvider.getTokenType(token);
            if (!"access".equals(tokenType)) {
                sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "TOKEN_INVALID", "Not an access token");
                return;
            }

            tryAuthenticate(token, request);

        } catch (ExpiredJwtException e) {
            sendError(response, HttpServletResponse.SC_UNAUTHORIZED, "TOKEN_EXPIRED", "Token has expired");
            return;
        }

        filterChain.doFilter(request, response);
    }

    private void tryAuthenticate(String token, HttpServletRequest request) {
        try {
            UUID userId = jwtTokenProvider.getUserIdFromToken(token);
            User user = userRepository.findById(userId).orElse(null);
            if (user != null) {
                UserPrincipal principal = new UserPrincipal(user);
                UsernamePasswordAuthenticationToken auth =
                        new UsernamePasswordAuthenticationToken(principal, null, principal.getAuthorities());
                auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                SecurityContextHolder.getContext().setAuthentication(auth);
            }
        } catch (Exception ignored) {
        }
    }

    private String extractToken(HttpServletRequest request) {
        String header = request.getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return header.substring(7);
        }
        return null;
    }

    private void sendError(HttpServletResponse response, int status, String errorCode, String message)
            throws IOException {
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        ErrorResponse error = ErrorResponse.builder()
                .errorCode(errorCode)
                .message(message)
                .build();
        response.getWriter().write(objectMapper.writeValueAsString(error));
    }
}