package com.example.shop.filter;

import com.example.shop.security.UserPrincipal;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestLoggingFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    private static final Set<String> MUTATING_METHODS = Set.of("POST", "PUT", "DELETE", "PATCH");
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
                         FilterChain chain) throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String requestId = UUID.randomUUID().toString();
        long startTime = System.currentTimeMillis();

        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request);
        ContentCachingResponseWrapper wrappedResponse = new ContentCachingResponseWrapper(response);

        wrappedResponse.setHeader("X-Request-Id", requestId);

        try {
            chain.doFilter(wrappedRequest, wrappedResponse);
        } finally {
            long duration = System.currentTimeMillis() - startTime;

            Map<String, Object> logEntry = new LinkedHashMap<>();
            logEntry.put("request_id", requestId);
            logEntry.put("method", request.getMethod());
            logEntry.put("endpoint", request.getRequestURI());
            logEntry.put("status_code", wrappedResponse.getStatus());
            logEntry.put("duration_ms", duration);
            logEntry.put("user_id", extractUserId());
            logEntry.put("timestamp", Instant.now().toString());

            if (MUTATING_METHODS.contains(request.getMethod())) {
                String body = new String(
                    wrappedRequest.getContentAsByteArray(), StandardCharsets.UTF_8);
                if (!body.isBlank()) {
                    body = body.replaceAll(
                        "\"password\"\\s*:\\s*\"[^\"]*\"",
                        "\"password\":\"***\"");
                    logEntry.put("request_body", body);
                }
            }

            try {
                log.info(mapper.writeValueAsString(logEntry));
            } catch (Exception e) {
                log.info("request_id={} method={} endpoint={} status={} duration={}ms",
                        requestId, request.getMethod(), request.getRequestURI(),
                        wrappedResponse.getStatus(), duration);
            }

            wrappedResponse.copyBodyToResponse();
        }
    }

    private String extractUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof UserPrincipal principal) {
            return principal.getId().toString();
        }
        return null;
    }
}