package com.jobscheduler.api.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.api.web.dto.ErrorResponse;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Requires a valid X-API-Key on /api/**. Actuator endpoints stay open for Prometheus. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class ApiKeyFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-API-Key";

    private final List<byte[]> keys;
    private final ObjectMapper mapper;

    public ApiKeyFilter(@Value("${api.keys}") List<String> keys, ObjectMapper mapper) {
        this.keys = keys.stream().map(k -> k.trim().getBytes(StandardCharsets.UTF_8)).toList();
        this.mapper = mapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String key = request.getHeader(HEADER);
        if (key == null || !isKnown(key.getBytes(StandardCharsets.UTF_8))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            mapper.writeValue(response.getOutputStream(),
                    new ErrorResponse("UNAUTHORIZED", "Missing or invalid " + HEADER, MDC.get(TraceIdFilter.MDC_KEY)));
            return;
        }
        chain.doFilter(request, response);
    }

    /** Constant-time compare against every key, so timing does not leak which prefix matched. */
    private boolean isKnown(byte[] candidate) {
        boolean found = false;
        for (byte[] k : keys) {
            found |= MessageDigest.isEqual(k, candidate);
        }
        return found;
    }
}
