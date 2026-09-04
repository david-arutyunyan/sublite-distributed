package com.sublite.subscription.infrastructure;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Same idea as sublite-core's own CorrelationIdFilter, carried over here
 * for the same reason plus one more: every log line for a request still
 * gets the same id, but now that id is also what
 * SubscriptionPurchaseService puts on the outbox event it writes - so
 * the SAME id shows up again later in billing-service's logs when it
 * consumes that event, tying a whole saga's log lines together across
 * process boundaries, not just one request's.
 *
 * A plain @Component here (unlike sublite-core's, which is wired in
 * explicitly by SecurityConfig) because this service has no Spring
 * Security filter chain to accidentally double-register through -
 * Boot's generic auto-registration is the only path in, so it's fine.
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-Correlation-Id";
    private static final String MDC_KEY = "correlationId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = request.getHeader(HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        MDC.put(MDC_KEY, correlationId);
        response.setHeader(HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
        }
    }
}
