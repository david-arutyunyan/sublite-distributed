package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.PaymentGateway;
import com.sublite.billing.domain.PaymentGatewayUnavailableException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * The Resilience4j layer (step 8) around whatever PaymentGateway this
 * wraps - the ONLY Spring-managed PaymentGateway bean in this service,
 * decorating RandomPaymentGateway (a plain object, not a bean) rather
 * than replacing it. SubscriptionChargeService and
 * SubscriptionCancellationService are completely unaware of any of this -
 * they still just call gateway.charge()/refund() through the interface;
 * the resilience concern is applied purely at the wiring layer.
 *
 * Composition order (outermost to innermost): Retry(CircuitBreaker(
 * TimeLimiter(call))) - the standard Resilience4j shape for a
 * synchronous caller that wants a bounded-time call. TimeLimiter needs a
 * CompletableFuture to bound, so the actual gateway call runs on a
 * background executor - but this method still BLOCKS the calling thread
 * on the result before returning, which is what keeps this safe to call
 * from inside SubscriptionChargeService's/SubscriptionCancellationService's
 * own @Transactional methods: no database work ever happens on the
 * background thread, only the (pure, side-effect-free) gateway call, so
 * there's no transaction-propagation problem to worry about.
 *
 * This is a SEPARATE, inner layer of retry from the Kafka-level
 * DefaultErrorHandler retry+DLQ from step 6, not a replacement for it:
 * this layer retries a single flaky GATEWAY CALL, fast, within one
 * message-processing attempt (a few hundred ms); step 6's layer retries
 * the whole MESSAGE across separate poll cycles (seconds), and is what
 * eventually dead-letters a message if the gateway stays down long
 * enough to exhaust both layers. A brief blip gets absorbed here without
 * ever bothering the outer layer; a real outage still ends up on the DLQ.
 */
@Component
public class ResilientPaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(ResilientPaymentGateway.class);
    private static final String INSTANCE_NAME = "paymentGateway";

    private final PaymentGateway delegate;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final TimeLimiter timeLimiter;
    private final ExecutorService executor;

    @Autowired
    public ResilientPaymentGateway(
            @Value("${sublite.resilience.circuit-breaker.failure-rate-threshold:50}") float failureRateThreshold,
            @Value("${sublite.resilience.circuit-breaker.sliding-window-size:10}") int slidingWindowSize,
            @Value("${sublite.resilience.circuit-breaker.wait-duration-in-open-state-ms:5000}") long waitDurationInOpenStateMs,
            @Value("${sublite.resilience.circuit-breaker.permitted-calls-in-half-open-state:3}") int permittedCallsInHalfOpenState,
            @Value("${sublite.resilience.retry.max-attempts:3}") int retryMaxAttempts,
            @Value("${sublite.resilience.retry.wait-duration-ms:200}") long retryWaitDurationMs,
            @Value("${sublite.resilience.timeout-ms:2000}") long timeoutMs
    ) {
        this(new RandomPaymentGateway(), failureRateThreshold, slidingWindowSize, waitDurationInOpenStateMs,
                permittedCallsInHalfOpenState, retryMaxAttempts, retryWaitDurationMs, timeoutMs);
    }

    /**
     * Package-private - lets a same-package unit test substitute a
     * controllable stub delegate instead of the real RandomPaymentGateway,
     * without needing a second Spring-visible PaymentGateway bean (which
     * would break @MockitoBean PaymentGateway in the existing ITs - those
     * mock the whole gateway and don't need this wrapper's behavior).
     */
    ResilientPaymentGateway(
            PaymentGateway delegate,
            float failureRateThreshold,
            int slidingWindowSize,
            long waitDurationInOpenStateMs,
            int permittedCallsInHalfOpenState,
            int retryMaxAttempts,
            long retryWaitDurationMs,
            long timeoutMs
    ) {
        this.delegate = delegate;

        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .slidingWindowSize(slidingWindowSize)
                // Explicit, not left to the library default (100) - ties
                // "how many calls before the circuit can even evaluate
                // opening" to the same window size everything else here
                // is configured against, which is also what makes this
                // deterministic to unit-test.
                .minimumNumberOfCalls(slidingWindowSize)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenStateMs))
                .permittedNumberOfCallsInHalfOpenState(permittedCallsInHalfOpenState)
                .build();
        this.circuitBreaker = CircuitBreaker.of(INSTANCE_NAME, circuitBreakerConfig);
        circuitBreaker.getEventPublisher().onStateTransition(event ->
                log.warn("Payment gateway circuit breaker state transition: {}", event.getStateTransition()));

        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(retryMaxAttempts)
                .waitDuration(Duration.ofMillis(retryWaitDurationMs))
                // Only technical failures are retryable - never
                // CallNotPermittedException (the circuit is already
                // open; retrying immediately just hammers a dependency
                // everyone already agrees is down) and never a
                // ChargeResult itself (Declined is a normal return
                // value, not a thrown exception, so it never reaches
                // Retry's classification in the first place).
                .retryExceptions(PaymentGatewayUnavailableException.class)
                .build();
        this.retry = Retry.of(INSTANCE_NAME, retryConfig);
        retry.getEventPublisher().onRetry(event ->
                log.warn("Retrying payment gateway call (attempt {}): {}",
                        event.getNumberOfRetryAttempts(), event.getLastThrowable().getMessage()));

        this.timeLimiter = TimeLimiter.of(INSTANCE_NAME, TimeLimiterConfig.custom()
                .timeoutDuration(Duration.ofMillis(timeoutMs))
                .build());

        this.executor = Executors.newCachedThreadPool();
    }

    @Override
    public ChargeResult charge(Money amount) {
        return execute(() -> delegate.charge(amount));
    }

    @Override
    public ChargeResult refund(Money amount) {
        return execute(() -> delegate.refund(amount));
    }

    private ChargeResult execute(Supplier<ChargeResult> call) {
        Supplier<CompletableFuture<ChargeResult>> futureSupplier =
                () -> CompletableFuture.supplyAsync(call, executor);
        Callable<ChargeResult> withTimeout = TimeLimiter.decorateFutureSupplier(timeLimiter, futureSupplier);
        Callable<ChargeResult> withCircuitBreaker = CircuitBreaker.decorateCallable(circuitBreaker, withTimeout);
        Callable<ChargeResult> withRetry = Retry.decorateCallable(retry, withCircuitBreaker);

        try {
            return withRetry.call();
        } catch (PaymentGatewayUnavailableException e) {
            throw e;
        } catch (Exception e) {
            // Normalizes CallNotPermittedException (circuit open) and
            // TimeoutException (TimeLimiter) into the same exception
            // type callers already handle - SubscriptionChargeService/
            // SubscriptionCancellationService, and beyond them the
            // step-6 Kafka error handler, don't need to know WHICH of
            // the three resilience mechanisms is why the gateway call
            // ultimately failed.
            throw new PaymentGatewayUnavailableException("Payment gateway call failed: " + e.getMessage(), e);
        }
    }

    @PreDestroy
    void shutdown() {
        executor.shutdown();
    }
}
