package com.sublite.billing.infrastructure;

import com.sublite.billing.domain.ChargeResult;
import com.sublite.billing.domain.Money;
import com.sublite.billing.domain.PaymentGateway;
import com.sublite.billing.domain.PaymentGatewayUnavailableException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A plain unit test, deliberately not a @SpringBootTest/Testcontainers IT -
 * Resilience4j's behavior here is pure algorithmic composition (given a
 * delegate, retry/circuit-break/timeout appropriately), nothing about it
 * needs a real Kafka broker or database. The package-private constructor
 * (see ResilientPaymentGateway's own comment) is what makes substituting
 * a controllable stub delegate possible without a second Spring-visible
 * PaymentGateway bean.
 */
class ResilientPaymentGatewayTest {

    private static final Money AMOUNT = new Money(new BigDecimal("9.99"), "USD");

    /**
     * PaymentGateway has two abstract methods (charge/refund), so it
     * isn't a lambda-friendly functional interface - this stub routes
     * both through the same supplied behavior, since these tests only
     * ever exercise charge().
     */
    private static PaymentGateway stub(Supplier<ChargeResult> behavior) {
        return new PaymentGateway() {
            @Override
            public ChargeResult charge(Money amount) {
                return behavior.get();
            }

            @Override
            public ChargeResult refund(Money amount) {
                return behavior.get();
            }
        };
    }

    @Test
    void retriesATransientFailureAndEventuallySucceeds() {
        AtomicInteger calls = new AtomicInteger();
        PaymentGateway flakyThenOk = stub(() -> {
            if (calls.incrementAndGet() <= 2) {
                throw new PaymentGatewayUnavailableException("simulated blip");
            }
            return new ChargeResult.Success("ref-after-retry");
        });
        ResilientPaymentGateway gateway = new ResilientPaymentGateway(
                flakyThenOk,
                50, 10, 5000, 3,
                3, 10,
                2000,
                new SimpleMeterRegistry()
        );

        ChargeResult result = gateway.charge(AMOUNT);

        assertThat(result).isInstanceOf(ChargeResult.Success.class);
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void circuitOpensAfterRepeatedFailuresAndFailsFastWithoutCallingTheDelegate() {
        AtomicInteger calls = new AtomicInteger();
        PaymentGateway alwaysFails = stub(() -> {
            calls.incrementAndGet();
            throw new PaymentGatewayUnavailableException("simulated outage");
        });
        // max-attempts=1 (no retry) so each charge() call maps to exactly
        // one delegate invocation - keeps the call-count assertions
        // below unambiguous.
        ResilientPaymentGateway gateway = new ResilientPaymentGateway(
                alwaysFails,
                50, 4, 5000, 3,
                1, 10,
                2000,
                new SimpleMeterRegistry()
        );

        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> gateway.charge(AMOUNT)).isInstanceOf(PaymentGatewayUnavailableException.class);
        }
        assertThat(calls.get()).isEqualTo(4);

        // The circuit should be open now (4/4 failures, minimumNumberOfCalls=4,
        // failureRateThreshold=50%). This 5th call must fail WITHOUT
        // reaching the delegate at all.
        assertThatThrownBy(() -> gateway.charge(AMOUNT)).isInstanceOf(PaymentGatewayUnavailableException.class);
        assertThat(calls.get())
                .as("the circuit breaker should have failed fast, without calling the delegate a 5th time")
                .isEqualTo(4);
    }

    @Test
    void aSlowCallIsTimedOutRatherThanWaitedOut() {
        PaymentGateway slow = stub(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new ChargeResult.Success("ref-too-slow-to-matter");
        });
        // timeout-ms=100, max-attempts=1: this call should fail around
        // 100ms in, nowhere near the delegate's own 2-second sleep.
        ResilientPaymentGateway gateway = new ResilientPaymentGateway(
                slow,
                50, 10, 5000, 3,
                1, 10,
                100,
                new SimpleMeterRegistry()
        );

        long start = System.currentTimeMillis();
        assertThatThrownBy(() -> gateway.charge(AMOUNT)).isInstanceOf(PaymentGatewayUnavailableException.class);
        long elapsedMs = System.currentTimeMillis() - start;

        assertThat(elapsedMs)
                .as("should time out around 100ms, not wait for the delegate's full 2s sleep")
                .isLessThan(1000);
    }
}
