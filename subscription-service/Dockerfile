# ---- Build stage ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw -B dependency:go-offline

COPY src/ src/
# Tests skipped here on purpose - the Testcontainers-backed integration
# suite needs a Docker daemon this build stage doesn't have. Tests run
# via `mvn verify` locally/CI; this only packages a jar from code that
# already passed there.
RUN ./mvnw -B clean package -DskipTests

# ---- Runtime stage ----
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN apk add --no-cache curl \
    && addgroup -S sublite && adduser -S sublite -G sublite

# OpenTelemetry Java agent (step 9-10b) - bytecode auto-instrumentation
# for Spring Web/JDBC/Kafka clients, attached via -javaagent below.
# Deliberately NOT Spring Boot's own Micrometer Tracing bridge - that
# route runs through Boot's own auto-configuration, exactly the surface
# that's already caused real version-mismatch surprises in this project
# under Boot 4 (Mongo's property prefix in step 5, spring-kafka's
# package layout in step 4, the Resilience4j starter avoided in step 8
# for the same reason). The agent instruments at the JVM/bytecode level,
# independent of whatever Spring Boot version is running - including
# propagating trace context through Kafka message HEADERS automatically,
# which is what makes a trace span subscription-service -> Kafka ->
# billing-service -> Kafka -> subscription-service as one connected trace
# in Jaeger, not three disconnected ones.
RUN curl -sSL -o /app/otel-javaagent.jar \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.31.1/opentelemetry-javaagent.jar

COPY --from=build /app/target/subscription-service-*.jar app.jar

USER sublite

EXPOSE 8081

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
    CMD curl -f http://localhost:8081/actuator/health || exit 1

ENTRYPOINT ["java", "-javaagent:/app/otel-javaagent.jar", "-jar", "app.jar"]
