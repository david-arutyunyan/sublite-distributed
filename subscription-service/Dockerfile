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

COPY --from=build /app/target/subscription-service-*.jar app.jar

USER sublite

EXPOSE 8081

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
    CMD curl -f http://localhost:8081/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
