# Multi-stage Dockerfile for Salesforce-Kafka Bridge
# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /app

# Copy pom.xml and download dependencies (cached layer)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build
COPY src ./src
RUN mvn clean package -DskipTests -B

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine

# Create non-root user and group
RUN addgroup -S bridge && adduser -S bridge -G bridge

WORKDIR /app

# Copy JAR from build stage
COPY --from=build /app/target/salesforce-kafka-bridge-*.jar app.jar

# Change ownership to non-root user
RUN chown -R bridge:bridge /app

# Switch to non-root user
USER bridge

# Expose application port
EXPOSE 8080

# Health check using actuator endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
