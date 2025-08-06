# Multi-stage build for better security and smaller image size
FROM eclipse-temurin:17-jdk-alpine AS builder

# Create app directory
WORKDIR /app

# Copy Maven files
COPY pom.xml .
COPY src ./src

# Build the application
RUN apk add --no-cache maven && \
    mvn clean package -DskipTests && \
    mv target/bitacora-*-jar-with-dependencies.jar app.jar

# Runtime stage
FROM eclipse-temurin:17-jre-alpine

# Create non-root user
RUN addgroup -g 1001 -S bitacora && \
    adduser -S bitacora -u 1001 -G bitacora

# Install required packages
RUN apk add --no-cache curl

# Set working directory
WORKDIR /app

# Copy jar from builder stage
COPY --from=builder /app/app.jar .

# Change ownership to non-root user
RUN chown -R bitacora:bitacora /app
USER bitacora

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8082/health || exit 1

# Expose port
EXPOSE 8082

# Set JVM options for container
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:+UseStringDeduplication"

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
