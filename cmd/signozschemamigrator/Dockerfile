# Multi-stage Dockerfile for signoz-schema-migrator
# Builds Go binary, then creates minimal runtime image

FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o signoz-schema-migrator ./cmd/signozschemamigrator

FROM debian:bookworm-slim

# Install ca-certificates for TLS connections to ClickHouse Cloud
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -u 10001 -m migrator

# Copy the built binary
COPY --from=builder /app/signoz-schema-migrator /signoz-schema-migrator

# Run as non-root
USER migrator

ENTRYPOINT ["/signoz-schema-migrator"]
CMD ["--help"]
