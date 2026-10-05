FROM --platform=$BUILDPLATFORM docker.io/golang:1.27 AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

WORKDIR /app/cmd/iot-things-v2

ARG TARGETARCH
ARG TARGETOS
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build

FROM registry.access.redhat.com/ubi9/ubi-minimal

WORKDIR /opt/diwise

# Ingen seed bakas in i imagen: katalog (catalog-seed.json) och saker
# (things.csv) tillförs via mount (CATALOG_SEED_FILE / things.csv) eller
# skapas via API. Saknade filer varnar vid start och tjänsten startar
# med återställbar katalog från databasen.
COPY --chown=1001 assets/config/authz.rego /opt/diwise/config/authz.rego
COPY --from=builder --chown=1001 /app/cmd/iot-things-v2/iot-things-v2 /opt/diwise

RUN chown 1001 /opt/diwise

EXPOSE 8080
USER 1001

ENTRYPOINT ["/opt/diwise/iot-things-v2"]