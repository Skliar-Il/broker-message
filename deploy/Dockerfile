FROM golang:1.26-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /broker ./cmd/broker

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /broker /app/broker
COPY config /app/config
EXPOSE 1883 8883 8080 9090
ENTRYPOINT ["/app/broker", "-config", "/app/config/broker.yaml"]
