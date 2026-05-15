FROM golang:1.26-alpine3.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /pub ./example/publisher && \
    CGO_ENABLED=0 go build -o /cons ./example/consumer

FROM alpine:3.20
WORKDIR /app
COPY --from=build /pub /app/pub
COPY --from=build /cons /app/cons
