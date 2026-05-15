.PHONY: build tls-dev run compose-up

build:
	go build -o bin/broker ./cmd/broker

tls-dev:
	mkdir -p config/tls
	openssl req -x509 -newkey rsa:2048 -keyout config/tls/server.key -out config/tls/server.pem -days 365 -nodes -subj "/CN=localhost"

run: build
	./bin/broker -config config/broker.yaml

compose-up:
	cd deploy && docker compose up -d --build
