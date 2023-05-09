all: client server

client:
	cd yadb-client && go build -o ../client

server:
	cd yadb-server && go build -o ../server

clean:
	rm client
	rm server

.PHONY: all client server clean
