package main

import (
	"bufio"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
	"net"
	"os"
	"io"
	"strings"
)

type Storage struct {
	db *pebble.DB
}

func (s *Storage) put(key, value []byte) error {
	return s.db.Set(key, value, pebble.Sync)
}

func (s *Storage) get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	value_copy := make([]byte, len(value))
	copy(value_copy, value)
	if len(value) != 0 {
		closer.Close()
	}
	return value_copy, err
}

func (s *Storage) delete(key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}

func SendResponse(f io.Writer, format string, a ...interface{}) {
	_, err := fmt.Fprintf(f, format, a...)
	if err != nil {
		log.Println("Network: Response error " + err.Error())
	}
}

func handleClient(conn net.Conn, storage *Storage) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := strings.Fields(scanner.Text())
		if len(command) == 0 {
			continue
		}

		switch command[0] {
		case "put":
			if len(command) != 3 {
				SendResponse(conn, "Invalid put, please provide 2 arguments\n")
				continue
			}
			key, value := []byte(command[1]), []byte(command[2])
			err := storage.put(key, value)
			if err != nil {
				log.Println("Storage: Put Error" + err.Error())
				continue
			}
			SendResponse(conn, "Success\n")

		case "get":
			if len(command) != 2 {
				SendResponse(conn, "Invalid get, please provide 1 argument\n")
				continue
			}
			key := []byte(command[1])
			value, err := storage.get(key)
			if err != nil && err != pebble.ErrNotFound {
				log.Println("Storage: Get Error" + err.Error())
				continue
			}
			if err == pebble.ErrNotFound {
				SendResponse(conn, "Key not found\n")
			} else {
				SendResponse(conn, "%s\n", value)
			}

		case "delete":
			if len(command) != 2 {
				SendResponse(conn, "Invalid delete, please provide 1 argument\n")
				continue
			}
			key := []byte(command[1])
			err := storage.delete(key)
			if err != nil && err != pebble.ErrNotFound {
				log.Println("Storage: Delete Error" + err.Error())
				continue
			}
			SendResponse(conn, "Success\n",)

		default:
			SendResponse(conn, "Please use get put or delete\n")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println("Network: Request error " + err.Error())
	}
}

func main() {
	logFile, err := os.Create("./logs/log")
	if err != nil {
		log.Fatal(err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	db, err := pebble.Open("yadb", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	storage := &Storage{db: db}

	ln, err := net.Listen("tcp", "127.0.0.1:8000")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("server up")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("client at: %v", conn.RemoteAddr())
		go handleClient(conn, storage)
	}
}
