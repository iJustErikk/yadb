package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8000")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	buffer := make([]byte, 1024)

	for {
		fmt.Print("Enter command: ")
		text, _ := reader.ReadString('\n')
		_, err := conn.Write([]byte(text))
		if err != nil {
			fmt.Println(err)
			return
		}

		bytesRead, err := conn.Read(buffer)
		if err != nil {
			fmt.Println(err)
			return
		}
		response := string(buffer[:bytesRead])
		fmt.Print(response)
	}
}
