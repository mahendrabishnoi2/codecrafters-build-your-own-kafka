package main

import (
	"fmt"
	"net"
	"os"
	"slices"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	buffer := make([]byte, 1024)
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading data: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Received data: ", buffer)

	// get message size from the first 4 bytes
	messageSizeBytes := buffer[:4]
	buffer = buffer[4:]

	requestApiKeyBytes := buffer[:4]
	_ = requestApiKeyBytes
	buffer = buffer[4:]

	requestApiVersionBytes := buffer[:4]
	_ = requestApiVersionBytes
	buffer = buffer[4:]

	correlationIdBytes := buffer[:8]

	dataToWrite := slices.Concat(messageSizeBytes, correlationIdBytes)
	_, err = conn.Write(dataToWrite)
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Sent data: ", dataToWrite)
}
