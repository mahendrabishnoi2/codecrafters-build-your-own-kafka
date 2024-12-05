package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

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

	data, err := io.ReadAll(conn)
	if err != nil {
		fmt.Println("Error reading data: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Received data: ", string(data))

	dataToWrite := []byte{0, 0, 0, 0, 0, 0, 0, 7}
	_, err = conn.Write(dataToWrite)
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Sent data: ", dataToWrite)
}
