package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
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
	fmt.Printf("Received data: %v (%d)", buffer[4:8], int32(binary.BigEndian.Uint32(buffer[8:12])))

	resp := make([]byte, 10)
	copy(resp[0:4], buffer[4:8])
	copy(resp[4:8], buffer[8:12])
	binary.BigEndian.PutUint16(resp[8:10], 35)
	_, err = conn.Write(resp)
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Response sent", resp)
}
