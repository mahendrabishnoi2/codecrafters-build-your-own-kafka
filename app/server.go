package main

import (
	"encoding/binary"
	"encoding/hex"
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

	resp := make([]byte, 23)
	// message size
	binary.BigEndian.PutUint32(resp[0:4], 14)
	copy(resp[4:8], buffer[8:12]) // correlation id

	// API Versions Response Body
	// error code
	binary.BigEndian.PutUint16(resp[8:10], 0)

	// API Versions array
	// length of array = 1 byte
	resp[10] = 0x02

	// element 1
	// api key (2 bytes), min version (2 bytes), max version (2 bytes), tag buffer (0x00) (1 byte)
	binary.BigEndian.PutUint16(resp[11:13], 18) // api key
	binary.BigEndian.PutUint16(resp[13:15], 0)  // min version
	binary.BigEndian.PutUint16(resp[15:17], 10) // max version
	resp[17] = 0x00
	binary.BigEndian.PutUint32(resp[18:], 0) // throttle time
	binary.BigEndian.PutUint16(resp[19:23], 0)

	_, err = conn.Write(resp)
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Response sent", resp)

	fmt.Println(hex.Dump(resp))
}
