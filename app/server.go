package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"os"
)

const (
	NoError                 int16 = 0
	ErrorUnsupportedVersion int16 = 35
)

type Message struct {
	MessageSize int32
	Header      RequestHeader
	Error       int16
}

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
}

type ResponseHeaderV0 struct {
	CorrelationId int32
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponseV4 struct {
	Header ResponseHeaderV0
	Body   ApiVersionsResponseV4ResponseBody
}

func (a ApiVersionsResponseV4) Bytes() []byte {
	// https://binspec.org/kafka-api-versions-Response-v4
	// correlation id - 4 bytes
	// error code - 2 bytes
	// api versions array length - 1 byte
	// array length * (api key - 2 byte, min version - 2 byte, max version - 2 byte, tag buffer - 1 byte = 7 bytes)
	// throttle time - 4 bytes
	// tag buffer - 1 byte

	size := 4 + 2 + 1 + len(a.Body.ApiVersions)*7 + 4 + 1
	if a.Body.ErrorCode != NoError {
		size = 6 // correlation id + error code
		out := make([]byte, size)
		binary.BigEndian.PutUint32(out, uint32(a.Header.CorrelationId))
		binary.BigEndian.PutUint16(out[4:], uint16(a.Body.ErrorCode))
		return out
	}

	out := make([]byte, size+4)

	// message size
	binary.BigEndian.PutUint32(out[0:4], uint32(size))

	// correlation id
	binary.BigEndian.PutUint32(out[4:8], uint32(a.Header.CorrelationId))

	// error code
	binary.BigEndian.PutUint16(out[8:10], uint16(a.Body.ErrorCode))

	// api versions array length
	out[10] = byte(len(a.Body.ApiVersions) + 1)

	// api versions array
	for i, v := range a.Body.ApiVersions {
		offset := 11 + i*7
		binary.BigEndian.PutUint16(out[offset:offset+2], uint16(v.ApiKey))
		binary.BigEndian.PutUint16(out[offset+2:offset+4], uint16(v.MinVersion))
		binary.BigEndian.PutUint16(out[offset+4:offset+6], uint16(v.MaxVersion))
		out[offset+6] = 0
	}

	// throttle time
	binary.BigEndian.PutUint32(out[size-5:size-1], uint32(a.Body.ThrottleTime))

	// tag buffer
	out[size-1] = 0

	fmt.Println(hex.Dump(out))

	return out
}

type ApiVersionsResponseV4ResponseBody struct {
	ErrorCode    int16
	ApiVersions  []ApiVersion
	ThrottleTime int32
}

func Read(conn net.Conn) (*Message, error) {
	msg := Message{}

	// Headers are 8 bytes long
	err := binary.Read(conn, binary.BigEndian, &msg.MessageSize)
	if err != nil {
		return nil, err
	}
	err = binary.Read(conn, binary.BigEndian, &msg.Header.ApiKey)
	if err != nil {
		return nil, err
	}
	err = binary.Read(conn, binary.BigEndian, &msg.Header.ApiVersion)
	if err != nil {
		return nil, err
	}
	err = binary.Read(conn, binary.BigEndian, &msg.Header.CorrelationId)
	if err != nil {
		return nil, err
	}

	remainingBody := make([]byte, msg.MessageSize-8)
	_, err = conn.Read(remainingBody)
	if err != nil {
		return nil, err
	}

	if msg.Header.ApiVersion < 0 || msg.Header.ApiVersion > 4 {
		msg.Error = ErrorUnsupportedVersion
		return &msg, nil
	}
	return &msg, nil
}

func Send(conn net.Conn, response []byte) error {
	_, err := conn.Write(response)
	return err
}

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

	msg, err := Read(conn)
	if err != nil {
		fmt.Println("Error reading data: ", err.Error())
		os.Exit(1)
	}

	fmt.Printf("Received message: %+v\n", msg)

	resp := ApiVersionsResponseV4{
		Header: ResponseHeaderV0{CorrelationId: msg.Header.CorrelationId},
		Body: ApiVersionsResponseV4ResponseBody{
			ErrorCode: msg.Error,
		},
	}

	if msg.Error == NoError {
		resp.Body.ApiVersions = []ApiVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 5},
			{ApiKey: 0, MinVersion: 0, MaxVersion: 11},
		}
	}

	fmt.Printf("Sending response: %+v\n", resp)
	err = Send(conn, resp.Bytes())
	if err != nil {
		fmt.Println("Error writing data: ", err.Error())
		os.Exit(1)
	}
	return
}
