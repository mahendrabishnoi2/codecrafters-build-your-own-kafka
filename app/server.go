package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/api"
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
)

func Read(conn net.Conn) (*api.Message, error) {
	msg := &api.Message{}

	messageSizeBytes := make([]byte, 4)
	_, err := conn.Read(messageSizeBytes)
	if err != nil {
		return nil, err
	}

	messageSize := int32(binary.BigEndian.Uint32(messageSizeBytes))
	bodyBytes := make([]byte, messageSize)
	_, err = conn.Read(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &api.RawRequest{}
	req = req.From(messageSizeBytes, bodyBytes)

	return msg.FromRawRequest(req)
}

func Send(conn net.Conn, response []byte) error {
	_, err := conn.Write(response)
	return err
}

func handleRequest(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			log.Println("Error closing connection: ", err.Error())
		}
	}(conn)

	for {
		msg, err := Read(conn)
		if err != nil && err.Error() == "EOF" {
			break
		}
		if err != nil {
			log.Println("Error reading data: ", err.Error())
			os.Exit(1)
		}

		respBytes := make([]byte, 8192)
		enc := &encoder.BinaryEncoder{}
		enc.Init(respBytes)
		switch msg.Header.ApiKey {
		case api.ApiVersions:
			resp := api.PrepareAPIVersionsResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error preparing response: ", err.Error())
				os.Exit(1)
			}
		case api.DescribeTopicPartitions:
			resp := api.PrepareDescribeTopicPartitionsResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error encoding response: ", err.Error())
				os.Exit(1)
			}
		case api.Fetch:
			resp := api.PrepareFetchResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error encoding response: ", err.Error())
				os.Exit(1)
			}
		}
		respBytes = enc.ToKafkaResponse()
		err = Send(conn, respBytes)
		if err != nil {
			log.Println("Error writing data: ", err.Error())
			os.Exit(1)
		}
	}
}

func main() {
	log.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer func(l net.Listener) {
		err := l.Close()
		if err != nil {
			log.Println("Error closing listener: ", err.Error())
		}
	}(l)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}
