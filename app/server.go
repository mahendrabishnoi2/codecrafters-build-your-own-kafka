package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/google/uuid"
)

const (
	RequestHeaderVersion0 int8 = 0
	RequestHeaderVersion1 int8 = 1
	RequestHeaderVersion2 int8 = 2
)

const (
	ResponseHeaderVersion0 int8 = 0
	ResponseHeaderVersion1 int8 = 1
)

type ErrorCode = int16

const (
	NoError                 ErrorCode = 0
	UnknownTopicOrPartition ErrorCode = 3
	ErrorUnsupportedVersion ErrorCode = 35
)

type ApiKey = int16

const (
	ApiVersions             ApiKey = 18
	DescribeTopicPartitions ApiKey = 75
)

const NilByte = 0xff

type Message struct {
	MessageSize int32
	Header      RequestHeader
	Error       int16
	RequestBody any
}

type RequestHeader struct {
	ApiKey        ApiKey
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

type ResponseHeader struct {
	CorrelationId int32
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponseV4 struct {
	Header ResponseHeader
	Body   ApiVersionsResponseV4ResponseBody
}

type DescribeTopicPartitionsRequestV0 struct {
	TopicNames             []string
	ResponsePartitionLimit int32
	Cursor                 *int8
}

type DescribeTopicPartitionsResponseV0 struct {
	Header ResponseHeader
	Body   DescribeTopicPartitionsResponseV0ResponseBody
}

func (d DescribeTopicPartitionsResponseV0) Bytes() ([]byte, error) {
	prettyPrint("DescribeTopicPartitionsResponseV0", d)
	buf := &bytes.Buffer{}

	// prepare the response header v1
	err := binary.Write(buf, binary.BigEndian, d.Header.CorrelationId)
	if err != nil {
		return nil, err
	}
	err = buf.WriteByte(0) // tag buffer
	if err != nil {
		return nil, err
	}

	// prepare the response body
	err = binary.Write(buf, binary.BigEndian, d.Body.ThrottleTime)
	if err != nil {
		return nil, err
	}
	err = buf.WriteByte(byte(len(d.Body.Topics) + 1))
	if err != nil {
		return nil, err
	}
	for _, topic := range d.Body.Topics {
		err = binary.Write(buf, binary.BigEndian, topic.ErrorCode)
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.BigEndian, int8(len(topic.Name)+1))
		if err != nil {
			return nil, err
		}

		_, err = buf.WriteString(topic.Name)
		if err != nil {
			return nil, err
		}

		uuidBytes, _ := topic.ID.MarshalBinary()
		_, err = buf.Write(uuidBytes)
		if err != nil {
			return nil, err
		}

		err = buf.WriteByte(topic.IsInternal)
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.BigEndian, int32(len(topic.Partitions)+1))
		if err != nil {
			return nil, err
		}

		err = binary.Write(buf, binary.BigEndian, topic.AuthorizedOperations)
		if err != nil {
			return nil, err
		}

		// tag buffer
		err = buf.WriteByte(0)
		if err != nil {
			return nil, err
		}
	}

	// if d.Body.NextCursor != nil {
	// 	err = buf.WriteByte(byte(*d.Body.NextCursor))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// } else {
	err = buf.WriteByte(NilByte)
	if err != nil {
		return nil, err
	}
	// }

	// tag buffer
	err = buf.WriteByte(0)
	if err != nil {
		return nil, err
	}

	bodyBytes := buf.Bytes()
	messageSize := len(bodyBytes)
	out := make([]byte, 4+messageSize)
	binary.BigEndian.PutUint32(out[0:4], uint32(messageSize))
	copy(out[4:], bodyBytes)

	fmt.Println(hex.Dump(out))

	return out, nil
}

type DescribeTopicPartitionsResponseV0ResponseBody struct {
	ThrottleTime int32
	Topics       []DescribeTopicPartitionsResponseV0Topic
	NextCursor   *int8
}

type DescribeTopicPartitionsResponseV0Topic struct {
	ErrorCode            int16
	Name                 string
	ID                   uuid.UUID
	IsInternal           byte
	Partitions           []any
	AuthorizedOperations int32
}

func getRequestHeaderFromApiKey(apiKey ApiKey) int8 {
	switch apiKey {
	case ApiVersions:
		return RequestHeaderVersion1
	default:
		return RequestHeaderVersion2
	}
}

func getResponseHeaderFromApiKey(apiKey ApiKey) int8 {
	switch apiKey {
	case ApiVersions:
		return ResponseHeaderVersion0
	default:
		return ResponseHeaderVersion1
	}
}

func (a ApiVersionsResponseV4) Bytes() ([]byte, error) {
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
		out := make([]byte, size+4)
		binary.BigEndian.PutUint32(out[0:4], uint32(size))
		binary.BigEndian.PutUint32(out[4:8], uint32(a.Header.CorrelationId))
		binary.BigEndian.PutUint16(out[8:], uint16(a.Body.ErrorCode))
		return out, nil
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

	return out, nil
}

type ApiVersionsResponseV4ResponseBody struct {
	ErrorCode    int16
	ApiVersions  []ApiVersion
	ThrottleTime int32
}

func Read(conn net.Conn) (*Message, error) {
	msg := Message{}
	var readBytes int32

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
	if msg.Header.ApiVersion < 0 || msg.Header.ApiVersion > 4 {
		msg.Error = ErrorUnsupportedVersion
		return &msg, nil
	}
	err = binary.Read(conn, binary.BigEndian, &msg.Header.CorrelationId)
	if err != nil {
		return nil, err
	}

	// fmt.Printf("Request 1: %+v\n", msg)

	readBytes += 8

	// Read the client id
	requestHeaderVersion := getRequestHeaderFromApiKey(msg.Header.ApiKey)
	if requestHeaderVersion == RequestHeaderVersion1 || requestHeaderVersion == RequestHeaderVersion2 {
		var clientIdLength int16
		err = binary.Read(conn, binary.BigEndian, &clientIdLength)
		if err != nil {
			return nil, err
		}
		readBytes += 2
		clientId := make([]byte, clientIdLength)
		_, err = conn.Read(clientId)
		if err != nil {
			return nil, err
		}
		msg.Header.ClientId = string(clientId)
		readBytes += int32(clientIdLength)
	}

	// fmt.Printf("Request 2: %+v\n", msg)

	// read tagged fields if header version is 2 (for now just discard 1 byte)
	if requestHeaderVersion == RequestHeaderVersion2 {
		var taggedFieldsLength int8
		err = binary.Read(conn, binary.BigEndian, &taggedFieldsLength)
		if err != nil {
			return nil, err
		}
		readBytes += 1
		// for now, we are assuming that the tagged fields are empty
	}

	// fmt.Printf("Request 3: %+v\n", msg)

	remainingBody := make([]byte, msg.MessageSize-readBytes)
	_, err = conn.Read(remainingBody)
	if err != nil {
		return nil, err
	}

	// fmt.Printf("Request 4: %+v\n", msg)

	// Parse the request body
	switch msg.Header.ApiKey {
	case DescribeTopicPartitions:
		reqBody := DescribeTopicPartitionsRequestV0{}
		topicNamesArrayLength := int(remainingBody[0]) - 1
		// fmt.Println("Topic names array length: ", topicNamesArrayLength)
		topicNames := make([]string, topicNamesArrayLength)
		offset := 1
		for i := 0; i < topicNamesArrayLength; i++ {
			topicNameLength := int(remainingBody[offset])
			// fmt.Println("Topic name length: ", topicNameLength)
			offset++
			topicNames[i] = string(remainingBody[offset : offset+topicNameLength])
			// fmt.Println("Topic name: ", topicNames[i])
			fmt.Println()
			offset += topicNameLength
			// topic tag buffer
			offset++
		}
		reqBody.TopicNames = topicNames

		// fmt.Println("offset", offset, "Remaining body: ", remainingBody[offset:])
		reqBody.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(remainingBody[offset : offset+4]))
		offset += 4

		cursor := remainingBody[offset]
		if cursor != 0xff {
			reqBody.Cursor = Ptr(int8(cursor))
		}
		// tag buffer
		offset++
		msg.RequestBody = reqBody
	}

	prettyPrint("Read", msg)

	return &msg, nil
}

func Send(conn net.Conn, response []byte) error {
	_, err := conn.Write([]byte{0, 0, 0, 0})
	return err
}

func handleRequest(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("Error closing connection: ", err.Error())
		}
	}(conn)

	for {
		msg, err := Read(conn)
		if err != nil {
			fmt.Println("Error reading data: ", err.Error())
			os.Exit(1)
		}
		fmt.Printf("Received message: %+v\n", msg)

		type bytess interface {
			Bytes() ([]byte, error)
		}

		var resp bytess
		switch msg.Header.ApiKey {
		case ApiVersions:
			resp = prepareApiVersionsResponse(msg)
		case DescribeTopicPartitions:
			resp = prepareDescribeTopicPartitionsResponse(msg)
		}

		respBytes, err := resp.Bytes()
		if err != nil {
			fmt.Println("Error preparing response: ", err.Error())
			os.Exit(1)
		}

		err = Send(conn, respBytes)
		if err != nil {
			fmt.Println("Error writing data: ", err.Error())
			os.Exit(1)
		}
	}
}

func prepareDescribeTopicPartitionsResponse(msg *Message) DescribeTopicPartitionsResponseV0 {
	requestBody := msg.RequestBody.(DescribeTopicPartitionsRequestV0)
	fmt.Printf("Request body: %+v", requestBody)
	resp := DescribeTopicPartitionsResponseV0{
		Header: ResponseHeader{
			CorrelationId: msg.Header.CorrelationId,
		},
		Body: DescribeTopicPartitionsResponseV0ResponseBody{
			ThrottleTime: 0,
			Topics:       nil,
			NextCursor:   nil,
		},
	}

	resp.Body.Topics = []DescribeTopicPartitionsResponseV0Topic{
		{
			ErrorCode:            UnknownTopicOrPartition,
			Name:                 requestBody.TopicNames[0],
			ID:                   uuid.UUID{},
			IsInternal:           0,
			Partitions:           nil,
			AuthorizedOperations: 3576, // hardcoded for stage vt6
		},
	}
	fmt.Printf("DescribeTopicPartitionsResponseV0 Response: %+v\n", resp)
	return resp
}

func prepareApiVersionsResponse(msg *Message) ApiVersionsResponseV4 {
	resp := ApiVersionsResponseV4{
		Header: ResponseHeader{CorrelationId: msg.Header.CorrelationId},
		Body: ApiVersionsResponseV4ResponseBody{
			ErrorCode: msg.Error,
		},
	}

	if msg.Error == NoError {
		resp.Body.ApiVersions = []ApiVersion{
			{ApiKey: 18, MinVersion: 0, MaxVersion: 5},
			{ApiKey: 75, MinVersion: 0, MaxVersion: 11},
		}
	}

	return resp
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer func(l net.Listener) {
		err := l.Close()
		if err != nil {
			fmt.Println("Error closing listener: ", err.Error())
		}
	}(l)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func Ptr[T any](val T) *T {
	return &val
}

func prettyPrint(identifier string, data any) {
	b, _ := json.MarshalIndent(data, "", "\t")
	fmt.Println(identifier, string(b))
}
