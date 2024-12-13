package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/api"
	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
	"github.com/google/uuid"
)

type ErrorCode = int16

const (
	NoError                 ErrorCode = 0
	UnknownTopicOrPartition ErrorCode = 3
	ErrorUnsupportedVersion ErrorCode = 35
)

type Message struct {
	MessageSize int32
	Header      api.RequestHeader
	Error       int16
	RequestBody any
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponseV4 struct {
	Header api.ResponseHeader
	Body   ApiVersionsResponseV4ResponseBody
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

	return out, nil
}

type ApiVersionsResponseV4ResponseBody struct {
	ErrorCode    int16
	ApiVersions  []ApiVersion
	ThrottleTime int32
}

func Read(conn net.Conn) (*Message, error) {
	msg := Message{}

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

	var req api.RawRequest
	req = req.From(messageSizeBytes, bodyBytes)

	dec := &decoder.BinaryDecoder{}
	dec.Init(req.Payload)

	// Parse the request header
	var reqHeader api.RequestHeader
	err = reqHeader.DecodeV2(dec)
	if err != nil {
		return nil, err
	}

	msg.MessageSize = messageSize
	msg.Header = reqHeader
	if msg.Header.ApiVersion < 0 || msg.Header.ApiVersion > 4 {
		msg.Error = ErrorUnsupportedVersion
	}

	// Parse the request body
	switch reqHeader.ApiKey {
	case api.DescribeTopicPartitions:
		reqBody := api.DescribeTopicPartitionsRequestBody{}
		err = reqBody.DecodeV0(dec)
		if err != nil {
			return nil, err
		}
		msg.RequestBody = reqBody
	}

	return &msg, nil
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
		switch msg.Header.ApiKey {
		case api.ApiVersions:
			resp := prepareApiVersionsResponse(msg)
			respBytes, err = resp.Bytes()
			if err != nil {
				log.Println("Error preparing response: ", err.Error())
				os.Exit(1)
			}
		case api.DescribeTopicPartitions:
			enc := &encoder.BinaryEncoder{}
			enc.Init(respBytes)
			resp := prepareDescribeTopicPartitionsResponse(msg)
			err = resp.Encode(enc)
			if err != nil {
				log.Println("Error encoding response: ", err.Error())
				os.Exit(1)
			}
			respBytes = enc.ToKafkaResponse()
		}

		err = Send(conn, respBytes)
		if err != nil {
			log.Println("Error writing data: ", err.Error())
			os.Exit(1)
		}
	}
}

func getTopicFromMetadata(metadata []api.RecordBatch, topicName string) *api.TopicRecord {
	for _, recordBatch := range metadata {
		for _, record := range recordBatch.Records {
			recordValue := api.ClusterMetadataRecordValue{}
			_ = recordValue.DecodeBytes(record.Value)
			switch recordValue.Type {
			case 2:
				topicRecord := recordValue.Data.(*api.TopicRecord)
				if topicRecord.TopicName == topicName {
					return topicRecord
				}
			}
		}
	}
	return nil
}

func getPartitionsFromMetadata(metadata []api.RecordBatch, topicId uuid.UUID) []*api.PartitionRecord {
	var partitions []*api.PartitionRecord
	for _, recordBatch := range metadata {
		for _, record := range recordBatch.Records {
			recordValue := api.ClusterMetadataRecordValue{}
			_ = recordValue.DecodeBytes(record.Value)
			switch recordValue.Type {
			case 3:
				partitionRecord := recordValue.Data.(*api.PartitionRecord)
				if partitionRecord.TopicUUID == topicId {
					partitions = append(partitions, partitionRecord)
				}
			}
		}
	}
	return partitions
}

func prepareDescribeTopicPartitionsResponse(msg *Message) api.DescribeTopicPartitionsResponse {
	topicMetadata := api.GetTopicMetadata()
	req := msg.RequestBody.(api.DescribeTopicPartitionsRequestBody)
	resp := api.DescribeTopicPartitionsResponse{
		Header: api.ResponseHeader{
			CorrelationId: msg.Header.CorrelationId,
		},
		Body: api.DescribeTopicPartitionsResponseV0ResponseBody{
			ThrottleTime: 0,
			Topics:       nil,
			Cursor:       api.Cursor{NextCursor: nil},
		},
	}
	for _, topic := range req.TopicNames {
		topicRecord := getTopicFromMetadata(topicMetadata, topic.Name)
		if topicRecord == nil {
			resp.Body.Topics = append(resp.Body.Topics, api.DescribeTopicPartitionsResponseV0Topic{
				ErrorCode:            UnknownTopicOrPartition,
				Name:                 topic.Name,
				ID:                   uuid.UUID{},
				IsInternal:           0,
				Partitions:           nil,
				AuthorizedOperations: 3576, // hardcoded for stage vt6
			})
			continue
		}
		partitionRecords := getPartitionsFromMetadata(topicMetadata, topicRecord.TopicUUID)
		partitions := make([]api.Partition, 0, len(partitionRecords))
		for _, partition := range partitionRecords {
			partitions = append(partitions, api.Partition{
				ErrorCode:       NoError,
				PartitionIndex:  partition.PartitionID,
				LeaderID:        partition.Leader,
				LeaderEpoch:     partition.LeaderEpoch,
				ReplicaNodes:    partition.Replicas,
				ISRNodes:        partition.InSyncReplicas,
				ELRs:            nil,
				LastKnownELRs:   nil,
				OffLineReplicas: nil,
			})
		}
		resp.Body.Topics = append(resp.Body.Topics, api.DescribeTopicPartitionsResponseV0Topic{
			ErrorCode:            NoError,
			Name:                 topicRecord.TopicName,
			ID:                   topicRecord.TopicUUID,
			IsInternal:           0,
			Partitions:           partitions,
			AuthorizedOperations: 3576,
		})
	}
	return resp
}

func prepareApiVersionsResponse(msg *Message) ApiVersionsResponseV4 {
	resp := ApiVersionsResponseV4{
		Header: api.ResponseHeader{CorrelationId: msg.Header.CorrelationId},
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

// func Ptr[T any](val T) *T {
// 	return &val
// }

// func prettyPrint(identifier string, data any) {
// 	b, _ := json.MarshalIndent(data, "", "\t")
// 	fmt.Println(identifier, string(b))
// }
