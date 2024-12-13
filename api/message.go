package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
)

type Message struct {
	MessageSize int32
	Header      RequestHeader
	Error       int16
	RequestBody any
}

func (m *Message) FromRawRequest(req *RawRequest) (*Message, error) {
	dec := &decoder.BinaryDecoder{}
	dec.Init(req.Payload)

	// Parse the request header
	var reqHeader RequestHeader
	err := reqHeader.DecodeV2(dec)
	if err != nil {
		return nil, err
	}

	m.MessageSize = int32(len(req.Payload))
	m.Header = reqHeader
	if m.Header.ApiVersion < 0 || m.Header.ApiVersion > 4 {
		m.Error = ErrorUnsupportedVersion
	}

	// Parse the request body
	switch reqHeader.ApiKey {
	case DescribeTopicPartitions:
		reqBody := DescribeTopicPartitionsRequestBody{}
		err = reqBody.DecodeV0(dec)
		if err != nil {
			return nil, err
		}
		m.RequestBody = reqBody
	case Fetch:
		reqBody := FetchRequestBody{}
		err = reqBody.DecodeV16(dec)
		if err != nil {
			return nil, err
		}
		m.RequestBody = reqBody
	}
	return m, nil
}
