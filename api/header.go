package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
)

type ApiKey = int16

const (
	Fetch                   ApiKey = 1
	ApiVersions             ApiKey = 18
	DescribeTopicPartitions ApiKey = 75
)

type RequestHeader struct {
	ApiKey        ApiKey
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

func (r *RequestHeader) DecodeV2(dec *decoder.BinaryDecoder) error {
	r.ApiKey = dec.GetInt16()
	r.ApiVersion = dec.GetInt16()
	r.CorrelationId = dec.GetInt32()
	r.ClientId = dec.GetString()
	dec.GetEmptyTaggedFieldArray()
	return nil
}

type ResponseHeader struct {
	CorrelationId int32
}

func (r *ResponseHeader) EncodeV0(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(r.CorrelationId)
	return nil
}

func (r *ResponseHeader) EncodeV1(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(r.CorrelationId)
	enc.PutEmptyTaggedFieldArray()
	return nil
}
