package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
	"github.com/google/uuid"
)

type FetchResponse struct {
	Header ResponseHeader
	Body   FetchResponseBody
}

func (r *FetchResponse) Encode(enc *encoder.BinaryEncoder) error {
	if err := r.Header.EncodeV1(enc); err != nil {
		return err
	}

	if err := r.Body.Encode(enc); err != nil {
		return err
	}
	return nil
}

type FetchResponseBody struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionId      int32
	Responses      []FetchResponseTopic
}

func (b *FetchResponseBody) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(b.ThrottleTimeMs)
	enc.PutInt16(b.ErrorCode)
	enc.PutInt32(b.SessionId)
	enc.PutCompactArrayLen(len(b.Responses))
	for _, response := range b.Responses {
		if err := response.Encode(enc); err != nil {
			return err
		}
	}
	enc.PutEmptyTaggedFieldArray()
	return nil
}

type FetchResponseTopic struct {
	TopicID    uuid.UUID
	Partitions []FetchResponsePartition
}

func (t *FetchResponseTopic) Encode(enc *encoder.BinaryEncoder) error {
	uuidBytes, _ := t.TopicID.MarshalBinary()
	enc.PutRawBytes(uuidBytes)
	enc.PutCompactArrayLen(len(t.Partitions))
	for _, partition := range t.Partitions {
		if err := partition.Encode(enc); err != nil {
			return err
		}
	}
	enc.PutEmptyTaggedFieldArray()
	return nil
}

type FetchResponsePartition struct {
	PartitionIndex       int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	Records              []RecordBatch
}

func (p *FetchResponsePartition) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(p.PartitionIndex)
	enc.PutInt16(p.ErrorCode)
	enc.PutInt64(p.HighWatermark)
	enc.PutInt64(p.LastStableOffset)
	enc.PutInt64(p.LogStartOffset)
	enc.PutCompactArrayLen(len(p.AbortedTransactions))
	for _, transaction := range p.AbortedTransactions {
		if err := transaction.Encode(enc); err != nil {
			return err
		}
	}
	enc.PutInt32(p.PreferredReadReplica)
	enc.PutCompactArrayLen(len(p.Records))
	for _, record := range p.Records {
		if err := record.Encode(enc); err != nil {
			return err
		}
	}
	enc.PutEmptyTaggedFieldArray()
	return nil
}

type AbortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
}

func (a *AbortedTransaction) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt64(a.ProducerId)
	enc.PutInt64(a.FirstOffset)
	return nil
}
