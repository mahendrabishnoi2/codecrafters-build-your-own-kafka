package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
	"github.com/google/uuid"
)

type FetchRequestBody struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []FetchTopic
	ForgottenTopicsData []ForgottenTopic
	RackID              string
}

type FetchTopic struct {
	TopicID    uuid.UUID
	Partitions []FetchPartition
}

type FetchPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

type ForgottenTopic struct {
	TopicID    uuid.UUID
	Partitions []int32
}

func (f *FetchRequestBody) DecodeV16(enc *decoder.BinaryDecoder) error {
	f.MaxWaitMs = enc.GetInt32()
	f.MinBytes = enc.GetInt32()
	f.MaxBytes = enc.GetInt32()
	f.IsolationLevel = enc.GetInt8()
	f.SessionID = enc.GetInt32()
	f.SessionEpoch = enc.GetInt32()
	f.Topics = make([]FetchTopic, enc.GetCompactArrayLen())
	for i := 0; i < len(f.Topics); i++ {
		if err := f.Topics[i].Decode(enc); err != nil {
			return err
		}
	}
	f.ForgottenTopicsData = make([]ForgottenTopic, enc.GetCompactArrayLen())
	for i := 0; i < len(f.ForgottenTopicsData); i++ {
		if err := f.ForgottenTopicsData[i].Decode(enc); err != nil {
			return err
		}
	}
	f.RackID = enc.GetCompactString()
	enc.GetEmptyTaggedFieldArray()
	return nil
}

func (f *FetchTopic) Decode(dec *decoder.BinaryDecoder) error {
	f.TopicID = dec.GetUUID()
	partitionsLength := dec.GetCompactArrayLen()
	f.Partitions = make([]FetchPartition, partitionsLength)
	for i := 0; i < partitionsLength; i++ {
		if err := f.Partitions[i].Decode(dec); err != nil {
			return err
		}
	}
	dec.GetEmptyTaggedFieldArray()
	return nil
}

func (f *FetchPartition) Decode(dec *decoder.BinaryDecoder) error {
	f.PartitionIndex = dec.GetInt32()
	f.CurrentLeaderEpoch = dec.GetInt32()
	f.FetchOffset = dec.GetInt64()
	f.LastFetchedEpoch = dec.GetInt32()
	f.LogStartOffset = dec.GetInt64()
	f.PartitionMaxBytes = dec.GetInt32()
	dec.GetEmptyTaggedFieldArray()
	return nil
}

func (f *ForgottenTopic) Decode(dec *decoder.BinaryDecoder) error {
	f.TopicID = dec.GetUUID()
	f.Partitions = dec.GetCompactInt32Array()
	dec.GetEmptyTaggedFieldArray()
	return nil
}
