package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
	"github.com/google/uuid"
)

type ClusterMetadataRecordValue struct {
	FrameVersion int8
	Type         int8
	Version      int8
	Data         ClusterMetadataRecordValuePayload
}

type ClusterMetadataRecordValuePayload interface {
	isClusterMetadataRecordValuePayload()
	Decode(dec *decoder.BinaryDecoder) error
}

func (c *ClusterMetadataRecordValue) DecodeBytes(bytes []byte) error {
	dec := &decoder.BinaryDecoder{}
	dec.Init(bytes)
	return c.Decode(dec)
}

func (c *ClusterMetadataRecordValue) Decode(dec *decoder.BinaryDecoder) error {
	c.FrameVersion = dec.GetInt8()
	c.Type = dec.GetInt8()
	c.Version = dec.GetInt8()
	switch c.Type {
	case 2:
		c.Data = &TopicRecord{}
	case 3:
		c.Data = &PartitionRecord{}
	case 12:
		c.Data = &FeatureLevelRecord{}
	default:
		return nil
	}
	return c.Data.Decode(dec)
}

type FeatureLevelRecord struct {
	Name         string
	FeatureLevel int16
}

func (f *FeatureLevelRecord) isClusterMetadataRecordValuePayload() {}

func (f *FeatureLevelRecord) Decode(dec *decoder.BinaryDecoder) error {
	f.Name = dec.GetCompactString()
	f.FeatureLevel = dec.GetInt16()
	dec.GetEmptyTaggedFieldArray()
	return nil
}

type TopicRecord struct {
	TopicName string
	TopicUUID uuid.UUID
}

func (t *TopicRecord) isClusterMetadataRecordValuePayload() {}

func (t *TopicRecord) Decode(dec *decoder.BinaryDecoder) error {
	t.TopicName = dec.GetCompactString()
	t.TopicUUID = dec.GetUUID()
	dec.GetEmptyTaggedFieldArray()
	return nil
}

type PartitionRecord struct {
	PartitionID      int32
	TopicUUID        uuid.UUID
	Replicas         []int32
	InSyncReplicas   []int32
	RemovingReplicas []int32
	AddingReplicas   []int32
	Leader           int32
	LeaderEpoch      int32
	PartitionEpoch   int32
	Directories      []uuid.UUID
}

func (p *PartitionRecord) isClusterMetadataRecordValuePayload() {}

func (p *PartitionRecord) Decode(dec *decoder.BinaryDecoder) error {
	p.PartitionID = dec.GetInt32()
	p.TopicUUID = dec.GetUUID()
	p.Replicas = dec.GetCompactInt32Array()
	p.InSyncReplicas = dec.GetCompactInt32Array()
	p.RemovingReplicas = dec.GetCompactInt32Array()
	p.AddingReplicas = dec.GetCompactInt32Array()
	p.Leader = dec.GetInt32()
	p.LeaderEpoch = dec.GetInt32()
	p.PartitionEpoch = dec.GetInt32()
	directoriesLength := dec.GetCompactArrayLen()
	p.Directories = make([]uuid.UUID, directoriesLength)
	for i := 0; i < directoriesLength; i++ {
		p.Directories[i] = dec.GetUUID()
	}
	dec.GetEmptyTaggedFieldArray()
	return nil
}
