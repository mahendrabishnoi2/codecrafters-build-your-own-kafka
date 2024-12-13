package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
)

type DescribeTopicPartitionsRequestBody struct {
	TopicNames             []TopicName
	ResponsePartitionLimit int32
	Cursor                 *int8
}

func (d *DescribeTopicPartitionsRequestBody) DecodeV0(dec *decoder.BinaryDecoder) error {
	topicNamesLength := dec.GetCompactArrayLen()
	d.TopicNames = make([]TopicName, topicNamesLength)
	for i := 0; i < topicNamesLength; i++ {
		topicName := TopicName{}
		_ = topicName.Decode(dec)
		d.TopicNames[i] = topicName
	}
	d.ResponsePartitionLimit = dec.GetInt32()
	cursor := dec.GetInt8()
	if cursor != -1 {
		d.Cursor = &cursor
	}
	dec.GetEmptyTaggedFieldArray()
	return nil
}

type TopicName struct {
	Name string
}

func (t *TopicName) Decode(dec *decoder.BinaryDecoder) error {
	t.Name = dec.GetCompactString()
	dec.GetEmptyTaggedFieldArray()
	return nil
}
