package api

import (
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
)

type TopicName struct {
	Name string
}

func (t *TopicName) Decode(dec *decoder.BinaryDecoder) error {
	t.Name = dec.GetCompactString()
	dec.GetEmptyTaggedFieldArray()
	return nil
}

type DescribeTopicPartitionsRequestBody struct {
	TopicNames             []TopicName
	ResponsePartitionLimit int32
	Cursor                 *int8
}

func (d *DescribeTopicPartitionsRequestBody) DecodeV0(dec *decoder.BinaryDecoder) error {
	topicNamesLength := dec.GetCompactArrayLen()
	fmt.Println("topicNamesLength", topicNamesLength)
	d.TopicNames = make([]TopicName, topicNamesLength)
	for i := 0; i < topicNamesLength; i++ {
		topicName := TopicName{}
		_ = topicName.Decode(dec)
		d.TopicNames[i] = topicName
	}
	fmt.Println("d.TopicNames", d.TopicNames)
	d.ResponsePartitionLimit = dec.GetInt32()
	fmt.Println("d.ResponsePartitionLimit", d.ResponsePartitionLimit)
	cursor := dec.GetInt8()
	if cursor != -1 {
		d.Cursor = &cursor
	}
	fmt.Println("d.Cursor", d.Cursor)
	return nil
}
