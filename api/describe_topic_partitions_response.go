package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
	"github.com/google/uuid"
)

type DescribeTopicPartitionsResponse struct {
	Header ResponseHeader
	Body   DescribeTopicPartitionsResponseV0ResponseBody
}

type DescribeTopicPartitionsResponseV0ResponseBody struct {
	ThrottleTime int32
	Topics       []DescribeTopicPartitionsResponseV0Topic
	Cursor       Cursor
}

func (d *DescribeTopicPartitionsResponseV0ResponseBody) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt32(d.ThrottleTime)
	enc.PutCompactArrayLen(len(d.Topics))
	for _, topic := range d.Topics {
		err := topic.Encode(enc)
		if err != nil {
			return err
		}
	}
	err := d.Cursor.Encode(enc)
	if err != nil {
		return err
	}
	enc.PutEmptyTaggedFieldArray()
	return nil
}

type Cursor struct {
	NextCursor *int8
}

func (c *Cursor) Encode(enc *encoder.BinaryEncoder) error {
	if c.NextCursor != nil {
		enc.PutInt8(*c.NextCursor)
	} else {
		enc.PutInt8(-1)
	}
	return nil
}

type DescribeTopicPartitionsResponseV0Topic struct {
	ErrorCode            int16
	Name                 string
	ID                   uuid.UUID
	IsInternal           byte
	Partitions           []any
	AuthorizedOperations int32
}

func (d *DescribeTopicPartitionsResponseV0Topic) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt16(d.ErrorCode)
	enc.PutCompactString(d.Name)
	uuidBytes, err := d.ID.MarshalBinary()
	if err != nil {
		return err
	}
	enc.PutRawBytes(uuidBytes)
	enc.PutInt8(int8(d.IsInternal))
	enc.PutCompactArrayLen(len(d.Partitions))
	enc.PutInt32(d.AuthorizedOperations)
	enc.PutEmptyTaggedFieldArray()
	return nil
}

func (d *DescribeTopicPartitionsResponse) Encode(enc *encoder.BinaryEncoder) error {
	err := d.Header.EncodeV1(enc)
	if err != nil {
		return err
	}
	err = d.Body.Encode(enc)
	if err != nil {
		return err
	}
	return nil
}
