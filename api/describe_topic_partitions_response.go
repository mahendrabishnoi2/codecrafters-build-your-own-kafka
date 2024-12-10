package api

import (
	"bytes"
	"encoding/binary"

	"github.com/google/uuid"
)

type DescribeTopicPartitionsResponseV0 struct {
	Header ResponseHeader
	Body   DescribeTopicPartitionsResponseV0ResponseBody
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

func (d DescribeTopicPartitionsResponseV0) Bytes() ([]byte, error) {
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

		err = binary.Write(buf, binary.BigEndian, int8(len(topic.Partitions)+1))
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

	if d.Body.NextCursor != nil {
		err = buf.WriteByte(byte(*d.Body.NextCursor))
		if err != nil {
			return nil, err
		}
	} else {
		err = buf.WriteByte(NilByte)
		if err != nil {
			return nil, err
		}
	}

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

	return out, nil
}
