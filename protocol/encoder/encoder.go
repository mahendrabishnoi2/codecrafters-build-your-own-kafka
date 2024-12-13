package encoder

import (
	"encoding/binary"
)

type BinaryEncoder struct {
	raw    []byte
	offset int
}

func (e *BinaryEncoder) Init(raw []byte) {
	e.raw = raw
	e.offset = 0
}

func (e *BinaryEncoder) PutRawBytes(in []byte) {
	copy(e.raw[e.offset:], in)
	e.offset += len(in)
}

func (e *BinaryEncoder) PutInt8(value int8) {
	e.raw[e.offset] = byte(value)
	e.offset++
}

func (e *BinaryEncoder) PutInt16(value int16) {
	binary.BigEndian.PutUint16(e.raw[e.offset:], uint16(value))
	e.offset += 2
}

func (e *BinaryEncoder) PutInt32(value int32) {
	binary.BigEndian.PutUint32(e.raw[e.offset:], uint32(value))
	e.offset += 4
}

func (e *BinaryEncoder) PutInt64(value int64) {
	binary.BigEndian.PutUint64(e.raw[e.offset:], uint64(value))
	e.offset += 8
}

func (e *BinaryEncoder) PutUvarint(value int64) {
	e.offset += binary.PutUvarint(e.raw[e.offset:], uint64(value))
}

func (e *BinaryEncoder) PutCompactArrayLen(len int) {
	e.PutUvarint(int64(len + 1))
}

func (e *BinaryEncoder) PutEmptyTaggedFieldArray() {
	e.PutUvarint(0)
}

func (e *BinaryEncoder) PutCompactString(name string) {
	e.PutCompactArrayLen(len(name))
	e.PutRawBytes([]byte(name))
}

func (e *BinaryEncoder) ToBytes() []byte {
	return e.raw[:e.offset]
}

func (e *BinaryEncoder) ToKafkaResponse() []byte {
	messageSize := int32(e.offset)
	res := make([]byte, 4+messageSize)
	binary.BigEndian.PutUint32(res, uint32(messageSize))
	copy(res[4:], e.raw[:e.offset])
	return res
}

func (e *BinaryEncoder) PutCompactInt32Array(value []int32) {
	e.PutCompactArrayLen(len(value))
	for _, v := range value {
		e.PutInt32(v)
	}
}

func (e *BinaryEncoder) Offset() int {
	return e.offset
}
