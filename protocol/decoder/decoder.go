package decoder

import (
	"encoding/binary"

	"github.com/google/uuid"
)

type BinaryDecoder struct {
	raw    []byte
	offset int
}

func (d *BinaryDecoder) Init(raw []byte) {
	d.raw = raw
	d.offset = 0
}

func (d *BinaryDecoder) GetInt8() int8 {
	value := int8(d.raw[d.offset])
	d.offset++
	return value
}

func (d *BinaryDecoder) GetInt16() int16 {
	value := int16(binary.BigEndian.Uint16(d.raw[d.offset:]))
	d.offset += 2
	return value
}

func (d *BinaryDecoder) GetInt32() int32 {
	value := int32(binary.BigEndian.Uint32(d.raw[d.offset:]))
	d.offset += 4
	return value
}

func (d *BinaryDecoder) GetInt64() int64 {
	value := int64(binary.BigEndian.Uint64(d.raw[d.offset:]))
	d.offset += 8
	return value
}

func (d *BinaryDecoder) GetStringLen() int16 {
	value := int16(binary.BigEndian.Uint16(d.raw[d.offset:]))
	d.offset += 2
	return value
}

func (d *BinaryDecoder) GetString() string {
	length := d.GetStringLen()
	value := string(d.raw[d.offset : d.offset+int(length)])
	d.offset += int(length)
	return value
}

func (d *BinaryDecoder) GetEmptyTaggedFieldArray() any {
	tagsLength := d.GetInt8()
	if tagsLength != 0 {
		panic("expected empty tagged field array")
	}
	return nil
}

func (d *BinaryDecoder) GetUnsignedVarint() uint64 {
	value, n := binary.Uvarint(d.raw[d.offset:])
	d.offset += n
	return value
}

func (d *BinaryDecoder) GetCompactArrayLen() int {
	value := d.GetUnsignedVarint()
	return int(value) - 1
}

func (d *BinaryDecoder) GetCompactString() string {
	length := d.GetUnsignedVarint() - 1
	value := string(d.raw[d.offset : d.offset+int(length)])
	d.offset += int(length)
	return value
}

func (d *BinaryDecoder) GetSignedVarint() int64 {
	value, n := binary.Varint(d.raw[d.offset:])
	d.offset += n
	return value
}

func (d *BinaryDecoder) GetBytes(length int) []byte {
	value := d.raw[d.offset : d.offset+length]
	d.offset += length
	return value
}

func (d *BinaryDecoder) GetUUID() uuid.UUID {
	val, err := uuid.FromBytes(d.raw[d.offset : d.offset+16])
	if err != nil {
		panic(err)
	}
	d.offset += 16
	return val
}

func (d *BinaryDecoder) GetCompactInt32Array() []int32 {
	arrayLength := d.GetCompactArrayLen()
	array := make([]int32, arrayLength)
	for i := 0; i < arrayLength; i++ {
		array[i] = d.GetInt32()
	}
	return array
}

func (d *BinaryDecoder) Remaining() int {
	return len(d.raw) - d.offset
}
