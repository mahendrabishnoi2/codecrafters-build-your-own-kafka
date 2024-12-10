package decoder

import (
	"encoding/binary"
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
	d.offset++
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
