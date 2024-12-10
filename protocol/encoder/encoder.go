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

func (e *BinaryEncoder) PutUvarint(value int64) {
	e.offset += binary.PutUvarint(e.raw[e.offset:], uint64(value))
}

func (e *BinaryEncoder) PutCompactArrayLen(len int) {
	e.PutUvarint(int64(len + 1))
}
