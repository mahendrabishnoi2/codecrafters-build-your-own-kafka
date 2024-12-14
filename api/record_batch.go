package api

import (
	"hash/crc32"

	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
)

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerId           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

func (r *RecordBatch) Decode(dec *decoder.BinaryDecoder) error {
	r.BaseOffset = dec.GetInt64()
	r.BatchLength = dec.GetInt32()
	r.PartitionLeaderEpoch = dec.GetInt32()
	r.Magic = dec.GetInt8()
	r.CRC = dec.GetInt32()
	r.Attributes = dec.GetInt16()
	r.LastOffsetDelta = dec.GetInt32()
	r.FirstTimestamp = dec.GetInt64()
	r.MaxTimestamp = dec.GetInt64()
	r.ProducerId = dec.GetInt64()
	r.ProducerEpoch = dec.GetInt16()
	r.BaseSequence = dec.GetInt32()
	recordsLength := dec.GetInt32()
	r.Records = make([]Record, recordsLength)
	for i := 0; i < int(recordsLength); i++ {
		record := Record{}
		if err := record.Decode(dec); err != nil {
			return err
		}
		r.Records[i] = record
	}
	return nil
}

func (r *RecordBatch) Encode(enc *encoder.BinaryEncoder) error {
	startOffset := enc.Offset()

	enc.PutInt64(r.BaseOffset)
	batchLengthStartOffset := enc.Offset()
	enc.PutInt32(0) // placeholder
	enc.PutInt32(r.PartitionLeaderEpoch)
	enc.PutInt8(r.Magic)
	crcStartOffset := enc.Offset()
	enc.PutInt32(0) // placeholder
	crcEndOffset := enc.Offset()
	enc.PutInt16(r.Attributes)
	enc.PutInt32(r.LastOffsetDelta)
	enc.PutInt64(r.FirstTimestamp)
	enc.PutInt64(r.MaxTimestamp)
	enc.PutInt64(r.ProducerId)
	enc.PutInt16(r.ProducerEpoch)
	enc.PutInt32(r.BaseSequence)
	enc.PutInt32(int32(len(r.Records)))
	for i, record := range r.Records {
		record.OffsetDelta = int64(i)
		if err := record.Encode(enc); err != nil {
			return err
		}
	}

	batchLength := enc.Offset() - 12 - startOffset // 8 bytes for BaseOffset and 4 bytes for BatchLength
	enc.PutInt32At(int32(batchLength), batchLengthStartOffset)

	// Calculate CRC
	crcData := enc.Bytes()[crcEndOffset:enc.Offset()]
	crcCheckSum := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	enc.PutInt32At(int32(crcCheckSum), crcStartOffset)
	return nil
}

type Record struct {
	Length         int64 // signed varint
	Attributes     int8
	TimestampDelta int64  // signed varint
	OffsetDelta    int64  // signed varint
	KeyLength      int64  // signed varint (-1 if null)
	Key            []byte // nullable
	ValueLength    int64  // signed varint
	Value          []byte // nullable
	Headers        []RecordHeader
}

func (r *Record) Decode(dec *decoder.BinaryDecoder) error {
	r.Length = dec.GetSignedVarint()
	r.Attributes = dec.GetInt8()
	r.TimestampDelta = dec.GetSignedVarint()
	r.OffsetDelta = dec.GetSignedVarint()
	r.KeyLength = dec.GetSignedVarint()
	if r.KeyLength != -1 {
		r.Key = dec.GetBytes(int(r.KeyLength))
	}
	r.ValueLength = dec.GetSignedVarint()
	if r.ValueLength != -1 {
		r.Value = dec.GetBytes(int(r.ValueLength))
	}
	recordHeadersLen := dec.GetUnsignedVarint()
	r.Headers = make([]RecordHeader, recordHeadersLen)
	for i := 0; i < int(recordHeadersLen); i++ {
		recordHeader := RecordHeader{}
		if err := recordHeader.Decode(dec); err != nil {
			return err
		}
		r.Headers[i] = recordHeader
	}
	return nil
}

func (r *Record) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutUvarint(r.Length) // placeholder length
	enc.PutInt8(r.Attributes)
	enc.PutUvarint(r.TimestampDelta)
	enc.PutUvarint(r.OffsetDelta)
	if len(r.Key) != 0 {
		enc.PutCompactArrayLen(int(r.KeyLength))
		enc.PutRawBytes(r.Key)
	} else {
		enc.PutCompactArrayLen(0)
	}
	cmrv := ClusterMetadataRecordValue{}
	_ = cmrv.DecodeBytes(r.Value)
	enc.PutVarint(r.ValueLength)
	enc.PutRawBytes(r.Value)
	enc.PutVarint(int64(len(r.Headers)))
	for _, recordHeader := range r.Headers {
		if err := recordHeader.Encode(enc); err != nil {
			return err
		}
	}
	return nil
}

func (r *Record) GetEncodedLength() int64 {
	enc := encoder.BinaryEncoder{}
	enc.Init(make([]byte, 1024))

	enc.PutInt8(r.Attributes)
	enc.PutUvarint(r.TimestampDelta)
	enc.PutUvarint(r.OffsetDelta)
	if len(r.Key) != 0 {
		enc.PutCompactArrayLen(int(r.KeyLength))
		enc.PutRawBytes(r.Key)
	} else {
		enc.PutCompactArrayLen(0)
	}
	enc.PutVarint(int64(len(r.Value)))
	enc.PutRawBytes(r.Value)
	enc.PutVarint(int64(len(r.Headers)))
	for _, header := range r.Headers {
		header.Encode(&enc)
	}

	return int64(enc.Offset())
}

type RecordHeader struct {
	// for now keep it empty since it's not defined at https://binspec.org/kafka-cluster-metadata?highlight=90-90
}

func (r *RecordHeader) Decode(dec *decoder.BinaryDecoder) error {
	return nil
}

func (r *RecordHeader) Encode(enc *encoder.BinaryEncoder) error {
	return nil
}
