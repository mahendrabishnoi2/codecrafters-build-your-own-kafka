package api

import (
	"io"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/protocol/decoder"
)

const clusterMetadataLogFilePath = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

type RecordBatch struct {
	BaseOffset           int64
	Length               int32
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
	r.Length = dec.GetInt32()
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

type Record struct {
	Length         int64 // signed varint
	Attributes     int8
	TimestampDelta int64  // signed varint
	OffsetDelta    int64  // signed varint
	KeyLength      int64  // signed varint (-1 if null)
	Key            []byte // nullable
	ValueLength    int64  // signed varint (-1 if null)
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

type RecordHeader struct {
	// for now keep it empty since it's not defined at https://binspec.org/kafka-cluster-metadata?highlight=90-90
}

func (r *RecordHeader) Decode(dec *decoder.BinaryDecoder) error {
	return nil
}

func GetTopicMetadata() []RecordBatch {
	var res []RecordBatch
	file, err := os.Open(clusterMetadataLogFilePath)
	if err != nil {
		return res
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		return res
	}
	dec := &decoder.BinaryDecoder{}
	dec.Init(bytes)
	for dec.Remaining() > 0 {
		recordBatch := RecordBatch{}
		if err := recordBatch.Decode(dec); err != nil {
			return res
		}
		res = append(res, recordBatch)
	}

	return res
}
