package api

import (
	"github.com/codecrafters-io/kafka-starter-go/protocol/encoder"
)

type ApiVersionsResponse struct {
	Header ResponseHeader
	Body   ApiVersionsResponseBody
}

type ApiVersionsResponseBody struct {
	ErrorCode    int16
	ApiVersions  []ApiVersion
	ThrottleTime int32
}

func (b *ApiVersionsResponseBody) Encode(enc *encoder.BinaryEncoder) error {
	// https://binspec.org/kafka-api-versions-Response-v4
	enc.PutInt16(b.ErrorCode)
	if b.ErrorCode != NoError {
		return nil
	}

	enc.PutCompactArrayLen(len(b.ApiVersions))
	for _, v := range b.ApiVersions {
		if err := v.Encode(enc); err != nil {
			return err
		}
	}

	enc.PutInt32(b.ThrottleTime)
	enc.PutEmptyTaggedFieldArray()

	return nil
}

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (a *ApiVersion) Encode(enc *encoder.BinaryEncoder) error {
	enc.PutInt16(a.ApiKey)
	enc.PutInt16(a.MinVersion)
	enc.PutInt16(a.MaxVersion)
	enc.PutEmptyTaggedFieldArray()
	return nil
}

func (a *ApiVersionsResponse) Encode(enc *encoder.BinaryEncoder) error {
	if err := a.Header.EncodeV0(enc); err != nil {
		return err
	}

	if err := a.Body.Encode(enc); err != nil {
		return err
	}
	return nil
}

// func (a ApiVersionsResponse) Bytes() ([]byte, error) {
//
// correlation id - 4 bytes
// error code - 2 bytes
// api versions array length - 1 byte
// array length * (api key - 2 byte, min version - 2 byte, max version - 2 byte, tag buffer - 1 byte = 7 bytes)
// throttle time - 4 bytes
// tag buffer - 1 byte

// size := 4 + 2 + 1 + len(a.Body.ApiVersions)*7 + 4 + 1
// if a.Body.ErrorCode != NoError {
// 	size = 6 // correlation id + error code
// 	out := make([]byte, size+4)
// 	binary.BigEndian.PutUint32(out[0:4], uint32(size))
// 	binary.BigEndian.PutUint32(out[4:8], uint32(a.Header.CorrelationId))
// 	binary.BigEndian.PutUint16(out[8:], uint16(a.Body.ErrorCode))
// 	return out, nil
// }
//
// out := make([]byte, size+4)

// // message size
// binary.BigEndian.PutUint32(out[0:4], uint32(size))

// // correlation id
// binary.BigEndian.PutUint32(out[4:8], uint32(a.Header.CorrelationId))
//
// // error code
// binary.BigEndian.PutUint16(out[8:10], uint16(a.Body.ErrorCode))

// // api versions array length
// out[10] = byte(len(a.Body.ApiVersions) + 1)

// // api versions array
// for i, v := range a.Body.ApiVersions {
// 	offset := 11 + i*7
// 	binary.BigEndian.PutUint16(out[offset:offset+2], uint16(v.ApiKey))
// 	binary.BigEndian.PutUint16(out[offset+2:offset+4], uint16(v.MinVersion))
// 	binary.BigEndian.PutUint16(out[offset+4:offset+6], uint16(v.MaxVersion))
// 	out[offset+6] = 0
// }

// // throttle time
// binary.BigEndian.PutUint32(out[size-5:size-1], uint32(a.Body.ThrottleTime))
//
// // tag buffer
// out[size-1] = 0
//
// return out, nil
// }
