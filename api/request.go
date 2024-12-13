package api

type RawRequest struct {
	RawBytes []byte
	Payload  []byte
}

func (r *RawRequest) From(messageSizeBytes, bodyBytes []byte) *RawRequest {
	r.RawBytes = append(messageSizeBytes, bodyBytes...)
	r.Payload = bodyBytes
	return r
}
