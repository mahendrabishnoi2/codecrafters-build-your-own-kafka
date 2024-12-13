package api

const NilByte = 0xff

type ErrorCode = int16

const (
	NoError                 ErrorCode = 0
	UnknownTopicOrPartition ErrorCode = 3
	ErrorUnsupportedVersion ErrorCode = 35
	ErrorUnknownTopic       ErrorCode = 100
)
