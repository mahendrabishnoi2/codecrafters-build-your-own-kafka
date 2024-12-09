package api

type ApiKey = int16

const (
	ApiVersions             ApiKey = 18
	DescribeTopicPartitions ApiKey = 75
)

type RequestHeader struct {
	ApiKey        ApiKey
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

type ResponseHeader struct {
	CorrelationId int32
}
