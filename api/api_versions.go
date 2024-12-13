package api

func PrepareAPIVersionsResponse(msg *Message) ApiVersionsResponse {
	resp := ApiVersionsResponse{
		Header: ResponseHeader{CorrelationId: msg.Header.CorrelationId},
		Body: ApiVersionsResponseBody{
			ErrorCode: msg.Error,
		},
	}

	if msg.Error == NoError {
		resp.Body.ApiVersions = []ApiVersion{
			{ApiKey: ApiVersions, MinVersion: 0, MaxVersion: 5},
			{ApiKey: DescribeTopicPartitions, MinVersion: 0, MaxVersion: 11},
			{ApiKey: Fetch, MinVersion: 0, MaxVersion: 17},
		}
	}

	return resp
}
