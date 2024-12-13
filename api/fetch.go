package api

func PrepareFetchResponse(msg *Message) FetchResponse {
	resp := FetchResponse{
		Header: ResponseHeader{CorrelationId: msg.Header.CorrelationId},
		Body: FetchResponseBody{
			ThrottleTimeMs: 0,
			ErrorCode:      NoError,
			SessionId:      0,
		},
	}
	req := msg.RequestBody.(FetchRequestBody)
	topicResponses := make([]FetchResponseTopic, len(req.Topics))
	for i, topic := range req.Topics {
		partitionResponses := make([]FetchResponsePartition, len(topic.Partitions))
		for j, partition := range topic.Partitions {
			partitionResponses[j] = FetchResponsePartition{
				PartitionIndex:      partition.PartitionIndex,
				ErrorCode:           ErrorUnknownTopic,
				HighWatermark:       0,
				LastStableOffset:    0,
				LogStartOffset:      0,
				AbortedTransactions: nil,
				Records:             nil,
			}
		}
		topicResponses[i] = FetchResponseTopic{
			TopicID:    topic.TopicID,
			Partitions: partitionResponses,
		}
	}
	resp.Body.Responses = topicResponses

	return resp
}
