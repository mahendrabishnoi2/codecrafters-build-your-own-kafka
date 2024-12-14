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
		partitionResponses := []FetchResponsePartition{}
		clusterMetadata := GetClusterMetadata("__cluster_metadata", 0)
		topicRec := clusterMetadata.GetTopicByID(topic.TopicID)
		if topicRec == nil {
			partitionResponses = append(partitionResponses, FetchResponsePartition{
				PartitionIndex:      0,
				ErrorCode:           ErrorUnknownTopic,
				HighWatermark:       0,
				LastStableOffset:    0,
				LogStartOffset:      0,
				AbortedTransactions: nil,
				Records:             nil,
			})
		} else {
			for _, partition := range topic.Partitions {
				partitionMetadata := GetClusterMetadata(topicRec.TopicName, partition.PartitionIndex)
				partitionResponses = append(partitionResponses, FetchResponsePartition{
					PartitionIndex:       partition.PartitionIndex,
					ErrorCode:            0,
					HighWatermark:        0,
					LastStableOffset:     0,
					LogStartOffset:       0,
					AbortedTransactions:  nil,
					PreferredReadReplica: 0,
					Records:              partitionMetadata.RecordBatches,
				})
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
