package api

import (
	"github.com/google/uuid"
)

func PrepareDescribeTopicPartitionsResponse(msg *Message) DescribeTopicPartitionsResponse {
	clusterMetadata := GetClusterMetadata("__cluster_metadata", 0)
	req := msg.RequestBody.(DescribeTopicPartitionsRequestBody)
	resp := DescribeTopicPartitionsResponse{
		Header: ResponseHeader{
			CorrelationId: msg.Header.CorrelationId,
		},
		Body: DescribeTopicPartitionsResponseV0ResponseBody{
			ThrottleTime: 0,
			Topics:       nil,
			Cursor:       Cursor{NextCursor: nil},
		},
	}
	for _, topic := range req.TopicNames {
		topicRecord := clusterMetadata.GetTopicByName(topic.Name)
		if topicRecord == nil {
			resp.Body.Topics = append(resp.Body.Topics, DescribeTopicPartitionsResponseV0Topic{
				ErrorCode:            UnknownTopicOrPartition,
				Name:                 topic.Name,
				ID:                   uuid.UUID{},
				IsInternal:           0,
				Partitions:           nil,
				AuthorizedOperations: 3576, // hardcoded for stage vt6
			})
			continue
		}
		partitionRecords := clusterMetadata.GetPartitionByTopicId(topicRecord.TopicUUID)
		partitions := make([]Partition, 0, len(partitionRecords))
		for _, partition := range partitionRecords {
			partitions = append(partitions, Partition{
				ErrorCode:       NoError,
				PartitionIndex:  partition.PartitionID,
				LeaderID:        partition.Leader,
				LeaderEpoch:     partition.LeaderEpoch,
				ReplicaNodes:    partition.Replicas,
				ISRNodes:        partition.InSyncReplicas,
				ELRs:            nil,
				LastKnownELRs:   nil,
				OffLineReplicas: nil,
			})
		}
		resp.Body.Topics = append(resp.Body.Topics, DescribeTopicPartitionsResponseV0Topic{
			ErrorCode:            NoError,
			Name:                 topicRecord.TopicName,
			ID:                   topicRecord.TopicUUID,
			IsInternal:           0,
			Partitions:           partitions,
			AuthorizedOperations: 3576,
		})
	}
	return resp
}
