package api

import (
	"github.com/google/uuid"
)

func getTopicFromMetadata(metadata []RecordBatch, topicName string) *TopicRecord {
	for _, recordBatch := range metadata {
		for _, record := range recordBatch.Records {
			recordValue := ClusterMetadataRecordValue{}
			_ = recordValue.DecodeBytes(record.Value)
			switch recordValue.Type {
			case 2:
				topicRecord := recordValue.Data.(*TopicRecord)
				if topicRecord.TopicName == topicName {
					return topicRecord
				}
			}
		}
	}
	return nil
}

func getPartitionsFromMetadata(metadata []RecordBatch, topicId uuid.UUID) []*PartitionRecord {
	var partitions []*PartitionRecord
	for _, recordBatch := range metadata {
		for _, record := range recordBatch.Records {
			recordValue := ClusterMetadataRecordValue{}
			_ = recordValue.DecodeBytes(record.Value)
			switch recordValue.Type {
			case 3:
				partitionRecord := recordValue.Data.(*PartitionRecord)
				if partitionRecord.TopicUUID == topicId {
					partitions = append(partitions, partitionRecord)
				}
			}
		}
	}
	return partitions
}

func PrepareDescribeTopicPartitionsResponse(msg *Message) DescribeTopicPartitionsResponse {
	topicMetadata := GetTopicMetadata()
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
		topicRecord := getTopicFromMetadata(topicMetadata, topic.Name)
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
		partitionRecords := getPartitionsFromMetadata(topicMetadata, topicRecord.TopicUUID)
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
