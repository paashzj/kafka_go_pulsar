// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package network

import (
	"github.com/paashzj/kafka_go_pulsar/pkg/network/ctx"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (s *Server) ReactMetadata(ctx *ctx.NetworkContext, req *codec.MetadataReq, config *KafkaProtocolConfig) (*codec.MetadataResp, gnet.Action) {
	logrus.Debug("metadata req ", req)
	topics := req.Topics
	if len(topics) > 1 {
		logrus.Error("currently, not support more than one topic", ctx.Addr)
		return nil, gnet.Close
	}
	var metadataResp = &codec.MetadataResp{
		BaseResp:                   codec.BaseResp{CorrelationId: req.CorrelationId},
		ClusterId:                  config.ClusterId,
		ControllerId:               config.NodeId,
		ClusterAuthorizedOperation: -2147483648,
		BrokerMetadataList: []*codec.BrokerMetadata{
			{NodeId: config.NodeId, Host: config.AdvertiseHost, Port: config.AdvertisePort, Rack: nil},
		},
	}
	if len(topics) == 0 {
		metadataResp.TopicMetadataList = make([]*codec.TopicMetadata, 0)
		return metadataResp, gnet.None
	}
	topic := topics[0].Topic
	partitionNum, err := s.kafsarImpl.PartitionNum(ctx.Addr, topic)
	if err != nil {
		metadataResp.TopicMetadataList = make([]*codec.TopicMetadata, 1)
		topicMetadata := codec.TopicMetadata{ErrorCode: codec.UNKNOWN_SERVER_ERROR, Topic: topic, IsInternal: false, TopicAuthorizedOperation: -2147483648}
		topicMetadata.PartitionMetadataList = make([]*codec.PartitionMetadata, 0)
		metadataResp.TopicMetadataList[0] = &topicMetadata
	} else {
		metadataResp.TopicMetadataList = make([]*codec.TopicMetadata, 1)
		topicMetadata := codec.TopicMetadata{ErrorCode: 0, Topic: topic, IsInternal: false, TopicAuthorizedOperation: -2147483648}
		topicMetadata.PartitionMetadataList = make([]*codec.PartitionMetadata, partitionNum)
		for i := 0; i < partitionNum; i++ {
			partitionMetadata := &codec.PartitionMetadata{ErrorCode: 0, PartitionId: i, LeaderId: config.NodeId, LeaderEpoch: 0, OfflineReplicas: nil}
			replicas := make([]*codec.Replica, 1)
			replicas[0] = &codec.Replica{ReplicaId: config.NodeId}
			partitionMetadata.Replicas = replicas
			partitionMetadata.CaughtReplicas = replicas
			topicMetadata.PartitionMetadataList[i] = partitionMetadata
		}
		metadataResp.TopicMetadataList[0] = &topicMetadata
	}
	return metadataResp, gnet.None
}
