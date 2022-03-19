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

package kafsar

import (
	"container/list"
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"time"
)

type KafkaImpl struct {
	server           Server
	pulsarConfig     PulsarConfig
	pulsarClient     pulsar.Client
	groupCoordinator *GroupCoordinatorImpl
	kafsarConfig     KafsarConfig
	consumerManager  map[string]*ConsumerMetadata
	mutex            sync.RWMutex
	userInfoManager  map[string]*userInfo
	offsetManager    OffsetManager
}

type userInfo struct {
	username string
	clientId string
}

type MessageIdPair struct {
	MessageId pulsar.MessageID
	Offset    int64
}

func NewKafsar(impl Server, config *Config) (*KafkaImpl, error) {
	kafka := KafkaImpl{server: impl, pulsarConfig: config.PulsarConfig, kafsarConfig: config.KafsarConfig}
	pulsarUrl := fmt.Sprintf("pulsar://%s:%d", kafka.pulsarConfig.Host, kafka.pulsarConfig.TcpPort)
	var err error
	kafka.pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
	if err != nil {
		return nil, err
	}
	kafka.offsetManager, err = NewOffsetManager(kafka.pulsarClient, config.KafsarConfig)
	if err != nil {
		kafka.pulsarClient.Close()
	}
	kafka.groupCoordinator = NewGroupCoordinator(kafka.pulsarConfig, kafka.kafsarConfig, kafka.pulsarClient)
	kafka.consumerManager = make(map[string]*ConsumerMetadata)
	kafka.userInfoManager = make(map[string]*userInfo)
	return &kafka, nil
}

func (k *KafkaImpl) Produce(addr net.Addr, topic string, partition int, req *service.ProducePartitionReq) (*service.ProducePartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) FetchPartition(addr net.Addr, topic string, req *service.FetchPartitionReq) (*service.FetchPartitionResp, error) {
	user, exist := k.userInfoManager[addr.String()]
	if !exist {
		logrus.Errorf("fetch partition failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s fetch topic: %s partition %d", addr.String(), topic, req.PartitionId)
	fullNameTopic, err := k.server.KafkaConsumeTopic(user.username, topic)
	if err != nil {
		logrus.Errorf("fetch partition failed when get pulsar topic %s, kafka topic: %s", addr.String(), topic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	k.mutex.RLock()
	consumerMetadata, exist := k.consumerManager[fullNameTopic]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("can not find consumer for topic: %s when fetch partition", fullNameTopic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	var records []*service.Record
	recordBatch := service.RecordBatch{Records: records}
	var baseOffset int64
	fistMessage := true
	fetchStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(k.kafsarConfig.MaxFetchWaitMs)*time.Millisecond)
	defer cancel()
OUT:
	for {
		select {
		case channel := <-consumerMetadata.channel:
			message := channel.Message
			logrus.Infof("receive msg: %s from %s", message.ID(), message.Topic())
			offset := convOffset(message, k.kafsarConfig.ContinuousOffset)
			if fistMessage {
				fistMessage = false
				baseOffset = offset
			}
			relativeOffset := offset - baseOffset
			record := service.Record{
				Value:          message.Payload(),
				RelativeOffset: int(relativeOffset),
			}
			recordBatch.Records = append(recordBatch.Records, &record)
			consumerMetadata.messageIds.PushBack(MessageIdPair{
				MessageId: message.ID(),
				Offset:    offset,
			})
			if time.Since(fetchStart).Milliseconds() >= int64(k.kafsarConfig.MaxFetchWaitMs) || len(recordBatch.Records) >= k.kafsarConfig.MaxFetchRecord {
				break OUT
			}
		case <-ctx.Done():
			logrus.Debugf("fetch empty message for topic %s", fullNameTopic)
			break OUT
		}
	}
	recordBatch.Offset = baseOffset
	return &service.FetchPartitionResp{
		ErrorCode:        service.NONE,
		PartitionId:      req.PartitionId,
		LastStableOffset: 0,
		LogStartOffset:   0,
		RecordBatch:      &recordBatch,
	}, nil
}

func (k *KafkaImpl) GroupJoin(addr net.Addr, req *service.JoinGroupReq) (*service.JoinGroupResp, error) {
	logrus.Infof("%s joining to group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	joinGroupResp, err := k.groupCoordinator.HandleJoinGroup(req.GroupId, req.MemberId, req.ClientId, req.ProtocolType,
		req.SessionTimeout, req.GroupProtocols)
	if err != nil {
		logrus.Errorf("unexpected exception in join group: %s, error: %s", req.GroupId, err)
		return &service.JoinGroupResp{
			ErrorCode:    service.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	return joinGroupResp, nil
}

func (k *KafkaImpl) GroupLeave(addr net.Addr, req *service.LeaveGroupReq) (*service.LeaveGroupResp, error) {
	logrus.Infof("%s leaving group: %s, members: %+v", addr.String(), req.GroupId, req.Members)
	leaveGroupResp, err := k.groupCoordinator.HandleLeaveGroup(req.GroupId, req.Members)
	if err != nil {
		logrus.Errorf("unexpected exception in leaving group: %s, error: %s", req.GroupId, err)
		return &service.LeaveGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	k.groupCoordinator.mutex.RLock()
	group, exist := k.groupCoordinator.groupManager[req.GroupId]
	k.groupCoordinator.mutex.RUnlock()
	if exist {
		delete(k.consumerManager, group.topic)
	}
	return leaveGroupResp, nil
}

func (k *KafkaImpl) GroupSync(addr net.Addr, req *service.SyncGroupReq) (*service.SyncGroupResp, error) {
	logrus.Infof("%s syncing group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	syncGroupResp, err := k.groupCoordinator.HandleSyncGroup(req.GroupId, req.MemberId, req.GenerationId, req.GroupAssignments)
	if err != nil {
		logrus.Errorf("unexpected exception in sync group: %s, error: %s", req.GroupId, err)
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	return syncGroupResp, nil
}

func (k *KafkaImpl) OffsetListPartition(addr net.Addr, topic string, req *service.ListOffsetsPartitionReq) (*service.ListOffsetsPartitionResp, error) {
	userInfo, exist := k.userInfoManager[addr.String()]
	if !exist {
		logrus.Errorf("offset list failed when get username by addr %s, kafka topic: %s", addr.String(), topic)
		return &service.ListOffsetsPartitionResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s offset list topic: %s, partition: %d", addr.String(), topic, req.PartitionId)
	fullTopicName, err := k.server.KafkaConsumeTopic(userInfo.username, topic)
	if err != nil {
		logrus.Errorf("get topic failed. err: %s", err)
		return &service.ListOffsetsPartitionResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	offset := constant.DefaultOffset
	if req.Time == constant.TimeLasted {
		msg, err := utils.GetLatestMsgId(topic, fullTopicName, req.PartitionId, k.getPulsarHttpUrl())
		if err != nil {
			logrus.Errorf("get topic %s latest offset failed %s\n", topic, err)
			return &service.ListOffsetsPartitionResp{
				PartitionId: req.PartitionId,
				Offset:      offset,
				Time:        constant.TimeEarliest,
			}, nil
		}
		lastedMsg := utils.ReadLastedMsg(fullTopicName, k.kafsarConfig.MaxFetchWaitMs, req.PartitionId, msg, k.pulsarClient)
		if lastedMsg != nil {
			offset = convOffset(lastedMsg, k.kafsarConfig.ContinuousOffset)
		}
	}
	if req.Time == constant.TimeEarliest {
		message := utils.ReadEarliestMsg(fullTopicName, k.kafsarConfig.MaxFetchWaitMs, req.PartitionId, k.pulsarClient)
		if message != nil {
			offset = convOffset(message, k.kafsarConfig.ContinuousOffset)
		}
	}
	return &service.ListOffsetsPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      offset,
		Time:        constant.TimeEarliest,
	}, nil
}

func (k *KafkaImpl) OffsetCommitPartition(addr net.Addr, topic string, req *service.OffsetCommitPartitionReq) (*service.OffsetCommitPartitionResp, error) {
	user, exist := k.userInfoManager[addr.String()]
	if !exist {
		logrus.Errorf("offset commit failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s topic: %s, partition: %d, commit offset: %d", addr.String(), topic, req.PartitionId, req.OffsetCommitOffset)
	fullNameTopic, err := k.server.KafkaConsumeTopic(user.username, topic)
	if err != nil {
		logrus.Errorf("offset commit failed when get pulsar topic %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	k.mutex.RLock()
	consumerMessages, exist := k.consumerManager[fullNameTopic]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("commit offset failed, topic: %s, does not exist", fullNameTopic)
		return &service.OffsetCommitPartitionResp{ErrorCode: service.UNKNOWN_TOPIC_ID}, nil
	}
	ids := consumerMessages.messageIds
	front := ids.Front()
	for element := front; element != nil; element = element.Next() {
		messageIdPair := element.Value.(MessageIdPair)
		if messageIdPair.Offset > req.OffsetCommitOffset {
			break
		}
		logrus.Infof("ack pulsar %s for %s", fullNameTopic, messageIdPair.MessageId)
		consumerMessages.consumer.AckID(messageIdPair.MessageId)
		consumerMessages.messageIds.Remove(element)
	}
	return &service.OffsetCommitPartitionResp{
		PartitionId: req.PartitionId,
		ErrorCode:   service.NONE,
	}, nil
}

func (k *KafkaImpl) OffsetFetch(addr net.Addr, topic string, req *service.OffsetFetchPartitionReq) (*service.OffsetFetchPartitionResp, error) {
	user, exist := k.userInfoManager[addr.String()]
	if !exist {
		logrus.Errorf("offset fetch failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetFetchPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	logrus.Infof("%s fetch topic: %s offset, partition: %d", addr.String(), topic, req.PartitionId)
	fullNameTopic, err := k.server.KafkaConsumeTopic(user.username, topic)
	if err != nil {
		logrus.Errorf("offset fetch failed when get pulsar topic %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetFetchPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	subscriptionName, err := k.server.SubscriptionName(req.GroupId)
	if err != nil {
		logrus.Errorf("sync group %s failed when offset fetch, error: %s", req.GroupId, err)
	}
	k.mutex.RLock()
	consumerMetadata, exist := k.consumerManager[fullNameTopic]
	k.mutex.RUnlock()
	if !exist {
		k.mutex.Lock()
		metadata := ConsumerMetadata{groupId: req.GroupId, messageIds: list.New()}
		channel, consumer, err := k.createConsumer(fullNameTopic, req.PartitionId, subscriptionName)
		if err != nil {
			logrus.Errorf("%s, create channel failed, error: %s", topic, err)
			return &service.OffsetFetchPartitionResp{
				ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
			}, nil
		}
		metadata.consumer = consumer
		metadata.channel = channel
		k.consumerManager[fullNameTopic] = &metadata
		consumerMetadata = &metadata
		k.mutex.Unlock()
	}
	k.mutex.RLock()
	group := k.groupCoordinator.groupManager[req.GroupId]
	k.mutex.RUnlock()
	group.topic = fullNameTopic
	group.consumerMetadata = consumerMetadata
	return &service.OffsetFetchPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      constant.UnknownOffset,
		LeaderEpoch: -1,
		Metadata:    nil,
		ErrorCode:   int16(service.NONE),
	}, nil
}

func (k *KafkaImpl) OffsetLeaderEpoch(addr net.Addr, topic string, req *service.OffsetLeaderEpochPartitionReq) (*service.OffsetLeaderEpochPartitionResp, error) {
	//TODO implement me
	panic("implement me")
}

func (k *KafkaImpl) SaslAuth(addr net.Addr, req service.SaslReq) (bool, service.ErrorCode) {
	auth, err := k.server.Auth(req.Username, req.Password, req.ClientId)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	_, exist := k.userInfoManager[addr.String()]
	if !exist {
		k.userInfoManager[addr.String()] = &userInfo{
			username: req.Username,
			clientId: req.ClientId,
		}
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthTopic(addr net.Addr, req service.SaslReq, topic, permissionType string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopic(req.Username, req.Password, req.ClientId, topic, permissionType)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthConsumerGroup(addr net.Addr, req service.SaslReq, consumerGroup string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopicGroup(req.Username, req.Password, req.ClientId, consumerGroup)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) Disconnect(addr net.Addr) {
	logrus.Infof("lost connection: %s", addr)
	delete(k.userInfoManager, addr.String())
}

func (k *KafkaImpl) Close() {
	k.offsetManager.Close()
	k.pulsarClient.Close()
}

func (k *KafkaImpl) createConsumer(topic string, partition int, subscriptionName string) (chan pulsar.ConsumerMessage, pulsar.Consumer, error) {
	channel := make(chan pulsar.ConsumerMessage, k.kafsarConfig.ConsumerReceiveQueueSize)
	options := pulsar.ConsumerOptions{
		Topic:                       topic + fmt.Sprintf(constant.PartitionSuffixFormat, partition),
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Failover,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              channel,
		ReceiverQueueSize:           k.kafsarConfig.ConsumerReceiveQueueSize,
	}
	consumer, err := k.pulsarClient.Subscribe(options)
	if err != nil {
		return nil, nil, err
	}
	return channel, consumer, nil
}

func (k *KafkaImpl) getPulsarHttpUrl() string {
	return fmt.Sprintf("http://%s:%d", k.pulsarConfig.Host, k.pulsarConfig.HttpPort)
}
