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

package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/model"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

func PartitionedTopic(topic string, partition int) string {
	return topic + fmt.Sprintf(constant.PartitionSuffixFormat, partition)
}

func ReadEarliestMsg(partitionedTopic string, maxWaitMs int, pulsarClient pulsar.Client) (pulsar.Message, error) {
	readerOptions := pulsar.ReaderOptions{
		Topic:          partitionedTopic,
		Name:           constant.OffsetReaderEarliestName,
		StartMessageID: pulsar.EarliestMessageID(),
	}

	return readNextMsg(readerOptions, maxWaitMs, pulsarClient)
}

func GetLatestMsgId(partitionedTopic, addr string) (msg []byte, err error) {
	tenant, namespace, shortPartitionedTopic, err := getTenantNamespaceTopicFromPartitionedTopic(partitionedTopic)
	if err != nil {
		logrus.Errorf("get tenant and namespace failed. topic: %s, err: %s", partitionedTopic, err)
		return nil, err
	}
	urlFormat := addr + constant.LastMsgIdUrl
	url := fmt.Sprintf(urlFormat, tenant, namespace, shortPartitionedTopic)
	msg, err = HttpGet(url, nil, nil)
	if err != nil {
		logrus.Errorf("unmarshal message id failed., topic: %s, err: %s", partitionedTopic, err)
		return nil, err
	}
	return msg, nil
}

func ReadLastedMsg(partitionedTopic string, maxWaitMs int, msgIdBytes []byte, pulsarClient pulsar.Client) (pulsar.Message, error) {
	var msgId pulsar.MessageID
	bytes, err := generateMsgBytes(msgIdBytes)
	if err != nil {
		logrus.Errorf("genrate msg bytes failed. topic: %s, err: %s", partitionedTopic, err)
		return nil, err
	}
	msgId, err = pulsar.DeserializeMessageID(bytes)
	if err != nil {
		logrus.Errorf("deserialize messageId failed. msgBytes: %s, topic: %s, err: %s", string(msgIdBytes), partitionedTopic, err)
		return nil, err
	}
	readerOptions := pulsar.ReaderOptions{
		Topic:                   partitionedTopic,
		Name:                    constant.OffsetReaderEarliestName,
		StartMessageID:          msgId,
		StartMessageIDInclusive: true,
	}
	return readNextMsg(readerOptions, maxWaitMs, pulsarClient)
}

func getTenantNamespaceTopicFromPartitionedTopic(partitionedTopic string) (tenant, namespace, shortPartitionedTopic string, err error) {
	if strings.Contains(partitionedTopic, "//") {
		topicArr := strings.Split(partitionedTopic, "//")
		if len(topicArr) < 2 {
			return "", "", "", errors.New("get tenant and namespace failed")
		}
		list := strings.Split(topicArr[1], "/")
		if len(list) < 3 {
			return "", "", "", errors.New("get tenant and namespace failed")
		}
		return list[0], list[1], list[2], nil
	}
	return "", "", "", errors.New("get tenant and namespace failed")
}

func readNextMsg(operation pulsar.ReaderOptions, maxWaitMs int, pulsarClient pulsar.Client) (pulsar.Message, error) {
	reader, err := pulsarClient.CreateReader(operation)
	if err != nil {
		logrus.Warnf("create pulsar lasted read failed. topic: %s, err: %s", operation.Topic, err)
		return nil, err
	}
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxWaitMs)*time.Millisecond)
	defer cancel()
	message, err := reader.Next(ctx)
	if err != nil {
		logrus.Errorf("get message failed. topic: %s, err: %s", operation.Topic, err)
		if strings.Contains(err.Error(), constant.ReadMsgTimeoutErr) {
			return message, nil
		}
		return nil, err
	}
	return message, nil
}

func CalculateMsgLength(message pulsar.Message) int {
	length := 0
	length += len([]byte(message.Key()))
	length += len(message.Payload())
	properties := message.Properties()
	for key, value := range properties {
		length += len([]byte(key))
		length += len([]byte(value))
	}
	return length
}

func generateMsgBytes(msgBytes []byte) ([]byte, error) {
	var msgId model.MessageID
	err := json.Unmarshal(msgBytes, &msgId)
	if err != nil {
		logrus.Errorf("unmarsha failed. msg: %s, err: %s", string(msgBytes), err)
		return nil, err
	}
	pulsarMessageData := pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(msgId.LedgerID)),
		EntryId:    proto.Uint64(uint64(msgId.EntryID)),
		BatchIndex: proto.Int32(msgId.BatchIdx),
		Partition:  proto.Int32(msgId.PartitionIdx),
	}
	data, err := proto.Marshal(&pulsarMessageData)
	if err != nil {
		logrus.Errorf("unmarsha failed. msg: %s, err: %s", string(msgBytes), err)
		return nil, err
	}
	return data, nil
}
