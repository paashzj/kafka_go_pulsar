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
	"github.com/paashzj/kafka_go_pulsar/pkg/network"
	"github.com/paashzj/kafka_go_pulsar/pkg/service"
	"github.com/protocol-laboratory/kafka-codec-go/kgnet"
	"github.com/sirupsen/logrus"
)

type Config struct {
	PulsarConfig PulsarConfig
	KafsarConfig KafsarConfig
	TraceConfig  TraceConfig
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

type KafsarConfig struct {
	// network config
	GnetConfig kgnet.GnetConfig
	NeedSasl   bool
	MaxConn    int32

	// Kafka protocol config
	ClusterId     string
	AdvertiseHost string
	AdvertisePort uint16

	MaxConsumersPerGroup     int
	GroupMinSessionTimeoutMs int
	GroupMaxSessionTimeoutMs int
	ConsumerReceiveQueueSize int
	MaxFetchRecord           int
	MinFetchWaitMs           int
	MaxFetchWaitMs           int
	ContinuousOffset         bool
	// PulsarTenant use for kafsar internal
	PulsarTenant string
	// PulsarNamespace use for kafsar internal
	PulsarNamespace string
	// OffsetTopic use to store kafka offset
	OffsetTopic string
	// GroupCoordinatorType enum: Standalone, Cluster; default Standalone
	GroupCoordinatorType GroupCoordinatorType
	// InitialDelayedJoinMs
	InitialDelayedJoinMs int
	// RebalanceTickMs
	RebalanceTickMs int
}

type TraceConfig struct {
	DisableTracing bool
	SkywalkingHost string
	SkywalkingPort int
	SampleRate     float64
}

type Broker struct {
	Config *Config
	aux    Server
	impl   *KafkaImpl
}

func (b *Broker) Close() {
	b.impl.Close()
}

func (b *Broker) Run() error {
	logrus.Info("kafsar started")
	k, err := newKafsar(b.aux, b.Config)
	if err != nil {
		return err
	}
	_, err = runKafka(&b.Config.KafsarConfig, k)
	if err != nil {
		return err
	}
	b.impl = k
	return nil
}

func runKafka(config *KafsarConfig, impl service.KfkServer) (*ServerControl, error) {
	kfkProtocolConfig := &network.KafkaProtocolConfig{}
	kfkProtocolConfig.ClusterId = config.ClusterId
	kfkProtocolConfig.AdvertiseHost = config.AdvertiseHost
	kfkProtocolConfig.AdvertisePort = config.AdvertisePort
	kfkProtocolConfig.NeedSasl = config.NeedSasl
	kfkProtocolConfig.MaxConn = config.MaxConn
	serverControl := &ServerControl{}
	var err error
	serverControl.networkServer, err = network.Run(&config.GnetConfig, kfkProtocolConfig, impl)
	return serverControl, err
}

func NewKafsarServer(config *Config, impl Server) *Broker {
	return &Broker{
		Config: config,
		aux:    impl,
	}
}
