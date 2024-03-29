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
	"github.com/paashzj/kafka_go_pulsar/pkg/test"
	"github.com/protocol-laboratory/kafka-codec-go/kgnet"
)

func SetupKafsar() (*Broker, int) {
	port, err := test.AcquireUnusedPort()
	if err != nil {
		panic(err)
	}
	kafsarImpl, err := setupKafsarInternal(port)
	if err != nil {
		panic(err)
	}
	return kafsarImpl, port
}

func setupKafsarInternal(port int) (*Broker, error) {
	config := &Config{}
	gnetConfig := kgnet.GnetServerConfig{
		ListenHost: "localhost",
		ListenPort: port,
	}
	config.PulsarConfig = PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	config.KafsarConfig.GnetConfig = gnetConfig
	config.KafsarConfig.AdvertiseHost = "localhost"
	config.KafsarConfig.AdvertisePort = port
	config.KafsarConfig.MaxConsumersPerGroup = 100
	config.KafsarConfig.GroupMaxSessionTimeoutMs = 60000
	config.KafsarConfig.GroupMinSessionTimeoutMs = 0
	config.KafsarConfig.MaxFetchRecord = 10
	config.KafsarConfig.MinFetchWaitMs = 10
	config.KafsarConfig.MaxFetchWaitMs = 100
	config.KafsarConfig.PulsarTenant = "public"
	config.KafsarConfig.PulsarNamespace = "default"
	config.KafsarConfig.OffsetTopic = "kafka_offset"
	kafsarImpl := &test.KafsarImpl{}
	server, err := NewKafsar(kafsarImpl, config)
	if err != nil {
		return nil, err
	}
	err = server.Run()
	if err != nil {
		return nil, err
	}
	return server, err
}
