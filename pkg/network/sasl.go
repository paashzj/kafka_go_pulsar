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
	"github.com/paashzj/kafka_go_pulsar/pkg/service"
)

func (s *Server) checkSasl(ctx *ctx.NetworkContext) bool {
	if !s.kafkaProtocolConfig.NeedSasl {
		return true
	}
	_, ok := s.SaslMap.Load(ctx.Addr)
	return ok
}

func (s *Server) checkSaslGroup(ctx *ctx.NetworkContext, groupId string) bool {
	if !s.kafkaProtocolConfig.NeedSasl {
		return true
	}
	saslReq, ok := s.SaslMap.Load(ctx.Addr)
	if !ok {
		return false
	}
	res, code := s.kafsarImpl.SaslAuthConsumerGroup(ctx.Addr, saslReq.(service.SaslReq), groupId)
	if code != 0 || !res {
		return false
	}
	return true
}

func (s *Server) checkSaslTopic(ctx *ctx.NetworkContext, topic, permissionType string) bool {
	if !s.kafkaProtocolConfig.NeedSasl {
		return true
	}
	saslReq, ok := s.SaslMap.Load(ctx.Addr)
	if !ok {
		return false
	}
	res, code := s.kafsarImpl.SaslAuthTopic(ctx.Addr, saslReq.(service.SaslReq), topic, permissionType)
	if code != 0 || !res {
		return false
	}
	return true
}
