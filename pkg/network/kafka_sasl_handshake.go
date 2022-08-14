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
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (s *Server) SaslHandshake(frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 1 || version == 2 {
		return s.ReactSaslVersion(frame, version)
	}
	logrus.Error("unknown fetch version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactSaslVersion(frame []byte, version int16) ([]byte, gnet.Action) {
	req, r, stack := codec.DecodeSaslHandshakeReq(frame, version)
	if r != nil {
		logrus.Warn("decode sasl handshake error", r, string(stack))
		return nil, gnet.Close
	}
	logrus.Debug("sasl handshake request ", req)
	saslHandshakeResp := codec.SaslHandshakeResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	saslHandshakeResp.EnableMechanisms = make([]*codec.EnableMechanism, 1)
	saslHandshakeResp.EnableMechanisms[0] = &codec.EnableMechanism{SaslMechanism: "PLAIN"}
	return saslHandshakeResp.Bytes(version), gnet.None
}
