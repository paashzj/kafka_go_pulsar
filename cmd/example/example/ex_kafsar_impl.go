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

package main

type ExampleKafsarImpl struct {
}

func (e ExampleKafsarImpl) Auth(username string, password string, clientId string) (bool, error) {
	return true, nil
}

func (e ExampleKafsarImpl) AuthTopic(username string, password, clientId, topic, permissionType string) (bool, error) {
	return true, nil
}

func (e ExampleKafsarImpl) AuthTopicGroup(username string, password, clientId, consumerGroup string) (bool, error) {
	return true, nil
}

func (e ExampleKafsarImpl) SubscriptionName(groupId string) (string, error) {
	return groupId, nil
}

func (e ExampleKafsarImpl) PulsarTopic(username, topic string) (string, error) {
	return "persistent://public/default/" + topic, nil
}
