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

package test

import (
	"bytes"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:       1,
			IdleConnTimeout:    5 * time.Second,
			DisableCompression: false,
		},
	}
}

func AcquireUnusedPort() (int, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func HttpGetRequest(url string) ([]byte, error) {
	var body bytes.Buffer
	request, err := http.NewRequest(http.MethodGet, url, &body)
	if err != nil {
		return nil, err
	}
	defer request.Body.Close()
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(response.Body)
}

func ConvertOffset(messageId pulsar.MessageID) int64 {
	offset, _ := strconv.Atoi(fmt.Sprint(messageId.LedgerID()) + fmt.Sprint(messageId.EntryID()) + fmt.Sprint(messageId.PartitionIdx()))
	return int64(offset)
}
