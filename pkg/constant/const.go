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

package constant

import "time"

const (
	DefaultOffset = int64(0)
	UnknownOffset = int64(-1)

	TimeEarliest = int64(-2)
	TimeLasted   = int64(-1)

	OffsetReaderEarliestName = "OFFSET_LIST_EARLIEST"

	DefaultProducerSendTimeout = 1 * time.Second
	DefaultMaxPendingMsg       = 100

	PartitionSuffixFormat = "-partition-%d"
)

const (
	LastMsgIdUrl = "/admin/v2/persistent/%s/%s/%s/lastMessageId"
)

const (
	ReadMsgTimeoutErr = "context deadline exceeded"
)
