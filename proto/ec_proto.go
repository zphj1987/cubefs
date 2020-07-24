// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package proto

type InodeExtentInfo struct {
	Inode   uint64      `json:"ino"`
	Extents []ExtentKey `json:"eks"`
}

type IssueMigrationTaskRequest struct {
	VolName     string   `json:"vol"`
	PartitionId uint64   `json:"pid"` // meta partition id
	Inodes      []uint64 `json:"inos"`
}

type GetMigrationTaskStatusRequest struct {
	VolName     string `json:"vol"`
	PartitionId uint64 `json:"pid"`
}

type GetMigrationTaskStatusResponse struct {
	VolName     string   `json:"vol"`
	PartitionId uint64   `json:"pid"`
	Inodes      []uint64 `json:"inos"`
}

type BatchCompleteMigrateRequest struct {
	VolName     string            `json:"vol"`
	PartitionId uint64            `json:"pid"`
	Inodes      []InodeExtentInfo `json:"inos"`
}
