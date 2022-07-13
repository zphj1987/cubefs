// Copyright 2018 The Chubao Authors.
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

package master

import (
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
)

// DataNode stores all the information about a data node
type DataNode struct {
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	AvailableSpace            uint64
	ID                        uint64
	ZoneName                  string `json:"Zone"`
	Addr                      string
	ReportTime                time.Time
	StartTime                 int64
	isActive                  bool
	sync.RWMutex              `graphql:"-"`
	UsageRatio                float64           // used / total space
	SelectedTimes             uint64            // number times that this datanode has been selected as the location for a data partition.
	Carry                     float64           // carry is a factor used in cacluate the node's weight
	TaskManager               *AdminTaskManager `graphql:"-"`
	DataPartitionReports      []*proto.PartitionReport
	DataPartitionCount        uint32
	TotalPartitionSize        uint64
	NodeSetID                 uint64
	PersistenceDataPartitions []uint64
	BadDisks                  []string
	ToBeOffline               bool
	RdOnly                    bool
	MigrateLock               sync.RWMutex
}

func newDataNode(addr, zoneName, clusterID string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.Carry = rand.Float64()
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.ZoneName = zoneName
	dataNode.TaskManager = newAdminTaskManager(dataNode.Addr, clusterID)
	return
}

func (dataNode *DataNode) checkLiveness() {
	dataNode.Lock()
	defer dataNode.Unlock()
	log.LogInfof("action[checkLiveness] datanode[%v] report time[%v],since report time[%v], need gap [%v]",
		dataNode.Addr, dataNode.ReportTime, time.Since(dataNode.ReportTime), time.Second*time.Duration(defaultNodeTimeOutSec))
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(defaultNodeTimeOutSec) {
		dataNode.isActive = false
	}

	return
}

func (dataNode *DataNode) badPartitions(diskPath string, c *Cluster) (partitions []*DataPartition) {
	partitions = make([]*DataPartition, 0)
	vols := c.copyVols()
	if len(vols) == 0 {
		return partitions
	}
	for _, vol := range vols {
		dps := vol.dataPartitions.checkBadDiskDataPartitions(diskPath, dataNode.Addr)
		partitions = append(partitions, dps...)
	}
	return
}

func (dataNode *DataNode) updateNodeMetric(resp *proto.DataNodeHeartbeatResponse) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Total = resp.Total
	dataNode.Used = resp.Used
	dataNode.AvailableSpace = resp.Available
	dataNode.ZoneName = resp.ZoneName
	dataNode.DataPartitionCount = resp.CreatedPartitionCnt
	dataNode.DataPartitionReports = resp.PartitionReports
	dataNode.TotalPartitionSize = resp.TotalPartitionSize
	dataNode.BadDisks = resp.BadDisks
	dataNode.StartTime = resp.StartTime
	if dataNode.Total == 0 {
		dataNode.UsageRatio = 0.0
	} else {
		dataNode.UsageRatio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	}
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true
}

func (dataNode *DataNode) canAlloc() bool {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if !overSoldLimit() {
		return true
	}

	maxCapacity := overSoldCap(dataNode.Total)
	if maxCapacity < dataNode.TotalPartitionSize {
		return false
	}

	return true
}

func (dataNode *DataNode) isWriteAble() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive && dataNode.AvailableSpace > 10*util.GB && !dataNode.RdOnly {
		ok = true
	}

	return
}

func (dataNode *DataNode) dpCntInLimit() bool {
	return dataNode.DataPartitionCount <= dpCntOneNodeLimit()
}

func (dataNode *DataNode) isWriteAbleWithSize(size uint64) (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive == true && dataNode.AvailableSpace > size {
		ok = true
	}

	return
}

func (dataNode *DataNode) isAvailCarryNode() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	return dataNode.Carry >= 1
}

func (dataNode *DataNode) GetID() uint64 {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.ID
}

func (dataNode *DataNode) GetAddr() string {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return dataNode.Addr
}

// SetCarry implements "SetCarry" in the Node interface
func (dataNode *DataNode) SetCarry(carry float64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Carry = carry
}

// SelectNodeForWrite implements "SelectNodeForWrite" in the Node interface
func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.UsageRatio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.SelectedTimes++
	dataNode.Carry = dataNode.Carry - 1.0
}

func (dataNode *DataNode) clean() {
	dataNode.TaskManager.exitCh <- struct{}{}
}

func (dataNode *DataNode) createHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, dataNode.Addr, request)
	return
}
