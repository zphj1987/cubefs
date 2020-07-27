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

package codecnode

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

var (
	MinWriteAbleEcPartitionCnt = 10
)

type EcPartitionView struct {
	EcPartitions []*EcPartition
}

type EcWrapper struct {
	sync.RWMutex
	clusterName           string
	volName               string
	masters               []string
	partitions            map[uint64]*EcPartition
	rwPartition           []*EcPartition
	mc                    *masterSDK.MasterClient
	stopOnce              sync.Once
	stopC                 chan struct{}

	HostsStatus map[string]bool
}

// NewEcPartitionWrapper returns a new ec partition wrapper.
func NewEcPartitionWrapper(volName string, masters []string) (w *EcWrapper, err error) {
	w = new(EcWrapper)
	w.stopC = make(chan struct{})
	w.masters = masters
	w.mc = masterSDK.NewMasterClient(masters, false)
	w.volName = volName
	w.rwPartition = make([]*EcPartition, 0)
	w.partitions = make(map[uint64]*EcPartition)
	w.HostsStatus = make(map[string]bool)
	if err = w.updateClusterInfo(); err != nil {
		err = errors.Trace(err, "NewEcPartitionWrapper:")
		return
	}
	if err = w.updateEcPartition(true); err != nil {
		err = errors.Trace(err, "NewEcPartitionWrapper:")
		return
	}
	go w.update()
	return
}

func (w *EcWrapper) Stop() {
	w.stopOnce.Do(func() {
		close(w.stopC)
	})
}

func (w *EcWrapper) updateClusterInfo() (err error) {
	var info *proto.ClusterInfo
	if info, err = w.mc.AdminAPI().GetClusterInfo(); err != nil {
		log.LogWarnf("UpdateClusterInfo: get cluster info fail: err(%v)", err)
		return
	}
	log.LogInfof("UpdateClusterInfo: get cluster info: cluster(%v) localIP(%v)", info.Cluster, info.Ip)
	w.clusterName = info.Cluster
	return
}

func (w *EcWrapper) update() {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			w.updateEcPartition(false)
		case <-w.stopC:
			return
		}
	}
}

func (w *EcWrapper) updateEcPartition(isInit bool) (err error) {

	var evp *proto.EcPartitionsView
	if evp, err = w.mc.ClientAPI().GetEcPartitions(w.volName); err != nil {
		log.LogErrorf("updateEcPartition: get ec partitions fail: volume(%v) err(%v)", w.volName, err)
		return
	}
	log.LogInfof("updateEcPartition: get ec partitions: volume(%v) partitions(%v)", w.volName, len(evp.EcPartitions))

	var convert = func(response *proto.EcPartitionResponse) *EcPartition {
		return &EcPartition{
			EcPartitionResponse: *response,
			ClientWrapper:         w,
		}
	}

	rwPartitionGroups := make([]*EcPartition, 0)
	for _, partition := range evp.EcPartitions {
		ep := convert(partition)
		log.LogInfof("updateEcPartition: dp(%v)", ep)
		w.replaceOrInsertPartition(ep)
		if ep.Status == proto.ReadWrite {
			rwPartitionGroups = append(rwPartitionGroups, ep)
		}
	}

	// isInit used to identify whether this call is caused by mount action
	if isInit || (len(rwPartitionGroups) >= MinWriteAbleEcPartitionCnt) {
		w.rwPartition = rwPartitionGroups
	} else {
		err = errors.New("updateEcPartition: no writable ec partition")
	}

	log.LogInfof("updateEcPartition: finish")
	return err
}

func (w *EcWrapper) replaceOrInsertPartition(ep *EcPartition) {
	var (
		oldstatus int8
	)
	w.Lock()
	old, ok := w.partitions[ep.PartitionID]
	if ok {
		oldstatus = old.Status
		old.Status = ep.Status
		old.ReplicaNum = ep.ReplicaNum
		old.Hosts = ep.Hosts
	} else {
		w.partitions[ep.PartitionID] = ep
	}

	w.Unlock()

	if ok && oldstatus != ep.Status {
		log.LogInfof("partition: status change (%v) -> (%v)", old, ep)
	}
}

func (w *EcWrapper) getRandomEcPartitionForWrite(exclude map[string]struct{}) *EcPartition {
	var ep *EcPartition
	if len(w.rwPartition) == 0 {
		return nil
	}
	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(len(w.rwPartition))
	ep = w.rwPartition[index]
	if !isExcluded(ep, exclude) {
		return ep
	}
	for _, ep = range w.rwPartition {
		if !isExcluded(ep, exclude) {
			return ep
		}
	}
	return nil
}

// GetEcPartition returns the ec partition based on the given partition ID.
func (w *EcWrapper) GetEcPartition(partitionID uint64) (*EcPartition, error) {
	w.RLock()
	defer w.RUnlock()
	ep, ok := w.partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("partition[%v] not exsit", partitionID)
	}
	return ep, nil
}

// WarningMsg returns the warning message that contains the cluster name.
func (w *EcWrapper) WarningMsg() string {
	return fmt.Sprintf("%s_client_warning", w.clusterName)
}
