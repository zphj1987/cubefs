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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"strconv"
	"time"
)

type ECNode struct {
	DataNode
	EcPartitionReports  []*proto.EcPartitionReport
}

type EcNodeValue struct {
	ID   uint64
	Addr string
	ZoneName  string
}

func newEcNodeValue(node *ECNode) *EcNodeValue {
	return &EcNodeValue{
		ID:   node.ID,
		Addr: node.Addr,
		ZoneName: node.ZoneName,
	}
}

func newEcNode(addr, clusterID, zoneName string) *ECNode {
	node := new(ECNode)
	node.Addr = addr
	node.ZoneName = zoneName
	node.TaskManager = newAdminTaskManager(addr, clusterID)
	return node
}

func (ecNode *ECNode) updateMetric(resp *proto.EcNodeHeartbeatResponse) {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.ReportTime = time.Now()
	ecNode.isActive = true
	ecNode.Total = resp.Total
	ecNode.Used = resp.Used
	ecNode.AvailableSpace = resp.Available
	ecNode.DataPartitionCount = resp.CreatedPartitionCnt
	ecNode.EcPartitionReports = resp.PartitionReports
	ecNode.ZoneName = resp.CellName
	//ecNode.BadDisks = resp.BadDisks
	if ecNode.Total == 0 {
		ecNode.UsageRatio = 0.0
	} else {
		ecNode.UsageRatio = (float64)(ecNode.Used) / (float64)(ecNode.Total)
	}
	ecNode.ReportTime = time.Now()
	ecNode.isActive = true
}

func (ecNode *ECNode) clean() {
	ecNode.TaskManager.exitCh <- struct{}{}
}

func (ecNode *ECNode) isWriteAble() (ok bool) {
	ecNode.RLock()
	defer ecNode.RUnlock()

	if ecNode.isActive == true && ecNode.AvailableSpace > 10*util.GB {
		ok = true
	}

	return
}

func (ecNode *ECNode) isAvailCarryNode() (ok bool) {
	ecNode.RLock()
	defer ecNode.RUnlock()

	return ecNode.Carry >= 1
}

// SetCarry implements "SetCarry" in the Node interface
func (ecNode *ECNode) SetCarry(carry float64) {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.Carry = carry
}

// SelectNodeForWrite implements "SelectNodeForWrite" in the Node interface
func (ecNode *ECNode) SelectNodeForWrite() {
	ecNode.Lock()
	defer ecNode.Unlock()
	ecNode.UsageRatio = float64(ecNode.Used) / float64(ecNode.Total)
	ecNode.SelectedTimes++
	ecNode.Carry = ecNode.Carry - 1.0
}

func (ecNode *ECNode) GetID() uint64 {
	ecNode.RLock()
	defer ecNode.RUnlock()
	return ecNode.ID
}

func (ecNode *ECNode) GetAddr() string {
	ecNode.RLock()
	defer ecNode.RUnlock()
	return ecNode.Addr
}

func (c *Cluster) checkEcNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.ecNodes.Range(func(addr, ecNode interface{}) bool {
		node := ecNode.(*ECNode)
		task := createHeartbeatTask(c.masterAddr(), node.Addr, proto.OpEcNodeHeartbeat)
		tasks = append(tasks, task)
		return true
	})
	c.addEcNodeTasks(tasks)
}

func (m *Server) addEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr, zoneName string
		id       uint64
		err      error
	)
	if nodeAddr, zoneName, err = parseRequestForAddNode(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if id, err = m.cluster.addEcNode(nodeAddr, zoneName); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr   string
		node       *ECNode
		ecNodeInfo *proto.EcNodeInfo
		err        error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.ecNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrCodecNodeNotExists))
		return
	}
	ecNodeInfo = &proto.EcNodeInfo{
		ID:         node.ID,
		Addr:       node.Addr,
		ReportTime: node.ReportTime,
		IsActive:   node.isActive,
	}

	sendOkReply(w, r, newSuccessHTTPReply(ecNodeInfo))
}

// Decommission a ec node
func (m *Server) decommissionEcNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *ECNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.ecNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.decommissionEcNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission ec node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) handleEcNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleEcNodeTaskResponse(tr.OperatorAddr, tr)
}

func (c *Cluster) handleEcNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[handleEcNodeTaskResponse] receive Task response:%v from %v", task.ID, nodeAddr))
	var (
		ecNode *ECNode
	)

	if ecNode, err = c.ecNode(nodeAddr); err != nil {
		goto errHandler
	}
	ecNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpEcNodeHeartbeat:
		response := task.Response.(*proto.EcNodeHeartbeatResponse)
		err = c.dealEcNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err := fmt.Errorf("unknown operate code %v", task.OpCode)
		log.LogError(err)
	}

	if err != nil {
		log.LogError(fmt.Sprintf("process task[%v] failed", task.ToString()))
	} else {
		log.LogInfof("process task:%v status:%v success", task.ID, task.Status)
	}
	return
errHandler:
	log.LogError(fmt.Sprintf("action[handleEcNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealEcNodeHeartbeatResp(nodeAddr string, resp *proto.EcNodeHeartbeatResponse) (err error) {
	var (
		ecNode *ECNode
		logMsg string
	)
	log.LogInfof("action[dealEcNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealEcNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if ecNode, err = c.ecNode(nodeAddr); err != nil {
		goto errHandler
	}
	if resp.CellName == "" {
		resp.CellName = DefaultZoneName
	}
	if ecNode.ZoneName != resp.CellName {
		c.t.deleteEcNode(ecNode)
		oldZoneName := ecNode.ZoneName
		ecNode.ZoneName = resp.CellName
		c.adjustEcNode(ecNode)
		log.LogWarnf("ecNode zone changed from [%v] to [%v]", oldZoneName, resp.CellName)
	}

	ecNode.updateMetric(resp)
	if err = c.t.putEcNode(ecNode); err != nil {
		log.LogErrorf("action[dealEcNodeHeartbeatResp] ecNode[%v],zone[%v], err[%v]", ecNode.Addr, ecNode.ZoneName, err)
	}
	c.updateEcNode(ecNode, resp.PartitionReports)
	logMsg = fmt.Sprintf("action[dealEcNodeHeartbeatResp],ecNode:%v ReportTime:%v  success", ecNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) adjustEcNode(ecNode *ECNode) {
	c.enMutex.Lock()
	defer c.enMutex.Unlock()
	var err error
	defer func() {
		if err != nil {
			err = fmt.Errorf("action[adjustEcNode],clusterID[%v] ecNodeAddr:%v,zone[%v] err:%v ", c.Name, ecNode.Addr, ecNode.ZoneName, err.Error())
			log.LogError(errors.Stack(err))
			Warn(c.Name, err.Error())
		}
	}()
	var zone *Zone
	zone, err = c.t.getZone(ecNode.ZoneName)
	if err != nil {
		zone = newZone(ecNode.ZoneName)
		c.t.putZone(zone)
	}
	if err = c.syncUpdateEcNode(ecNode); err != nil {
		return
	}
	err = c.t.putEcNode(ecNode)
	return
}

func (c *Cluster) updateEcNode(ecNode *ECNode, partitions []*proto.EcPartitionReport) {
	for _, vr := range partitions {
		if vr == nil {
			continue
		}
		vol, err := c.getVol(vr.VolName)
		if err != nil {
			continue
		}
		if vol.Status == markDelete {
			continue
		}
		if ecdp, err := vol.ecDataPartitions.get(vr.PartitionID); err == nil {
			ecdp.updateMetric(vr, ecNode, c)
		}
	}
	return
}

func (c *Cluster) addEcNode(nodeAddr, zoneName string) (id uint64, err error) {
	c.enMutex.Lock()
	defer c.enMutex.Unlock()
	var ecNode *ECNode
	if node, ok := c.ecNodes.Load(nodeAddr); ok {
		ecNode = node.(*ECNode)
		return ecNode.ID, nil
	}

	ecNode = newEcNode(nodeAddr, c.Name, zoneName)
	_, err = c.t.getZone(zoneName)
	if err != nil {
		c.t.putZoneIfAbsent(newZone(zoneName))
	}
	// allocate ecNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	ecNode.ID = id
	ecNode.ZoneName = zoneName
	if err = c.syncAddEcNode(ecNode); err != nil {
		goto errHandler
	}
	c.t.putEcNode(ecNode)
	c.ecNodes.Store(nodeAddr, ecNode)
	log.LogInfof("action[addEcNode],clusterID[%v] ecNodeAddr:%v success",
		c.Name, nodeAddr)
	return
errHandler:
	err = fmt.Errorf("action[addEcNode],clusterID[%v] ecNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

// key=#dn#id#Addr,value = json.Marshal(dnv)
func (c *Cluster) syncAddEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncPut, ecNode)
}

func (c *Cluster) syncDeleteEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncDelete, ecNode)
}

func (c *Cluster) syncUpdateEcNode(ecNode *ECNode) (err error) {
	return c.syncPutEcNodeInfo(opSyncUpdate, ecNode)
}

func (c *Cluster) syncPutEcNodeInfo(opType uint32, ecNode *ECNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ecNodePrefix + strconv.FormatUint(ecNode.ID, 10) + keySeparator + ecNode.Addr
	dnv := newEcNodeValue(ecNode)
	metadata.V, err = json.Marshal(dnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) ecNode(addr string) (ecNode *ECNode, err error) {
	value, ok := c.ecNodes.Load(addr)
	if !ok {
		err = errors.Trace(ecNodeNotFound(addr), "%v not found", addr)
		return
	}
	ecNode = value.(*ECNode)
	return
}

func (c *Cluster) delEcNodeFromCache(ecNode *ECNode) {
	c.ecNodes.Delete(ecNode.Addr)
	c.t.deleteEcNode(ecNode)
	go ecNode.clean()
}

func (c *Cluster) decommissionEcNode(ecNode *ECNode) (err error) {
	msg := fmt.Sprintf("action[decommissionEcNode], Node[%v] OffLine", ecNode.Addr)
	log.LogWarn(msg)
	safeVols := c.allVols()
	for _, vol := range safeVols {
		for _, ecdp := range vol.ecDataPartitions.partitions {
			if err = c.decommissionEcDataPartition(ecNode.Addr, ecdp, ""); err != nil {
				return
			}
		}
	}
	if err = c.syncDeleteEcNode(ecNode); err != nil {
		msg = fmt.Sprintf("action[decommissionEcNode],clusterID[%v] node[%v] offline failed,err[%v]",
			c.Name, ecNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delEcNodeFromCache(ecNode)
	go ecNode.clean()
	msg = fmt.Sprintf("action[decommissionEcNode],clusterID[%v] node[%v] offLine success",
		c.Name, ecNode.Addr)
	Warn(c.Name, msg)
	return
}


// Decommission a ec partition.
// 1. Check if we can decommission a ec partition. In the following cases, we are not allowed to do so:
// - (a) a replica is not in the latest host list;
// - (b) there is already a replica been taken offline;
// - (c) the remaining number of replicas < M (M + K = replicaNum)
// 2. Choose a new ec node.
// 3. synchronized decommission ec partition
// 4. synchronized create a new ec partition
// 5. Set the ec partition as readOnly.
// 6. persistent the new host list
func (c *Cluster) decommissionEcDataPartition(offlineAddr string, ecdp *EcDataPartition, errMsg string) (err error) {
	var (
		targetHosts     []string
		newAddr         string
		msg             string
		ecNode          *ECNode
		zone            *Zone
		newHosts        []string
		replica         *EcReplica
		zones           []string
		excludeZone     string
	)
	ecdp.RLock()
	if ok  := ecdp.hasHost(offlineAddr); !ok {
		ecdp.RUnlock()
		return
	}
	replica, _ = ecdp.getReplica(offlineAddr)
	ecdp.RUnlock()
	if err = c.validateDecommissionEcDataPartition(ecdp, offlineAddr); err != nil {
		goto errHandler
	}

	if ecNode, err = c.ecNode(offlineAddr); err != nil {
		goto errHandler
	}

	if ecNode.ZoneName == "" {
		err = fmt.Errorf("ecNode[%v] zone is nil", ecNode.Addr)
		goto errHandler
	}
	if zone, err = c.t.getZone(ecNode.ZoneName); err != nil {
		goto errHandler
	}

	if targetHosts, _, err = zone.getAvailEcNodeHosts(ecdp.Hosts, 1); err != nil {
		// select ec nodes from the other zone
		zones = ecdp.getLiveZones(offlineAddr)
		if len(zones) == 0 {
			excludeZone = zone.name
		} else {
			excludeZone = zones[0]
		}
		if targetHosts, err = c.chooseTargetEcNodes(excludeZone, ecdp.Hosts, 1); err != nil {
			goto errHandler
		}
	}
	log.LogInfof("action[decommissionEcDataPartition] target Hosts:%v", targetHosts)
	if err = c.removeEcDataReplica(ecdp, offlineAddr, false); err != nil {
		goto errHandler
	}
	newAddr = targetHosts[0]
	newHosts = ecdp.Hosts
	for index, host := range newHosts {
		if host == offlineAddr {
			newHosts[index] = newAddr
		}
	}
	if err := c.addEcReplica(ecdp, newAddr, newHosts); err != nil {
		goto errHandler
	}
	ecdp.Lock()
	ecdp.Hosts = newHosts
	if err = ecdp.update("decommissionEcDataPartition", ecdp.VolName, ecdp.Hosts, c); err != nil {
		ecdp.Unlock()
		return
	}
	ecdp.Unlock()
	if _, err := c.syncChangeEcPartitionMembers(ecdp); err != nil {
		goto errHandler
	}
	ecdp.Status = proto.ReadOnly
	ecdp.isRecover = true
	c.putBadEcPartitionIDs(replica, offlineAddr, ecdp.PartitionID)
	log.LogWarnf("clusterID[%v] partitionID:%v  on Node:%v offline success,newHost[%v],PersistenceHosts:[%v]",
		c.Name, ecdp.PartitionID, offlineAddr, newAddr, ecdp.Hosts)
	return
errHandler:
	msg = fmt.Sprintf(errMsg+" clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, ecdp.PartitionID, offlineAddr, newAddr, err, ecdp.Hosts)
	if err != nil {
		Warn(c.Name, msg)
		err = fmt.Errorf("vol[%v],partition[%v],err[%v]", ecdp.VolName, ecdp.PartitionID, err)
	}
	return
}

func (c *Cluster) validateDecommissionEcDataPartition(ecdp *EcDataPartition, offlineAddr string) (err error) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	var vol *Vol
	if vol, err = c.getVol(ecdp.VolName); err != nil {
		return
	}

	if err = ecdp.hasMissingOneReplica(int(vol.dpReplicaNum)); err != nil {
		return
	}

	// if the partition can be offline or not
	if err = ecdp.canBeOffLine(offlineAddr); err != nil {
		return
	}

	if ecdp.isRecover {
		err = fmt.Errorf("vol[%v],ec partition[%v] is recovering,[%v] can't be decommissioned", vol.Name, ecdp.PartitionID, offlineAddr)
		return
	}
	return
}

func (c *Cluster) loadEcNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(ecNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadEcNodes],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		ecnv := &EcNodeValue{}
		if err = json.Unmarshal(value, ecnv); err != nil {
			err = fmt.Errorf("action[loadEcNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		if ecnv.ZoneName == "" {
			ecnv.ZoneName = DefaultZoneName
		}
		ecNode := newEcNode(ecnv.Addr, c.Name, ecnv.ZoneName)
		zone, err := c.t.getZone(ecnv.ZoneName)
		if err != nil {
			log.LogErrorf("action[loadEcNodes], getZone err:%v", err)
			zone = newZone(ecnv.ZoneName)
			c.t.putZoneIfAbsent(zone)
		}
		ecNode.ID = ecnv.ID
		c.ecNodes.Store(ecNode.Addr, ecNode)
		log.LogInfof("action[loadEcNodes],EcNode[%v],ZoneName[%v]", ecNode.Addr, ecNode.ZoneName)
	}
	return
}

func (c *Cluster) allEcNodes() (ecNodes []proto.NodeView) {
	ecNodes = make([]proto.NodeView, 0)
	c.ecNodes.Range(func(key, value interface{}) bool {
		ecNode, ok := value.(*ECNode)
		if !ok {
			return true
		}
		ecNodes = append(ecNodes, proto.NodeView{Addr: ecNode.Addr, Status: ecNode.isActive, ID: ecNode.ID, IsWritable: ecNode.isWriteAble()})
		return true
	})
	return
}

// remove ec replica on ec node
func (c *Cluster) removeEcDataReplica(ecdp *EcDataPartition, addr string, validate bool) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[removeEcDataReplica],vol[%v],ec partition[%v],err[%v]", ecdp.VolName, ecdp.PartitionID, err)
		}
	}()
	if validate == true {
		if err = c.validateDecommissionEcDataPartition(ecdp, addr); err != nil {
			return
		}
	}
	ok := c.isEcRecovering(ecdp, addr)
	if ok {
		err = fmt.Errorf("vol[%v],ec partition[%v] can't decommision until it has recovered", ecdp.VolName, ecdp.PartitionID)
		return
	}
	ecNode, err := c.ecNode(addr)
	if err != nil {
		return
	}
	//todo delete peer?
	if err = c.deleteEcReplicaFromEcNodeOptimistic(ecdp, ecNode); err != nil {
		return
	}
	return
}

func (c *Cluster) addEcReplica(ecdp *EcDataPartition, addr string, hosts []string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[addEcReplica],vol[%v], ec partition[%v],err[%v]", ecdp.VolName, ecdp.PartitionID, err)
		}
	}()
	ecNode, err := c.ecNode(addr)
	if err != nil {
		return
	}
	if err = c.doCreateEcReplica(ecdp, ecNode, hosts); err != nil {
		return
	}
	return
}

func (c *Cluster) isEcRecovering(ecdp *EcDataPartition, addr string) (isRecover bool) {
	var key string
	ecdp.RLock()
	defer ecdp.RUnlock()
	replica, _ := ecdp.getReplica(addr)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	var badPartitionIDs []uint64
	badPartitions, ok := c.BadEcPartitionIds.Load(key)
	if ok {
		badPartitionIDs = badPartitions.([]uint64)
	}
	for _, id := range badPartitionIDs {
		if id == ecdp.PartitionID {
			isRecover = true
		}
	}
	return
}

func (c *Cluster) deleteEcReplicaFromEcNodeOptimistic(ecdp *EcDataPartition, ecNode *ECNode) (err error) {
	ecdp.Lock()
	// in case ecNode is unreachable, remove replica first.
	ecdp.removeReplicaByAddr(ecNode.Addr)
	ecdp.checkAndRemoveMissReplica(ecNode.Addr)
	ecdp.Unlock()
	task := ecdp.createTaskToDeleteEcPartition(ecNode.Addr)
	_, err = ecNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteEcReplicaFromEcNodeOptimistic] vol[%v], ec partition[%v], err[%v]", ecdp.VolName, ecdp.PartitionID, err)
	}
	return nil
}

func (c *Cluster) doCreateEcReplica(ecdp *EcDataPartition, addNode *ECNode, hosts []string) (err error) {
	vol, err := c.getVol(ecdp.VolName)
	if err != nil {
		return
	}
	diskPath, err := c.syncCreateEcDataPartitionToEcNode(addNode.Addr, vol.dataPartitionSize, ecdp, hosts)
	if err != nil {
		return
	}
	ecdp.Lock()
	defer ecdp.Unlock()
	if err = ecdp.afterCreation(addNode.Addr, diskPath, c); err != nil {
		return
	}
	return
}

func (c *Cluster) putBadEcPartitionIDs(replica *EcReplica, addr string, partitionID uint64) {
	var key string
	newBadPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", addr, "")
	}
	badPartitionIDs, ok := c.BadEcPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadEcPartitionIds.Store(key, newBadPartitionIDs)
}
