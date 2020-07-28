package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"strconv"
	"strings"
	"sync"
	"time"
	"encoding/json"
)

type EcDataPartition struct {
	*DataPartition
	DataUnitsNum   uint8
	ParityUnitsNum uint8
	LastParityTime string
	StripeUnitSize uint64
	ExtentFileSize uint64
	ecReplicas     []*EcReplica
}

type EcReplica struct {
	proto.EcReplica
	ecNode *ECNode
}

type ecDataPartitionValue struct {
	PartitionID    uint64
	ReplicaNum     uint8
	Hosts          string
	Status         int8
	VolID          uint64
	VolName        string
	DataUnitsNum   uint8
	ParityUnitsNum uint8
	StripeUnitSize uint64
	ExtentFileSize uint64
	Replicas       []*replicaValue
}

func newEcDataPartitionValue(ep *EcDataPartition) (edpv *ecDataPartitionValue) {
	edpv = &ecDataPartitionValue{
		PartitionID:    ep.PartitionID,
		ReplicaNum:     ep.ReplicaNum,
		Hosts:          ep.ecHostsToString(),
		Status:         ep.Status,
		VolID:          ep.VolID,
		VolName:        ep.VolName,
		StripeUnitSize: ep.StripeUnitSize,
		ExtentFileSize: ep.ExtentFileSize,
		DataUnitsNum:   ep.DataUnitsNum,
		ParityUnitsNum: ep.ParityUnitsNum,
		Replicas:       make([]*replicaValue, 0),
	}
	for _, replica := range ep.ecReplicas {
		rv := &replicaValue{Addr: replica.Addr, DiskPath: replica.DiskPath}
		edpv.Replicas = append(edpv.Replicas, rv)
	}
	return
}

type EcDataPartitionCache struct {
	partitions             map[uint64]*EcDataPartition
	volName                string
	vol                    *Vol
	responseCache          []byte
	readableAndWritableCnt int    // number of readable and writable partitionMap
	sync.RWMutex
}

func newEcDataPartitionCache(vol *Vol) (ecdpCache *EcDataPartitionCache) {
	ecdpCache = new(EcDataPartitionCache)
	ecdpCache.partitions = make(map[uint64]*EcDataPartition)
	ecdpCache.volName = vol.Name
	ecdpCache.vol = vol
	return
}

func (ecdpCache *EcDataPartitionCache) put(ecdp *EcDataPartition) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	ecdpCache.partitions[ecdp.PartitionID] = ecdp
}

func (ecdpCache *EcDataPartitionCache) get(partitionID uint64) (ecdp *EcDataPartition, err error) {
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	ecdp, ok := ecdpCache.partitions[partitionID]
	if !ok {
		return nil, proto.ErrEcPartitionNotExists
	}
	return
}

func (ecdpCache *EcDataPartitionCache) getEcPartitionsView(minPartitionID uint64) (epResps []*proto.EcPartitionResponse) {
	epResps = make([]*proto.EcPartitionResponse, 0)
	log.LogDebugf("volName[%v] EcPartitionMapLen[%v], minPartitionID[%v]",
		ecdpCache.volName, len(ecdpCache.partitions), minPartitionID)
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	for _, ep := range ecdpCache.partitions {
		if ep.PartitionID <= minPartitionID {
			continue
		}
		epResp := ep.convertToEcPartitionResponse()
		epResps = append(epResps, epResp)
	}
	return
}

func (ecdpCache *EcDataPartitionCache) updateResponseCache(needsUpdate bool, minPartitionID uint64) (body []byte, err error) {
	responseCache := ecdpCache.getEcPartitionResponseCache()
	if responseCache == nil || needsUpdate || len(responseCache) == 0 {
		epResps := ecdpCache.getEcPartitionsView(minPartitionID)
		if len(epResps) == 0 {
			log.LogError(fmt.Sprintf("action[updateEpResponseCache],volName[%v] minPartitionID:%v,err:%v",
				ecdpCache.volName, minPartitionID, proto.ErrNoAvailEcPartition))
			return nil, proto.ErrNoAvailEcPartition
		}
		cv := proto.NewEcPartitionsView()
		cv.EcPartitions = epResps
		reply := newSuccessHTTPReply(cv)
		if body, err = json.Marshal(reply); err != nil {
			log.LogError(fmt.Sprintf("action[updateEpResponseCache],minPartitionID:%v,err:%v",
				minPartitionID, err.Error()))
			return nil, proto.ErrMarshalData
		}
		ecdpCache.setEcPartitionResponseCache(body)
		return
	}
	body = make([]byte, len(responseCache))
	copy(body, responseCache)

	return
}

func (ecdpCache *EcDataPartitionCache) getEcPartitionResponseCache() []byte {
	ecdpCache.RLock()
	defer ecdpCache.RUnlock()
	return ecdpCache.responseCache
}

func (ecdpCache *EcDataPartitionCache) setEcPartitionResponseCache(responseCache []byte) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	if responseCache != nil {
		ecdpCache.responseCache = responseCache
	}
}

func (ecdpCache *EcDataPartitionCache) setReadWriteDataPartitions(readWrites int, clusterName string) {
	ecdpCache.Lock()
	defer ecdpCache.Unlock()
	ecdpCache.readableAndWritableCnt = readWrites
}

func newEcReplica(ecNode *ECNode) (replica *EcReplica) {
	replica = new(EcReplica)
	replica.Addr = ecNode.Addr
	replica.ecNode = ecNode
	replica.ReportTime = time.Now().Unix()
	return
}
func (replica *EcReplica) setAlive() {
	replica.ReportTime = time.Now().Unix()
}

func (replica *EcReplica) isMissing(interval int64) (isMissing bool) {
	if time.Now().Unix()-replica.ReportTime > interval {
		isMissing = true
	}
	return
}

func (replica *EcReplica) isLive(timeOutSec int64) (isAvailable bool) {
	if replica.ecNode.isActive == true && replica.Status != proto.Unavailable &&
		replica.isActive(timeOutSec) == true {
		isAvailable = true
	}

	return
}

func (replica *EcReplica) isActive(timeOutSec int64) bool {
	return time.Now().Unix()-replica.ReportTime <= timeOutSec
}

func (replica *EcReplica) getReplicaNode() (node Node) {
	return replica.ecNode
}

// check if the replica's location is available
func (replica *EcReplica) isLocationAvailable() (isAvailable bool) {
	ecNode := replica.getReplicaNode()
	if ecNode.IsOnline() == true && replica.isActive(defaultDataPartitionTimeOutSec) == true {
		isAvailable = true
	}
	return
}

func newEcDataPartition(ID uint64, volName string, volID uint64, ecDataBlockNum, ecParityBlockNum uint8, stripeUnitSize, extentFileSize uint64) (ecdp *EcDataPartition) {
	partition := newDataPartition(ID, ecDataBlockNum + ecParityBlockNum, volName, volID)
	ecdp = &EcDataPartition{DataPartition: partition}
	ecdp.ecReplicas = make([]*EcReplica, 0)
	ecdp.DataUnitsNum = ecDataBlockNum
	ecdp.ParityUnitsNum = ecParityBlockNum
	ecdp.StripeUnitSize = stripeUnitSize
	ecdp.ExtentFileSize = extentFileSize
	return
}

func (ecdp *EcDataPartition) getLeaderAddr() (leaderAddr string) {
	for _, erp := range ecdp.ecReplicas {
		if erp.IsLeader {
			return erp.Addr
		}
	}
	return
}

func (ecdp *EcDataPartition) getLeaderAddrWithLock() (leaderAddr string) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	for _, replica := range ecdp.ecReplicas {
		if replica.IsLeader {
			return replica.Addr
		}
	}
	return
}

func (ecdp *EcDataPartition) ecHostsToString() string {
	return strings.Join(ecdp.Hosts, underlineSeparator)
}

func (ecdp *EcDataPartition) getReplica(addr string) (replica *EcReplica, err error) {
	for index := 0; index < len(ecdp.ecReplicas); index++ {
		replica = ecdp.ecReplicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogErrorf("action[getReplica],partitionID:%v,locations:%v,err:%v",
		ecdp.PartitionID, addr, dataReplicaNotFound(addr))
	return nil, errors.Trace(dataReplicaNotFound(addr), "%v not found", addr)
}

func (ecdp *EcDataPartition) update(action, volName string, newHosts []string, c *Cluster) (err error) {
	orgHosts := make([]string, len(ecdp.Hosts))
	copy(orgHosts, ecdp.Hosts)
	ecdp.Hosts = newHosts
	if err = c.syncUpdateEcDataPartition(ecdp); err != nil {
		ecdp.Hosts = orgHosts
		return errors.Trace(err, "action[%v] update partition[%v] vol[%v] failed", action, ecdp.PartitionID, volName)
	}
	msg := fmt.Sprintf("action[%v] success,vol[%v] partitionID:%v "+
		"oldHosts:%v, newHosts:%v, newPeers[%v]",
		action, volName, ecdp.PartitionID, orgHosts, ecdp.Hosts, ecdp.Peers)
	log.LogInfo(msg)
	return
}

func (ecdp *EcDataPartition) updateMetric(vr *proto.EcPartitionReport, ecNode *ECNode, c *Cluster) {
	if !ecdp.hasHost(ecNode.Addr) {
		return
	}
	ecdp.Lock()
	defer ecdp.Unlock()
	replica, err := ecdp.getReplica(ecNode.Addr)
	if err != nil {
		replica = newEcReplica(ecNode)
		ecdp.addEcReplica(replica)
	}
	ecdp.total = vr.Total
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	replica.NodeIndex = vr.NodeIndex
	ecdp.setMaxUsed()
	replica.FileCount = uint32(vr.ExtentCount)
	replica.ReportTime = time.Now().Unix()
	replica.IsLeader = vr.IsLeader
	if vr.IsLeader {
		ecdp.isRecover = vr.IsRecover
	}
	replica.NeedsToCompare = vr.NeedCompare
	if replica.DiskPath != vr.DiskPath && vr.DiskPath != "" {
		oldDiskPath := replica.DiskPath
		replica.DiskPath = vr.DiskPath
		err = c.syncUpdateEcDataPartition(ecdp)
		if err != nil {
			replica.DiskPath = oldDiskPath
		}
	}
	ecdp.checkAndRemoveMissReplica(ecNode.Addr)
}

func (ecdp *EcDataPartition) addEcReplica(replica *EcReplica) {
	for _, r := range ecdp.ecReplicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	ecdp.ecReplicas = append(ecdp.ecReplicas, replica)
}

func (ecdp *EcDataPartition) afterCreation(nodeAddr, diskPath string, c *Cluster) (err error) {
	ecNode, err := c.ecNode(nodeAddr)
	if err != nil || ecNode == nil{
		return err
	}
	replica := newEcReplica(ecNode)
	replica.Status = proto.ReadWrite
	replica.DiskPath = diskPath
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	ecdp.addEcReplica(replica)
	ecdp.checkAndRemoveMissReplica(replica.Addr)
	return
}

func (ecdp *EcDataPartition) createTaskToCreateEcDataPartition(addr string, ecPartitionSize uint64, hosts []string) (task *proto.AdminTask) {
	var nodeIndex uint32
	if index := getIndex(addr, hosts); index > -1 {
		nodeIndex = uint32(index)
	}
	task = proto.NewAdminTask(proto.OpCreateEcDataPartition, addr, newCreateEcPartitionRequest(
		ecdp.VolName, ecdp.PartitionID, 16, 12, ecPartitionSize, uint32(ecdp.DataUnitsNum), uint32(ecdp.ParityUnitsNum), nodeIndex, hosts))
	ecdp.resetTaskID(task)
	return
}

func getIndex(addr string, hosts []string) (index int) {
	index = -1
	if len(hosts) == 0 {
		return
	}
	if "" == addr {
		return
	}
	for i, host := range hosts {
		if addr == host {
			index = i
			return
		}
	}
	return
}
func (ecdp *EcDataPartition) createTaskToDeleteEcPartition(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteEcDataPartition, addr, newDeleteEcPartitionRequest(ecdp.PartitionID))
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) createTaskToChangeEcPartitionMembers(newHosts []string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpChangeEcPartitionMembers, newHosts[0], newChangeEcPartitionMembersRequest(ecdp.PartitionID, newHosts))
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) createTaskToParityEcDataPartition(addr string) (task *proto.AdminTask) {

	task = proto.NewAdminTask(proto.OpParityEcDataPartition, addr, &proto.ParityEcDataPartitionRequest{
		PartitionId: ecdp.PartitionID,
		Hosts:       ecdp.Hosts,
	})
	ecdp.resetTaskID(task)
	return
}

func (ecdp *EcDataPartition) convertToEcPartitionResponse() (ecdpr *proto.EcPartitionResponse) {
	ecdpr = new(proto.EcPartitionResponse)
	ecdp.Lock()
	defer ecdp.Unlock()
	ecdpr.PartitionID = ecdp.PartitionID
	ecdpr.Status = ecdp.Status
	ecdpr.Hosts = make([]string, len(ecdp.Hosts))
	copy(ecdpr.Hosts, ecdp.Hosts)
	ecdpr.LeaderAddr = ecdp.getLeaderAddr()
	ecdpr.DataUnitsNum = ecdp.DataUnitsNum
	ecdpr.ParityUnitsNum = ecdp.ParityUnitsNum
	ecdpr.ReplicaNum = ecdp.ReplicaNum
	ecdpr.StripeUnitSize = uint32(ecdp.StripeUnitSize)
	ecdpr.ExtentFileSize = uint32(ecdp.ExtentFileSize)
	return
}

func (ecdp *EcDataPartition) ToProto(c *Cluster) *proto.EcPartitionInfo {
	ecdp.RLock()
	defer ecdp.RUnlock()
	var replicas = make([]*proto.EcReplica, len(ecdp.ecReplicas))
	for i, replica := range ecdp.ecReplicas {
		replicas[i] = &replica.EcReplica
	}
	zones := make([]string, len(ecdp.Hosts))
	for idx, host := range ecdp.Hosts {
		ecNode, err := c.ecNode(host)
		if err == nil {
			zones[idx] = ecNode.ZoneName
		}
	}
	partition := &proto.DataPartitionInfo{
		PartitionID:             ecdp.PartitionID,
		LastLoadedTime:          ecdp.LastLoadedTime,
		Status:                  ecdp.Status,
		Hosts:                   ecdp.Hosts,
		ReplicaNum:              ecdp.ParityUnitsNum + ecdp.DataUnitsNum,
		Peers:                   ecdp.Peers,
		Zones:                   zones,
		MissingNodes:            ecdp.MissingNodes,
		VolName:                 ecdp.VolName,
		VolID:                   ecdp.VolID,
	}
	return &proto.EcPartitionInfo{
		DataPartitionInfo: partition,
		EcReplicas:        replicas,
		ParityUnitsNum:    ecdp.ParityUnitsNum,
		DataUnitsNum:      ecdp.DataUnitsNum,
	}
}
func (ecdp *EcDataPartition) checkStatus(clusterName string, needLog bool, dpTimeOutSec int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	liveReplicas := ecdp.getLiveReplicasFromHosts(dpTimeOutSec)
	if len(ecdp.ecReplicas) > len(ecdp.Hosts) {
		ecdp.Status = proto.ReadOnly
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v has exceed repica, replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			ecdp.PartitionID, ecdp.ReplicaNum, len(liveReplicas), ecdp.Status, ecdp.Hosts)
		Warn(clusterName, msg)
		return
	}

	switch len(liveReplicas) {
	case (int)(ecdp.ReplicaNum):
		ecdp.Status = proto.ReadOnly
		if ecdp.checkReplicaStatusOnLiveNode(liveReplicas) == true && ecdp.canWrite() {
			ecdp.Status = proto.ReadWrite
		}
	default:
		ecdp.Status = proto.ReadOnly
	}
	if needLog == true && len(liveReplicas) != int(ecdp.ReplicaNum) {
		msg := fmt.Sprintf("action[extractStatus],partitionID:%v  replicaNum:%v  liveReplicas:%v   Status:%v  RocksDBHost:%v ",
			ecdp.PartitionID, ecdp.ReplicaNum, len(liveReplicas), ecdp.Status, ecdp.Hosts)
		log.LogInfo(msg)
		if time.Now().Unix()-ecdp.lastWarnTime > intervalToWarnDataPartition {
			Warn(clusterName, msg)
			ecdp.lastWarnTime = time.Now().Unix()
		}
	}
}

// Check if there is any missing replica for a ec partition.
func (ecdp *EcDataPartition) checkMissingReplicas(clusterID, leaderAddr string, ecPartitionMissSec, ecPartitionWarnInterval int64) {
	ecdp.Lock()
	defer ecdp.Unlock()
	for _, replica := range ecdp.ecReplicas {
		if ecdp.hasHost(replica.Addr) && replica.isMissing(ecPartitionMissSec) == true && ecdp.needToAlarmMissingDataPartition(replica.Addr, ecPartitionWarnInterval) {
			ecNode := replica.getReplicaNode()
			var (
				lastReportTime time.Time
			)
			isActive := true
			if ecNode != nil {
				lastReportTime = ecNode.GetReportTime()
				isActive = ecNode.IsOnline()
			}
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] paritionID:%v  on Node:%v  "+
				"miss time > %v  lastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v So Migrate by manual",
				clusterID, ecdp.PartitionID, replica.Addr, ecPartitionMissSec, replica.ReportTime, lastReportTime, isActive)
			msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/dataPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, replica.Addr)
			Warn(clusterID, msg)
		}
	}

	for _, addr := range ecdp.Hosts {
		if ecdp.hasMissingEcPartition(addr) == true && ecdp.needToAlarmMissingDataPartition(addr, ecPartitionWarnInterval) {
			msg := fmt.Sprintf("action[checkMissErr],clusterID[%v] partitionID:%v  on Node:%v  "+
				"miss time  > :%v  but server not exsit So Migrate", clusterID, ecdp.PartitionID, addr, ecPartitionMissSec)
			msg = msg + fmt.Sprintf(" decommissionDataPartitionURL is http://%v/ecPartition/decommission?id=%v&addr=%v", leaderAddr, ecdp.PartitionID, addr)
			Warn(clusterID, msg)
		}
	}
}

// Remove the replica address from the memory.
func (ecdp *EcDataPartition) removeReplicaByAddr(addr string) {
	delIndex := -1
	var replica *EcReplica
	for i := 0; i < len(ecdp.ecReplicas); i++ {
		replica = ecdp.ecReplicas[i]
		if replica.Addr == addr {
			delIndex = i
			break
		}
	}
	msg := fmt.Sprintf("action[removeReplicaByAddr], ec partition:%v  on Node:%v  OffLine,the node is in replicas:%v", ecdp.PartitionID, addr, replica != nil)
	log.LogDebug(msg)
	if delIndex == -1 {
		return
	}
	ecdp.FileInCoreMap = make(map[string]*FileInCore, 0)
	ecdp.deleteReplicaByIndex(delIndex)
	ecdp.modifyTime = time.Now().Unix()

	return
}

func (ecdp *EcDataPartition) checkAndRemoveMissReplica(addr string) {
	if _, ok := ecdp.MissingNodes[addr]; ok {
		delete(ecdp.MissingNodes, addr)
	}
}

func (ecdp *EcDataPartition) deleteReplicaByIndex(index int) {
	var replicaAddrs []string
	for _, replica := range ecdp.ecReplicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("deleteReplicaByIndex replica:%v  index:%v  locations :%v ", ecdp.PartitionID, index, replicaAddrs)
	log.LogInfo(msg)
	replicasAfter := ecdp.ecReplicas[index+1:]
	ecdp.ecReplicas = ecdp.ecReplicas[:index]
	ecdp.ecReplicas = append(ecdp.ecReplicas, replicasAfter...)
}

func (ecdp *EcDataPartition)  hasMissingEcPartition(addr string) (isMissing bool) {
	_, ok := ecdp.hasReplica(addr)
	if ok == false {
		isMissing = true
	}
	return
}

func (ecdp *EcDataPartition) hasReplica(host string) (replica *EcReplica, ok bool) {
	// using loop instead of map to save the memory
	for _, replica = range ecdp.ecReplicas {
		if replica.Addr == host {
			ok = true
			break
		}
	}
	return
}

// get all the live replicas from the persistent hosts
func (ecdp *EcDataPartition) getLiveReplicasFromHosts(timeOutSec int64) (replicas []*EcReplica) {
	replicas = make([]*EcReplica, 0)
	for _, host := range ecdp.Hosts {
		replica, ok := ecdp.hasReplica(host)
		if !ok {
			continue
		}
		if replica.isLive(timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}
	return
}

// Check if there is a replica missing or not.
func (ecdp *EcDataPartition) hasMissingOneReplica(replicaNum int) (err error) {
	hostNum := len(ecdp.ecReplicas)
	if hostNum <= replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", ecdp.PartitionID, proto.ErrHasOneMissingReplica))
		err = proto.ErrHasOneMissingReplica
	}
	return
}

func (ecdp *EcDataPartition) canBeOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		ecdp.PartitionID, ecdp.Hosts, offlineAddr)
	liveReplicas := ecdp.liveReplicas(defaultDataPartitionTimeOutSec)

	otherLiveReplicas := make([]*EcReplica, 0)
	for i := 0; i < len(liveReplicas); i++ {
		replica := liveReplicas[i]
		if replica.Addr != offlineAddr {
			otherLiveReplicas = append(otherLiveReplicas, replica)
		}
	}

	if len(otherLiveReplicas) < int(ecdp.DataUnitsNum) {
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas:%v, ecDataUnitsNum:%v", proto.ErrCannotBeOffLine, len(liveReplicas), ecdp.DataUnitsNum)
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}

	return
}
func (ecdp *EcDataPartition) getLiveZones(offlineAddr string) (zones []string) {
	ecdp.RLock()
	defer ecdp.RUnlock()
	for _, replica := range ecdp.ecReplicas {
		if replica.ecNode == nil {
			continue
		}
		if replica.ecNode.Addr == offlineAddr {
			continue
		}
		zones = append(zones, replica.ecNode.ZoneName)
	}
	return
}
func (ecdp *EcDataPartition) liveReplicas(timeOutSec int64) (replicas []*EcReplica) {
	replicas = make([]*EcReplica, 0)
	for i := 0; i < len(ecdp.ecReplicas); i++ {
		replica := ecdp.ecReplicas[i]
		if replica.isLive(timeOutSec) == true && ecdp.hasHost(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}
	return
}
func (ecdp *EcDataPartition) canWrite() bool {
	avail := ecdp.total - ecdp.used
	if int64(avail) > 10*util.GB {
		return true
	}
	return false
}

func (c *Cluster) batchCreateEcDataPartition(vol *Vol, reqCount int) (err error) {
	for i := 0; i < reqCount; i++ {
		if c.DisableAutoAllocate {
			return
		}
		if _, err = c.createEcDataPartition(vol); err != nil {
			log.LogErrorf("action[batchCreateEcPartition] after create [%v] ec partition,occurred error,err[%v]", i, err)
			break
		}
	}
	return
}
func (c *Cluster) createEcDataPartition(vol *Vol) (ecdp *EcDataPartition, err error) {
	var (
		targetHosts []string
		wg          sync.WaitGroup
		partitionID uint64
	)
	replicaNum := vol.EcDataBlockNum + vol.EcParityBlockNum
	// ec partition and data partition using the same id allocator
	vol.createDpMutex.Lock()
	defer vol.createDpMutex.Unlock()
	errChannel := make(chan error, replicaNum)

	if targetHosts, err = c.chooseTargetEcNodes("", nil, int(replicaNum)); err != nil {
		goto errHandler
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errHandler
	}
	ecdp = newEcDataPartition(partitionID, vol.Name, vol.ID, vol.EcDataBlockNum, vol.EcParityBlockNum, vol.StripeUnitSize, vol.ExtentFileSize)
	ecdp.Hosts = targetHosts
	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			var diskPath string
			if diskPath, err = c.syncCreateEcDataPartitionToEcNode(host, vol.dataPartitionSize, ecdp, ecdp.Hosts); err != nil {
				errChannel <- err
				return
			}
			ecdp.Lock()
			defer ecdp.Unlock()
			if err = ecdp.afterCreation(host, diskPath, c); err != nil {
				errChannel <- err
			}
		}(host)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		for _, host := range targetHosts {
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				_, err := ecdp.getReplica(host)
				if err != nil {
					return
				}
				task := ecdp.createTaskToDeleteEcPartition(host)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.addEcNodeTasks(tasks)
			}(host)
		}
		wg.Wait()
		goto errHandler
	default:
		ecdp.total = util.DefaultDataPartitionSize
		ecdp.Status = proto.ReadWrite
	}
	if err = c.syncAddEcDataPartition(ecdp); err != nil {
		goto errHandler
	}
	vol.ecDataPartitions.put(ecdp)
	log.LogInfof("action[createEcDataPartition] success,volName[%v],partitionId[%v],Hosts[%v]", vol.Name, partitionID, ecdp.Hosts)
	return
errHandler:
	err = fmt.Errorf("action[createEcDataPartition],clusterID[%v] vol[%v] Err:%v ", c.Name, vol.Name, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) chooseTargetEcNodes(excludeZone string, excludeHosts []string, replicaNum int) (hosts []string, err error) {
	var (
		zones          []*Zone
		zoneNum  int
	)
	excludeZones := make([]string, 0)
	zones = make([]*Zone, 0)
	//don't cross zone. If cross zone, set zoneNum=defaultEcPartitionAcrossZoneNun
	zoneNum = 1
	if excludeZone != "" {
		excludeZones = append(excludeZones, excludeZone)
	}
	zones, err = c.t.allocZonesForEcNode(zoneNum, replicaNum, excludeZones)
	if err != nil {
		return
	}
	for _, zone := range zones {
		selectedHosts, _, e := zone.getAvailEcNodeHosts(excludeHosts, replicaNum)
		if e != nil {
			return nil, errors.NewError(e)
		}
		hosts = append(hosts, selectedHosts...)
	}
	log.LogInfof("action[chooseTargetEcNodes] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNum, len(zones), hosts)
	if len(hosts) != replicaNum {
		log.LogErrorf("action[chooseTargetDataNodes] replicaNum[%v],zoneNum[%v],selectedZones[%v],hosts[%v]", replicaNum, zoneNum, len(zones), hosts)
		return nil, errors.Trace(proto.ErrNoDataNodeToCreateDataPartition, "hosts len[%v],replicaNum[%v],zoneNum[%v],selectedZones[%v]",
			len(hosts), replicaNum, zoneNum, len(zones))
	}
	return
}

func (c *Cluster) syncCreateEcDataPartitionToEcNode(host string, size uint64, ecdp *EcDataPartition, hosts []string) (diskPath string, err error) {
	task := ecdp.createTaskToCreateEcDataPartition(host, size, hosts)
	ecNode, err := c.ecNode(host)
	if err != nil {
		return
	}
	var resp *proto.Packet
	if resp, err = ecNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return string(resp.Data), nil
}

func (c *Cluster) syncChangeEcPartitionMembers(ecdp *EcDataPartition) (resp *proto.Packet, err error) {
	hosts := ecdp.Hosts
	task := ecdp.createTaskToChangeEcPartitionMembers(hosts)
	ecNode, err := c.ecNode(hosts[0])
	if err != nil {
		return
	}
	if resp, err = ecNode.TaskManager.syncSendAdminTask(task); err != nil {
		return
	}
	return resp, nil
}

// key=#ecdp#volID#partitionID,value=json.Marshal(ecPartitionValue)
func (c *Cluster) syncAddEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncAddEcPartition, partition)
}

func (c *Cluster) syncUpdateEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncUpdateEcPartition, partition)
}

func (c *Cluster) syncDeleteEcDataPartition(partition *EcDataPartition) (err error) {
	return c.putEcDataPartitionInfo(opSyncDeleteEcPartition, partition)
}

func (c *Cluster) putEcDataPartitionInfo(opType uint32, partition *EcDataPartition) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = ecPartitionPrefix + strconv.FormatUint(partition.VolID, 10) + keySeparator + strconv.FormatUint(partition.PartitionID, 10)
	ecdpv := newEcDataPartitionValue(partition)
	metadata.V, err = json.Marshal(ecdpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) getEcPartitionByID(partitionID uint64) (ep *EcDataPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		if ep, err = vol.getEcPartitionByID(partitionID); err == nil {
			return
		}
	}
	err = ecPartitionNotFound(partitionID)
	return
}

func (c *Cluster) loadEcPartitions() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(ecPartitionPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadEcPartitions],err:%v", err.Error())
		return err
	}
	for _, value := range result {

		edpv := &ecDataPartitionValue{}
		if err = json.Unmarshal(value, edpv); err != nil {
			err = fmt.Errorf("action[loadDataPartitions],value:%v,unmarshal err:%v", string(value), err)
			return err
		}
		vol, err1 := c.getVol(edpv.VolName)
		if err1 != nil {
			log.LogErrorf("action[loadEcPartitions] err:%v", err1.Error())
			continue
		}
		if vol.ID != edpv.VolID {
			Warn(c.Name, fmt.Sprintf("action[loadEcPartitions] has duplicate vol[%v],vol.ID[%v],edpv.VolID[%v]", edpv.VolName, vol.ID, edpv.VolID))
			continue
		}
		ep := newEcDataPartition(edpv.PartitionID, edpv.VolName, edpv.VolID, edpv.DataUnitsNum, edpv.ParityUnitsNum, edpv.StripeUnitSize, edpv.ExtentFileSize)
		ep.Hosts = strings.Split(edpv.Hosts, underlineSeparator)
		for _, rv := range edpv.Replicas {
			ep.afterCreation(rv.Addr, rv.DiskPath, c)
		}
		vol.ecDataPartitions.put(ep)
		log.LogInfof("action[loadEcPartitions],vol[%v],ep[%v]", vol.Name, ep.PartitionID)
	}
	return
}

func (vol *Vol) deleteEcPartitionsFromStore(c *Cluster) {
	vol.ecDataPartitions.RLock()
	defer vol.ecDataPartitions.RUnlock()
	for _, ep := range vol.ecDataPartitions.partitions {
		c.syncDeleteEcDataPartition(ep)
	}

}

func (vol *Vol) getTasksToDeleteEcPartitions() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	vol.ecDataPartitions.RLock()
	defer vol.ecDataPartitions.RUnlock()

	for _, ep := range vol.ecDataPartitions.partitions {
		for _, replica := range ep.ecReplicas {
			tasks = append(tasks, ep.createTaskToDeleteEcPartition(replica.Addr))
		}
	}
	return
}

func (vol *Vol) deleteEcReplicaFromEcNodePessimistic(c *Cluster, task *proto.AdminTask) {
	ep, err := vol.getEcPartitionByID(task.PartitionID)
	if err != nil {
		return
	}
	ecNode, err := c.ecNode(task.OperatorAddr)
	if err != nil {
		return
	}
	_, err = ecNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteEcReplicaFromEcNodePessimistic] vol[%v],data partition[%v],err[%v]", ep.VolName, ep.PartitionID, err)
		return
	}
	ep.Lock()
	ep.removeReplicaByAddr(ecNode.Addr)
	ep.checkAndRemoveMissReplica(ecNode.Addr)
	if err = ep.update("deleteEcReplicaFromEcNodePessimistic", ep.VolName, ep.Hosts, c); err != nil {
		ep.Unlock()
		return
	}
	ep.Unlock()

	return
}
