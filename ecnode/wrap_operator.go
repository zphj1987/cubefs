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

package ecnode

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
)

func (e *EcNode) OperatePacket(p *repl.Packet, c *net.TCPConn) (err error) {
	sz := p.Size
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	start := time.Now().UnixNano()
	defer func() {
		resultSize := p.Size
		p.Size = sz
		if p.IsErrPacket() {
			err = fmt.Errorf("op(%v) error(%v)", p.GetOpMsg(), string(p.Data[:resultSize]))
			logContent := fmt.Sprintf("action[OperatePacket] %v",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, err))
			log.LogErrorf(logContent)
		} else {
			logContent := fmt.Sprintf("action[OperatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), c.RemoteAddr().String(), start, nil))
			switch p.Opcode {
			default:
				log.LogInfo(logContent)
			}
		}
		p.Size = resultSize
		tpObject.Set(err)
	}()

	switch p.Opcode {
	case proto.OpCreateEcDataPartition:
		e.handlePacketToCreateEcPartition(p)
	case proto.OpEcNodeHeartbeat:
		e.handleHeartbeatPacket(p)
	case proto.OpCreateExtent:
		e.handlePacketToCreateExtent(p)
	case proto.OpWrite:
		e.handleWritePacket(p)
	case proto.OpRead:
		e.handleReadPacket(p, c)
	case proto.OpStreamRead:
		e.handleStreamReadPacket(p, c)
	case proto.OpStreamFollowerRead:
		e.handleStreamReadPacket(p, c)
	case proto.OpChangeEcPartitionMembers:
		e.handelChangeMember(p)
	case proto.OpListExtentsInPartition:
		e.handelListExtentAndUpdatePartition(p, c)
	default:
		p.PackErrorBody(repl.ErrorUnknownOp.Error(), repl.ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode)))
	}
	return
}

// Handle OpCreateEcDataPartition to create new EcPartition
func (e *EcNode) handlePacketToCreateEcPartition(p *repl.Packet) {
	var (
		err error
		ep  *EcPartition
	)

	log.LogDebugf("CreateEcPartition:%v", string(p.Data))
	task := &proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	if err != nil {
		log.LogErrorf("cannnot unmashal adminTask")
		err = fmt.Errorf("cannnot unmashal adminTask")
		return
	}
	if task.OpCode != proto.OpCreateEcDataPartition {
		log.LogErrorf("error unavaliable opcode")
		err = fmt.Errorf("from master Task(%v) failed, error unavaliable opcode(%v), expected opcode(%v)",
			task.ToString(), task.OpCode, proto.OpCreateEcDataPartition)
		return
	}

	request := &proto.CreateEcPartitionRequest{}
	bytes, err := json.Marshal(task.Request)
	err = json.Unmarshal(bytes, request)
	if err != nil {
		log.LogErrorf("cannot convert to CreateEcPartition")
		err = fmt.Errorf("from master Task(%v) cannot convert to CreateEcPartition", task.ToString())
		return
	}

	ep, err = e.space.CreatePartition(request)
	if err != nil {
		log.LogErrorf("cannot create Partition err(%v)", err)
		err = fmt.Errorf("from master Task(%v) cannot create Partition err(%v)", task.ToString(), err)
		return
	}
	p.PacketOkWithBody([]byte(ep.Disk().Path))

	return
}

// Handle OpHeartbeat packet
func (e *EcNode) handleHeartbeatPacket(p *repl.Packet) {
	task := &proto.AdminTask{}
	err := json.Unmarshal(p.Data, task)
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionHeartbeat, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	if err != nil {
		return
	}

	go func() {
		response := &proto.EcNodeHeartbeatResponse{
			Status: proto.TaskSucceeds,
		}
		e.buildHeartbeatResponse(response)

		if task.OpCode == proto.OpEcNodeHeartbeat {
			response.Status = proto.TaskSucceeds
		} else {
			response.Status = proto.TaskFailed
			err = fmt.Errorf("illegal opcode")
			response.Result = err.Error()
		}
		task.Response = *response

		log.LogDebugf(fmt.Sprintf("%v", task))

		err = MasterClient.NodeAPI().ResponseEcNodeTask(task)
		if err != nil {
			log.LogErrorf(err.Error())
			return
		}
	}()
}

// Handle OpCreateExtent packet.
func (e *EcNode) handlePacketToCreateExtent(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionCreateExtent, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := p.Object.(*EcPartition)
	if ep.Available() <= 0 || ep.disk.Status == proto.ReadOnly || ep.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if ep.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	extentId := p.ExtentID
	if extentId == 0 {
		log.LogDebugf("master->ecnode createExtent, packet:%v", p)
		// codecnode -> ecnode for create extent
		extentId, err = ep.GetNewExtentId()
		if err != nil {
			log.LogDebugf("master->ecnode GetNewExtentId fail, error:%v", err)
			err = storage.NoBrokenExtentError
			return
		}

		ok := e.createExtentOnFollower(ep, extentId)
		if !ok {
			log.LogDebugf("master->ecnode createExtentOnFollower fail, newExtentId:%v", extentId)
			err = storage.BrokenExtentError
			return
		}

		p.ExtentID = extentId
		return
	}

	log.LogDebugf("ecnode->ecnode CreateExtent, packet:%v", p)
	// master ecnode -> follower ecnode for create extent
	err = ep.ExtentStore().Create(extentId)
	return
}

func (e *EcNode) handleWritePacket(p *repl.Packet) {
	log.LogDebugf("ActionWrite")

	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionWrite, err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	partition := p.Object.(*EcPartition)
	if partition.Available() <= 0 || partition.disk.Status == proto.ReadOnly || partition.IsRejectWrite() {
		err = storage.NoSpaceError
		return
	} else if partition.disk.Status == proto.Unavailable {
		err = storage.BrokenDiskError
		return
	}

	store := partition.ExtentStore()
	// we only allow write by one stripe unit
	if uint64(p.Size)%partition.StripeUnitSize != 0 && uint64(p.Size) > partition.ExtentFileSize {
		err = errors.New("invalid size, must be in range (StripeUnitSize, ExtentFileSize)")
		return
	}

	if p.Size <= util.BlockSize {
		err = store.Write(p.ExtentID, p.ExtentOffset, int64(p.Size), p.Data, p.CRC, storage.AppendWriteType, p.IsSyncWrite())
		partition.checkIsDiskError(err)
	} else {
		size := p.Size
		offset := 0
		for size > 0 {
			if size <= 0 {
				break
			}
			currSize := util.Min(int(size), util.BlockSize)
			data := p.Data[offset : offset+currSize]
			crc := crc32.ChecksumIEEE(data)
			err = store.Write(p.ExtentID, p.ExtentOffset+int64(offset), int64(currSize), data, crc,
				storage.AppendWriteType, p.IsSyncWrite())
			partition.checkIsDiskError(err)
			if err != nil {
				break
			}
			size -= uint32(currSize)
			offset += currSize
		}
	}
	e.incDiskErrCnt(p.PartitionID, err, WriteFlag)
	return
}

func (e *EcNode) handleStreamReadPacket(p *repl.Packet, connect net.Conn) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("StreamRead fail, err:%v", err)
			p.PackErrorBody(ActionStreamRead, err.Error())
			err = p.WriteToConn(connect)
			if err != nil {
				log.LogDebugf("StreamRead write to client fail, packet:%v err:%v", p, err)
			}
		}
	}()

	ep := p.Object.(*EcPartition)

	ecExtent := NewEcExtent(e, ep, p.ExtentID)
	readSize := uint64(p.Size)
	extentOffset := uint64(p.ExtentOffset)
	for {
		if readSize <= 0 {
			break
		}

		nodeAddr := ecExtent.calcNode(extentOffset)
		extentFileOffset := ecExtent.calcExtentFileOffset(extentOffset)
		curReadSize := ecExtent.calcCanReadSize(extentOffset, readSize)
		data, crc, err := ecExtent.readStripeData(nodeAddr, extentFileOffset, curReadSize)
		log.LogDebugf("StreamRead readStripeData: %v %v %v %v %v %v", nodeAddr, extentFileOffset, curReadSize, len(data), crc, err)
		if err != nil {
			data, err = ecExtent.reconstructStripeData(nodeAddr, extentFileOffset, curReadSize)
			if err != nil {
				err = errors.NewErrorf("StreamRead reconstruct Stripe Data fail, ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset(%v) Size(%v) nodeAddr(%v) extentFileOffset(%v) curReadSize(%v) err:%v",
					p.ReqID, p.PartitionID, p.ExtentID, p.ExtentOffset, p.Size, nodeAddr, extentFileOffset, curReadSize, err)
				return
			}

			crc = crc32.ChecksumIEEE(data)
		}

		reply := NewReadReply(p)
		reply.ExtentOffset = int64(extentOffset)
		reply.Data = data
		reply.Size = uint32(curReadSize)
		reply.CRC = crc
		reply.ResultCode = proto.OpOk
		if err = reply.WriteToConn(connect); err != nil {
			msg := fmt.Sprintf("StreamRead write to client fail. ReqID(%v) PartitionID(%v) ExtentID(%v) ExtentOffset[%v] Size[%v] err:%v",
				reply.ReqID, reply.PartitionID, reply.ExtentID, reply.ExtentOffset, reply.Size, err)
			err = errors.New(msg)
			return
		}

		log.LogDebugf("StreamRead reply packet:%v ExtentOffset[%v] Size[%v] Crc[%v]", reply, reply.ExtentOffset, reply.Size, reply.CRC)
		extentOffset += curReadSize
		readSize -= curReadSize
	}

	p.PacketOkReply()
}

func (e *EcNode) handleReadPacket(p *repl.Packet, c *net.TCPConn) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody(ActionRead, err.Error())
			err = p.WriteToConn(c)
			if err != nil {
				log.LogDebugf("StreamRead write to client fail, packet:%v err:%v", p, err)
			}
		}
	}()

	packet := NewReadReply(p)
	partition := p.Object.(*EcPartition)
	// only allow write by one stripe unit
	if p.Size == 0 || uint64(p.Size) > partition.StripeUnitSize {
		err = errors.New("invalid read size, must be equals StripeUnitSize")
		return
	}

	packet.Data = make([]byte, p.Size)
	tpObject := exporter.NewTPCnt(p.GetOpMsg())
	extent := NewEcExtent(e, partition, packet.ExtentID)
	packet.CRC, err = extent.readExtentFile(uint64(packet.ExtentOffset), uint64(packet.Size), packet.Data)
	partition.checkIsDiskError(err)
	tpObject.Set(err)
	if err == nil {
		packet.ResultCode = proto.OpOk
		err = packet.WriteToConn(c)
		if err != nil {
			log.LogErrorf("Read write to client fail. packet:%v err:%v", packet, err)
		}
	}

	p.PacketOkReply()
	log.LogDebugf("Read reply packet:%v ExtentOffset[%v] Size[%v] Crc[%v], err:%v", p, p.ExtentOffset, p.Size, p.CRC, err)
}

func (e *EcNode) handelListExtentAndUpdatePartition(p *repl.Packet, conn *net.TCPConn) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("handelChangeMember", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

}

func (e *EcNode) handelChangeMember(p *repl.Packet) {
	var err error
	defer func() {
		if err != nil {
			p.PackErrorBody("handelChangeMember", err.Error())
		} else {
			p.PacketOkReply()
		}
	}()

	ep := e.space.Partition(p.PartitionID)
	if ep == nil {
		err = proto.ErrNoAvailEcPartition
		return
	}

	//ep.partitionStatus = proto.ReadOnly
	task := proto.AdminTask{}
	err = json.Unmarshal(p.Data, task)
	request := task.Request.(proto.ChangeEcPartitionMembersRequest)
	count := 0
	wg := sync.WaitGroup{}
	for _, host := range request.Hosts {
		wg.Add(1)
		go e.listExtentAndUpdatePartition(ep, host, &count)
	}

	wg.Wait()
	if count == len(request.Hosts) {
		p.PacketOkReply()
	} else {
	}

}
