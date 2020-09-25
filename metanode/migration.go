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

package metanode

import (
	"encoding/json"
	"fmt"
	"time"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/sdk/codec"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	DefaultColdDataThreshold = -1              //time.Hour * 24 * 30
	MigrationInterval        = time.Second * 5 //time.Hour
	UpdateCodecInterval      = time.Second * 5 //time.Minute
	MigrationTaskChanLen     = 1024
)

type MigrationTask struct {
	inode   uint64
}

type MigrationManager struct {
	sync.RWMutex

	cw      *codec.CodecWrapper

	cdt     int64 // ColdDataThreshold

	taskC   chan *MigrationTask
}

func (mp *metaPartition) setColdDataThreshold(cdt int64) {
	mm := mp.manager.metaNode.migrationManager
	mm.Lock()
	mm.cdt = cdt
	mm.Unlock()
	return
}

func (mp *metaPartition) getColdDataThreshold() int64 {
	mm := mp.manager.metaNode.migrationManager
	mm.RLock()
	cdt := mm.cdt
	mm.Unlock()
	return cdt
}

func (mp *metaPartition) AddEcMigrationTask(inodes []uint64) (err error) {
	log.LogInfof("add migration tasks [%v]", inodes)
	// mm := mp.manager.metaNode.migrationManager
	for _, inode := range inodes {
		go mp.migration(inode)
		// mm.taskC <- &MigrationTask{inode: inode}
	}
	return
}

func (mp *metaPartition) startMigrationTask() (err error) {
	mm := mp.manager.metaNode.migrationManager
	mm.cdt = DefaultColdDataThreshold
	mm.taskC = make(chan *MigrationTask, MigrationTaskChanLen)
	go func() {
		ticket := time.NewTimer(MigrationInterval)

		for {
			select {
			case <-ticket.C:
				_, ok := mp.IsLeader()
				if ok {
					go mp.autoMigration()
					ticket.Reset(MigrationInterval)
				}
			case task := <-mm.taskC:
				log.LogInfof("start migration task [%v]", task.inode)
				go mp.migration(task.inode)
			}
		}
	}()

	return
}

func (mp *metaPartition) migration(inode uint64) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("migration falid [%v], inode[%v]", err, inode)
		} else {
			log.LogInfof("issue migration task [%v] success!", inode)
		}
	}()

	ir := mp.getInode(NewInode(inode, 0))
	if (ir.Status != proto.OpOk) {
		log.LogErrorf("migration: get inode [%v] failed", inode)
		err = errors.NewErrorf("inode not found")
		return
	}
	ino := ir.Msg
	ino.SetMigratedMark()

	mm := mp.manager.metaNode.migrationManager

	mm.cw.RLock()
	codecNodes := mm.cw.CodecNodes
	mm.cw.RUnlock()

	if len(codecNodes) == 0 {
		err = errors.New("No valid codecnode")
		return
	}

	log.LogInfof("send migration task [%v]", inode)
	err = mm.cw.BatchInodeMigration([]uint64{inode}, mp.config.PartitionId, mp.config.VolName, codecNodes[0].Addr)
	return
}

func (mp *metaPartition) autoMigration() {
	var err error
	var inodes []uint64
	defer func() {
		if err != nil {
			log.LogErrorf("auto migration falid [%v], inodes[%v]", err, inodes)
		}
		if len(inodes) != 0 {
			log.LogInfof("issue auto migration task [%v] success!", inodes)
		}
	}()

	inodes, err = mp.prepareMigrationTask()
	if err != nil {
		return
	}

	if len(inodes) == 0 {
		return
	}

	mm := mp.manager.metaNode.migrationManager

	mm.cw.RLock()
	codecNodes := mm.cw.CodecNodes
	mm.cw.RUnlock()

	if len(codecNodes) == 0 {
		err = errors.New("No valid codecnode")
		return
	}

	err = mm.cw.BatchInodeMigration(inodes, mp.config.PartitionId, mp.config.VolName, codecNodes[0].Addr)
	return
}

func (mp *metaPartition) prepareMigrationTask() (inodes []uint64, err error) {
	if cdt := mp.getColdDataThreshold(); cdt >= 0 {
		mp.inodeTree.Ascend(func(i BtreeItem) bool {
			ino := i.(*Inode)
			if (!proto.IsRegular(ino.Type)) || (ino.IsMigrated()) {
				return true
			}
			if int64(time.Since(time.Unix(ino.ModifyTime, 0))) > cdt {
				inodes = append(inodes, ino.Inode)
				ino.SetMigratedMark()
			}
			return true
		})
	}

	// valData := proto.IssueMigrationTaskRequest{
	// 	VolName:     mp.config.VolName,
	// 	PartitionId: mp.config.PartitionId,
	// 	Inodes:      inodes,
	// }
	// val, err := json.Marshal(valData)
	// if err != nil {
	// 	return
	// }

	// _, err = mp.submit(opFSMBatchStartMigrate, val)

	return
}

func (mp *metaPartition) issueMigrationTask(inodes []uint64, codecNodeAddr string) (err error) {
	reqData := &proto.IssueMigrationTaskRequest{
		VolName:     mp.config.VolName,
		PartitionId: mp.config.PartitionId,
		Inodes:      inodes,
	}

	p := proto.NewPacket()
	p.Opcode = proto.OpIssueMigrationTask
	p.PartitionID = mp.config.PartitionId
	p.Data, err = json.Marshal(reqData)
	if err != nil {
		return
	}
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()

	conn, err := mp.config.ConnPool.GetConnect(codecNodeAddr)
	defer func() {
		if err != nil {
			mp.config.ConnPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mp.config.ConnPool.PutConnect(conn, NoClosedConnect)
		}
	}()
	if err != nil {
		return
	}

	err = p.WriteToConn(conn)
	if err != nil {
		return
	}
	err = p.ReadFromConn(conn, proto.ReadDeadlineTime)
	if err != nil {
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("[IssueMigrationTaskRequest] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg()))
	}

	return
}

func (m *MetaNode) startMigrationManager() (err error) {
	m.migrationManager = MigrationManager{}

	m.migrationManager.cw, err = codec.NewCodecWrapper(masterClient)

	return
}
