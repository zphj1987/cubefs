package ecnode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
	"sync/atomic"
	"time"
)

func (e *EcNode) createExtentOnFollower(ep *EcPartition, extentId uint64) bool {
	var count int32
	wg := sync.WaitGroup{}
	wg.Add(len(ep.Hosts))
	for _, host := range ep.Hosts {
		go func(addr string) {
			defer wg.Done()

			if addr == e.localServerAddr {
				// create extent locally
				if err := ep.ExtentStore().Create(extentId); err != nil {
					log.LogErrorf("NewCreateExtent locally addr:%v partition:%d extentId:%d error:%v",
						addr, ep.PartitionID, extentId, err)
					return
				}
			} else {
				request := NewCreateExtent(ep.PartitionID, extentId)
				err := DoRequest(request, addr, proto.ReadDeadlineTime)
				if err != nil || request.ResultCode != proto.OpOk {
					log.LogErrorf("NewCreateExtent addr:%v partition:%d extentId:%d error:%v ",
						addr, ep.PartitionID, extentId, err)
					return
				}
			}

			atomic.AddInt32(&count, 1)
		}(host)
	}

	wg.Wait()
	fmt.Printf("master->ecnode createExtent[%v], success count:%d node total count:%d\n", extentId, count, len(ep.Hosts))
	log.LogDebugf("master->ecnode createExtent[%v], success count:%d node total count:%d", extentId, count, len(ep.Hosts))
	if int(count) == len(ep.Hosts) {
		return true
	} else {
		return false
	}
}

func NewCreateExtent(partitionID uint64, extentID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.ExtentID = extentID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpCreateExtent
	p.ReqID = proto.GenerateRequestID()
	p.StartT = time.Now().UnixNano()
	return
}

func (e *EcNode) listExtentAndUpdatePartition(ep *EcPartition, host string, i *int) {

}
