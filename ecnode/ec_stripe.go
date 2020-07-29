package ecnode

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/codecnode"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
)

type stripe struct {
	id          int
	partitionID uint64
	extentID    uint64
	offset      int64
	size        uint32
}

func (s *stripe) ReadStripe() {

}

func (s *stripe) ReconstructStripe() {

}

func (ep *EcPartition) readFromEcNode(partitionID uint64, extentID uint64, offset int64, size uint32, stripeIndex int) ([]byte, error) {
	dataMap := &sync.Map{}
	parityMap := &sync.Map{}
	dataNodeWaitGroup := sync.WaitGroup{}
	dataNodeWaitGroup.Add(int(ep.DataNodeNum))
	parityNodeWaitGroup := sync.WaitGroup{}
	parityNodeWaitGroup.Add(int(ep.ParityNodeNum))

	// read from DataNode
	for i := 0; i < int(ep.DataNodeNum); i++ {
		nodeAddr := ep.Hosts[i]
		go func(nodeIndex int, nodeAddr string, partitionID uint64, extentID uint64, offset int64, size uint32) {
			request := repl.NewExtentStripeRead(partitionID, extentID, offset, size)
			defer func() {
				dataNodeWaitGroup.Done()
			}()

			ep.doRead(request, nodeIndex, nodeAddr, dataMap)
		}(i, nodeAddr, partitionID, extentID, offset, size)
	}

	// read from ParityNode
	for i := int(ep.DataNodeNum); i < len(ep.Hosts); i++ {
		nodeAddr := ep.Hosts[i]
		go func(nodeIndex int, nodeAddr string, partitionID uint64, extentID uint64, offset int64, size uint32) {
			request := repl.NewExtentStripeRead(partitionID, extentID, offset, size)
			defer func() {
				parityNodeWaitGroup.Done()
			}()

			ep.doRead(request, nodeIndex, nodeAddr, dataMap)
		}(i, nodeAddr, partitionID, extentID, offset, size)
	}

	dataNodeWaitGroup.Wait()

	validDataCount := 0
	fullDataBytes := make([][]byte, 0)
	for i := 0; i < int(ep.DataNodeNum); i++ {
		value, ok := dataMap.Load(i)
		if !ok {
			fullDataBytes[i] = nil
		} else {
			packet := value.(repl.Packet)
			fullDataBytes[i] = packet.Data
			validDataCount += 1
		}
	}

	if validDataCount == int(ep.DataNodeNum) {
		return joinBytes(fullDataBytes), nil
	}

	parityNodeWaitGroup.Wait()
	nextIndex := len(fullDataBytes)
	for i := 0; i < int(ep.ParityNodeNum); i++ {
		value, ok := parityMap.Load(i)
		if !ok {
			fullDataBytes[nextIndex+i] = nil
		} else {
			packet := value.(repl.Packet)
			fullDataBytes[nextIndex+i] = packet.Data
			validDataCount += 1
		}
	}

	if validDataCount >= int(ep.DataNodeNum) {
		return ep.reconstructData(fullDataBytes, ep.DataNodeNum)
	} else {
		return []byte{}, errors.New("no enough data for reconstruct")
	}
}

func joinBytes(pBytes [][]byte) []byte {
	sep := []byte("")
	return bytes.Join(pBytes, sep)
}

func (ep *EcPartition) reconstructData(pBytes [][]byte, len uint32) ([]byte, error) {
	coder, err := codecnode.NewEcCoder(int(ep.StripeUnitSize), int(ep.DataNodeNum), int(ep.ParityNodeNum))
	if err != nil {
		return []byte{}, errors.New(fmt.Sprintf("NewEcCoder error:%s", err))
	}

	err = coder.Reconstruct(pBytes)
	if err != nil {
		return []byte{}, errors.New(fmt.Sprintf("reconstruct data error:%s", err))
	}

	// TODO liuchengyu write back data
	return joinBytes(pBytes[0:len]), nil
}

func (ep *EcPartition) doRead(request *repl.Packet, nodeIndex int, nodeAddr string, dataMap *sync.Map) {
	err := DoRequest(request, nodeAddr)
	if err != nil {
		log.LogErrorf("Stripe RemoteRead EcPartition(%v) from host(%v) error(%v)", request.PartitionID,
			nodeAddr, err)
		return
	}

	if request.ResultCode != proto.OpOk {
		log.LogErrorf("Stripe RemoteRead EcPartition(%v) from host(%v) error(%v) resultCode(%v)",
			request.PartitionID, nodeAddr, err, request.ResultCode)
		return
	}

	dataMap.Store(nodeIndex, request)
}
