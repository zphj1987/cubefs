package ecnode

import (
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/codecnode"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/shopspring/decimal"
	"hash/crc32"
	"sync"
)

type ecExtent struct {
	e               *EcNode
	ep              *EcPartition
	partitionId     uint64
	extentID        uint64
	dataSize        uint64
	partitySize     uint64
	localServerAddr string
	hosts           []string
}

func NewEcExtent(e *EcNode, ep *EcPartition, extentID uint64) *ecExtent {
	nodeNum := int(ep.DataNodeNum + ep.ParityNodeNum)
	nodeIndex := int(extentID % uint64(nodeNum))
	hosts := make([]string, nodeNum)
	for i := 0; i < nodeNum; i++ {
		if nodeIndex >= nodeNum {
			nodeIndex = 0
		}

		hosts[i] = ep.Hosts[nodeIndex]
		nodeIndex = nodeIndex + 1
	}

	return &ecExtent{
		e:               e,
		ep:              ep,
		partitionId:     ep.PartitionID,
		extentID:        extentID,
		dataSize:        ep.StripeUnitSize * uint64(ep.DataNodeNum),
		partitySize:     ep.StripeUnitSize * uint64(ep.ParityNodeNum),
		localServerAddr: e.localServerAddr,
		hosts:           hosts,
	}
}

func (ee *ecExtent) readStripe(index int, nodeAddr string, offset uint64, wg *sync.WaitGroup, data [][]byte) {
	defer wg.Done()

	if nodeAddr == ee.localServerAddr {
		// read locality
		data[index] = make([]byte, ee.ep.StripeUnitSize)
		_, err := ee.readExtentFile(offset, ee.ep.StripeUnitSize, data[index])
		if err != nil {
			log.LogErrorf("read locality[%v] fail. partitionId[%v] extentId[%v] offset[%v] index[%v] err:%v",
				nodeAddr, ee.partitionId, ee.extentID, offset, index, err)
			return
		}

		log.LogDebugf("read locality[%v]. partitionId[%v] extentId[%v] offset[%v] index[%v]",
			nodeAddr, ee.partitionId, ee.extentID, offset, index)
	} else {
		// read from remote node
		request := repl.NewExtentStripeRead(ee.partitionId, ee.extentID, offset, ee.ep.StripeUnitSize)
		err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
		if err != nil || request.ResultCode != proto.OpOk {
			log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] index[%v] err:%v",
				nodeAddr, request, offset, request.Size, request.CRC, index, err)
			return
		}

		log.LogDebugf("read from remote[%v]. packet:%v offset[%v] size[%v] crc[%v] index[%v]",
			nodeAddr, request, offset, request.Size, request.CRC, index)
		data[index] = request.Data
	}
}

func (ee *ecExtent) readExtentFile(offset uint64, size uint64, data []byte) (uint32, error) {
	if ee.ep == nil {
		return 0, proto.ErrNoAvailEcPartition
	}

	store := ee.ep.extentStore
	if store == nil {
		return 0, proto.ErrNoAvailEcPartition
	}

	return store.Read(ee.extentID, int64(offset), int64(size), data, false)
}

func (ee *ecExtent) readStripeData(nodeAddr string, extentFileOffset, curReadSize uint64) (data []byte, crc uint32, err error) {
	if nodeAddr == ee.localServerAddr {
		// read locality
		data = make([]byte, curReadSize)
		crc, err = ee.readExtentFile(extentFileOffset, curReadSize, data)
		if err != nil {
			log.LogErrorf("read locality[%v] fail. partitionId[%v] extentId[%v] offset[%v] size[%v] err:%v",
				nodeAddr, ee.partitionId, ee.extentID, extentFileOffset, curReadSize, err)
			return
		}

		log.LogDebugf("read locality[%v]. partitionId[%v] extentId[%v] offset[%v] size[%v]",
			nodeAddr, ee.partitionId, ee.extentID, extentFileOffset, curReadSize)
	} else {
		// read from remote node
		request := repl.NewExtentStripeRead(ee.partitionId, ee.extentID, extentFileOffset, curReadSize)
		err = DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
		if err != nil || request.ResultCode != proto.OpOk {
			log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] err:%v",
				nodeAddr, request, extentFileOffset, request.Size, request.CRC, err)
			return
		}

		log.LogDebugf("read from remote[%v]. packet:%v offset[%v] size[%v] crc[%v]",
			nodeAddr, request, extentFileOffset, curReadSize, request.CRC)
		data = request.Data
		crc = request.CRC
	}

	return
}

func (ee *ecExtent) calcNode(extentOffset uint64) string {
	stripeUnitSizeDecimal := decimal.NewFromInt(int64(ee.ep.StripeUnitSize))
	extentOffsetDecimal := decimal.NewFromInt(int64(extentOffset))
	div := extentOffsetDecimal.Div(stripeUnitSizeDecimal).Floor()
	index := div.Mod(decimal.NewFromInt(int64(ee.ep.DataNodeNum))).IntPart()
	if index == 0 {
		fmt.Println(extentOffset)
	}
	return ee.hosts[index]
}

func (ee *ecExtent) calcExtentFileOffset(extentOffset uint64) uint64 {
	stripeUnitSizeDecimal := decimal.NewFromInt(int64(ee.ep.StripeUnitSize))
	extentOffsetDecimal := decimal.NewFromInt(int64(extentOffset))
	div := extentOffsetDecimal.Div(stripeUnitSizeDecimal).Floor()
	stripeN := div.Div(decimal.NewFromInt(int64(ee.ep.DataNodeNum))).Floor().IntPart()
	offset := extentOffsetDecimal.Mod(stripeUnitSizeDecimal).IntPart()
	return uint64(stripeN)*ee.ep.StripeUnitSize + uint64(offset)
}

func (ee *ecExtent) calcCanReadSize(extentOffset uint64, canReadSize uint64) uint64 {
	canReadSizeInStripeUnit := ee.ep.StripeUnitSize - extentOffset%ee.ep.StripeUnitSize
	if canReadSize < canReadSizeInStripeUnit {
		return canReadSize
	} else {
		return canReadSizeInStripeUnit
	}
}

func (ee *ecExtent) reconstructStripeData(nodeAddr string, extentFileOffset, curReadSize uint64) ([]byte, error) {
	log.LogDebugf("reconstructStripeData")
	data := ee.readStripeFromFollower(extentFileOffset)
	validDataCount := 0
	nodeIndex := 0
	for i := 0; i < len(ee.hosts); i++ {
		if nodeAddr == ee.hosts[i] {
			nodeIndex = i
			if data[i] != nil && len(data[i]) > 0 {
				return data[i], nil
			}
		}

		if data[i] != nil && len(data[i]) > 0 {
			validDataCount += 1
		}
	}

	if validDataCount >= int(ee.ep.DataNodeNum) {
		err := ee.reconstructData(data)
		if err != nil {
			return nil, errors.New("reconstruct data fail")
		}

		go ee.writeBackReconstructData(nodeAddr, extentFileOffset, data[nodeIndex])

		return data[nodeIndex], nil
	} else {
		return nil, errors.New("no enough data for reconstruct")
	}
}

func (ee *ecExtent) readStripeFromFollower(extentFileOffset uint64) [][]byte {
	ep := ee.ep
	data := make([][]byte, ep.DataNodeNum+ep.ParityNodeNum)
	wg := sync.WaitGroup{}
	wg.Add(int(ep.DataNodeNum))

	// read from DataNode
	for i, nodeAddr := range ee.hosts {
		go ee.readStripe(i, nodeAddr, extentFileOffset, &wg, data)
	}

	wg.Wait()
	return data
}

func (ee *ecExtent) reconstructData(pBytes [][]byte) error {
	ep := ee.ep
	coder, err := codecnode.NewEcCoder(int(ep.StripeUnitSize), int(ep.DataNodeNum), int(ep.ParityNodeNum))
	if err != nil {
		return errors.New(fmt.Sprintf("NewEcCoder error:%s", err))
	}

	err = coder.Reconstruct(pBytes)
	if err != nil {
		return errors.New(fmt.Sprintf("reconstruct data error:%s", err))
	}

	return err
}

func (ee *ecExtent) writeBackReconstructData(nodeAddr string, extentFileOffset uint64, data []byte) {
	request := repl.NewPacket()
	request.ExtentID = ee.extentID
	request.PartitionID = ee.partitionId
	request.ExtentOffset = ee.calcReconstructOffset(extentFileOffset)
	request.Size = uint32(len(data))
	request.Data = data
	request.CRC = crc32.ChecksumIEEE(data)
	request.Opcode = proto.OpWrite
	request.ReqID = proto.GenerateRequestID()
	err := DoRequest(request, nodeAddr, proto.ReadDeadlineTime)
	if err != nil || request.ResultCode != proto.OpOk {
		log.LogWarnf("write back reconstruct data to remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] err:%v",
			nodeAddr, request, request.ExtentOffset, request.Size, request.CRC, err)
	}
}

func (ee *ecExtent) calcReconstructOffset(extentFileOffset uint64) int64 {
	return int64(extentFileOffset / ee.ep.StripeUnitSize * ee.ep.StripeUnitSize)
}
