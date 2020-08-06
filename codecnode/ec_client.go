package codecnode

import (
	"sync"
	"net"
	"hash/crc32"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/errors"
)

var (
	gConnPool = util.NewConnectPool()
)

type EcClient struct {
	volume            string
	masters           []string

	wrapper     	  *EcWrapper
}

func NewEcClient(volume string, masters []string) (client *EcClient, err error) {
	client = new(EcClient)

	client.volume = volume
	client.masters = masters

	if client.wrapper, err = NewEcPartitionWrapper(volume, masters); err != nil {
		return
	}

	return
}

func (c *EcClient) GetPartitionIdForWrite() (pid uint64, err error) {
	exclude := make(map[string]struct{})
	ecp := c.wrapper.getRandomEcPartitionForWrite(exclude)
	if ecp == nil {
		err = errors.New("no writable partition")
		return
	}
	pid = ecp.PartitionID
	return
}

func (c *EcClient) CreateExtentsForWrite(pid uint64, size uint64) (extents []uint64, err error) {
	var ecp *EcPartition
	if ecp, err = c.wrapper.GetEcPartition(pid); err != nil {
		return
	}

	if ecp.ExtentFileSize == 0 || ecp.DataUnitsNum == 0 {
		err = errors.New("invalid partition args")
		return
	}

	extentNum := (size - 1) / uint64(ecp.ExtentFileSize * uint32(ecp.DataUnitsNum)) + 1

	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(ecp.Hosts[0]); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)

	for i := 0; i < int(extentNum); i++ {
		p := proto.NewPacketReqID()
		p.PartitionID = pid
		p.Opcode = proto.OpCreateExtent

		err = p.WriteToConn(conn)

		if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			return
		}

		if p.ResultCode != proto.OpOk {
			err = errors.New("failed to create extent")
			return
		}

		extents = append(extents, p.ExtentID)
	}

	return
}

func (c *EcClient) GetPartitionInfo(pid uint64) (dataNum, parityNum, extentSize, stripeSize uint32, err error) {
	var ecp *EcPartition
	if ecp, err = c.wrapper.GetEcPartition(pid); err != nil {
		return
	}
	dataNum    = uint32(ecp.DataUnitsNum)
	parityNum  = uint32(ecp.ParityUnitsNum)
	extentSize = ecp.ExtentFileSize
	stripeSize = ecp.StripeUnitSize
	return
}

func (c *EcClient) Write(data [][]byte, nStripe uint32, eid, pid uint64) (err error) {
	var ecp *EcPartition
	if ecp, err = c.wrapper.GetEcPartition(pid); err != nil {
		return
	}

	if len(data) != int(ecp.DataUnitsNum + ecp.ParityUnitsNum) {
		err = errors.New("unmatched number of shards")
		return
	}

	var wg sync.WaitGroup
	var mtx sync.Mutex
	count := 0
	for i, h := range ecp.Hosts {
		wg.Add(1)
		go func(block []byte, host string) {
			defer wg.Done()

			if len(block) % int(ecp.StripeUnitSize) != 0 {
				return
			}

			var (
				conn *net.TCPConn
				err  error
			)
			if conn, err = gConnPool.GetConnect(host); err != nil {
				return
			}
			defer gConnPool.PutConnect(conn, true)

			for offset := uint32(0); int(offset) < len(block) && offset < nStripe * ecp.StripeUnitSize;
				offset = offset + ecp.StripeUnitSize {
				p := proto.NewPacketReqID()
				p.Opcode = proto.OpWrite
				p.Data = block[offset : offset + ecp.StripeUnitSize]
				p.ExtentOffset = int64(offset)
				p.Size = ecp.StripeUnitSize
				p.PartitionID = pid
				p.ExtentID = eid
				p.CRC = crc32.ChecksumIEEE(p.Data)

				log.LogDebugf("WriteToEcNode(%v): ReqID(%x), Offset(%v), Size(%v), PartitionID(%v), ExtentID(%v), CRC(%x)",
					host, p.ReqID, p.ExtentOffset, p.Size, p.PartitionID, p.ExtentID, p.CRC)

				if err = p.WriteToConn(conn); err != nil {
					return
				}

				if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
					return
				}

				if p.ResultCode != proto.OpOk {
					err = errors.New("failed to write data")
					return
				}
			}

			mtx.Lock()
			count++
			mtx.Unlock()

		}(data[(i + len(ecp.Hosts) - int(eid) % len(ecp.Hosts)) % len(ecp.Hosts)], h)
	}
	wg.Wait()

	if count != len(ecp.Hosts) {
		return errors.New("failed to write data")
	}
	return
}

func (c *EcClient) Close() error {
	c.wrapper.Stop()
	return nil
}
