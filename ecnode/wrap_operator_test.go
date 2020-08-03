package ecnode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"net"
	"os"
	"testing"
	"time"
)

func TestEcNode_CreateExtent(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	e := newFakeEcNode(t, nil)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	e.fakeCreateExtent(ep, t)
}

func TestEcNode_handleWritePacket(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	e := newFakeEcNode(t, nil)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	e.fakeCreateExtent(ep, t)

	size := int(ep.StripeUnitSize)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = 0
	}

	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpWrite,
			PartitionID: fakePartitionID,
			ExtentID:    fakeExtentId,
			Size:        uint32(size),
			Data:        data,
			StartT:      time.Now().UnixNano(),
		},
	}

	e.handleWritePacket(p)

	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleWritePacket fail, error msg:%v", p.GetResultMsg())
	}
}

func TestEcNode_handleReadPacket(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	e := newFakeEcNode(t, nil)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	e.fakeCreateExtent(ep, t)

	beforeCrc := e.prepareTestData(t, ep)
	// read
	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpRead,
			PartitionID: fakePartitionID,
			ExtentID:    fakeExtentId,
			Size:        uint32(ep.StripeUnitSize),
			StartT:      time.Now().UnixNano(),
		},
	}

	conn, err := net.Dial("tcp", e.Hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	e.handleReadPacket(p, conn.(*net.TCPConn))
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", p.GetResultMsg())
	}

	if p.Size != 0 && p.CRC != beforeCrc {
		t.Fatalf("handleReadPacket crc is not same")
	}
}

func TestEcNode_handleStreamReadPacket(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	e := newFakeEcNode(t, fakeStreamReadDataHandler)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	e.fakeCreateExtent(ep, t)

	_ = e.prepareTestData(t, ep)
	// read
	p := &repl.Packet{
		Object: ep,
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpStreamRead,
			PartitionID: fakePartitionID,
			ExtentID:    fakeExtentId,
			Size:        512,
			StartT:      time.Now().UnixNano(),
		},
	}

	conn, err := net.Dial("tcp", e.Hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	e.handleStreamReadPacket(p, conn.(*net.TCPConn))
	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", p.GetResultMsg())
	}
}

func TestEcNode_handelChangeMember(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	e := newFakeEcNode(t, nil)
	_ = e.fakeCreateECPartition(t, fakePartitionID)

	//e.handelChangeMember()
}

func TestEcNode_handelListExtensInpartition(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

}

func TestCalc(t *testing.T)  {

	i := uint64(258)
	k := uint64(64)
	y := uint(i/k)
	fmt.Println(y)
}