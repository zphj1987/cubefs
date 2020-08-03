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

	e := newFakeEcNode(t, calcDataMd5)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	e.fakeCreateExtent(ep, t)

	beforeMd5 := e.prepareTestData(t, ep)
	fmt.Println("beforeMd5", beforeMd5)

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
	fmt.Println(fmt.Sprintf("%v", e.space.diskList))
	e.handleReadPacket(p, conn.(*net.TCPConn))

	if p.ResultCode != proto.OpOk {
		t.Fatalf("handleReadPacket fail, error msg:%v", p.GetResultMsg())
	}

	afterMd5 := e.ResultMap[e.Hosts[0]]
	fmt.Println("afterMd5", afterMd5)
	if beforeMd5 != afterMd5 {
		t.Fatalf("handleReadPacket md5 is not same")
	}
}

func TestEcNode_handleStreamReadPacket(t *testing.T) {
	// clean data
	defer os.RemoveAll(testBasePath)

	e := newFakeEcNode(t, nil)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	beforeMd5 := e.prepareTestData(t, ep)
	fmt.Println("beforeMd5", beforeMd5)

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
