package ecnode

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"net"
	"testing"
	"time"
)

func TestEcNode_CreatePartition(t *testing.T) {
	e := fakeEcNode(t)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	fakeStartAllEcNode(t, e, ep, fakeCreateExtentPacketHandle)

	fakeCreateExtent(e, t, fakePartitionID, fakeExtentId)
}

func fakeCreateExtent(e *EcNode, t *testing.T, partitionId, extentId uint64) {
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateExtent,
			PartitionID: partitionId,
			StartT:      time.Now().UnixNano(),
			ExtentID:    extentId,
		},
	}

	err := e.Prepare(p)
	if err != nil {
		t.Errorf("Prepare() error = %v", err)
	}

	conn, err := net.Dial("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Close()
	err = e.OperatePacket(p, conn.(*net.TCPConn))
	if err != nil {
		t.Errorf("OperatePacket() error = %v", err)
	}

	err = e.Post(p)
	if err != nil {
		t.Errorf("Post() error = %v", err)
	}
}

func TestEcNode_handleReadPacket(t *testing.T) {
	e := fakeEcNode(t)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	fakeStartAllEcNode(t, e, ep, nil)

}

func TestEcNode_handleStreamReadPacket(t *testing.T) {
	e := fakeEcNode(t)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	fakeStartAllEcNode(t, e, ep, nil)
}

func TestEcNode_handelChangeMember(t *testing.T) {
	e := fakeEcNode(t)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	fakeStartAllEcNode(t, e, ep, nil)

	//e.handelChangeMember()
}

func TestEcNode_handelListExtensInpartition(t *testing.T) {

}
