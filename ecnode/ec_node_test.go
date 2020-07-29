package ecnode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/config"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	fakePartitionID = 1
	testDiskPath    = "/tmp/cfs/disk"
	fakeExtentId    = 1001
)

func TestEcNode_handlePacketToCreateExtent(t *testing.T) {
	e := fakeEcNode(t)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	fakeStartAllEcNode(t, e, ep, fakeCreateExtentPacketHandle)

	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateExtent,
			PartitionID: fakePartitionID,
			StartT:      time.Now().UnixNano(),
		},
	}

	p.Object = ep

	e.handlePacketToCreateExtent(p)
	fmt.Println(p, p.ExtentID)
}


func TestEcNode_createExtentOnFollower(t *testing.T) {
	e := fakeEcNode(t)
	ep := e.fakeCreateECPartition(t, fakePartitionID)
	fakeStartAllEcNode(t, e, ep, fakeCreateExtentPacketHandle)

	ok := e.createExtentOnFollower(ep, fakeExtentId)
	if !ok {
		t.Errorf("createExtentOnFollower() = %v, want true", ok)
	}
}

func fakeEcNode(t *testing.T) *EcNode {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go startFakeServiceForTest(t, ":3000", &wg, nil)
	wg.Wait()

	e := &EcNode{
		clusterID:       "ecnode-cluster",
		port:            "17310",
		cellName:        "cell-01",
		localIP:         "127.0.0.1",
		localServerAddr: "127.0.0.1:17310",
		nodeID:          uint64(123),
		stopC:           make(chan bool),
	}

	path := fmt.Sprintf("%s%d", testDiskPath, time.Now().Nanosecond())
	_ = os.MkdirAll(path, 0766)
	err := e.startSpaceManager(&config.Config{
		Data: map[string]interface{}{
			ConfigKeyDisks: []interface{}{
				fmt.Sprintf("%s:123040", path),
			},
		},
	})

	if err != nil {
		t.Errorf("startSpaceManager() error = %v", err)
	}

	return e
}

func (e *EcNode) fakeCreateECPartition(t *testing.T, partitionId uint64) (ep *EcPartition) {
	req := &proto.CreateEcPartitionRequest{
		PartitionID:    uint64(partitionId),
		PartitionSize:  1024000,
		VolumeID:       "ltptest",
		StripeUnitSize: uint64(4 * 1024),
		DataNodeNum:    uint32(4),
		ParityNodeNum:  uint32(2),
		Hosts: []string{
			"127.0.0.1:17310",
			"127.0.0.1:17311",
			"127.0.0.1:17312",
			"127.0.0.1:17313",
			"127.0.0.1:17314",
			"127.0.0.1:17315",
		},
	}

	task := proto.NewAdminTask(proto.OpCreateEcDataPartition, "127.0.0.1:17310", req)
	body, err := json.Marshal(task)
	p := &repl.Packet{
		Packet: proto.Packet{
			Magic:       proto.ProtoMagic,
			ReqID:       proto.GenerateRequestID(),
			Opcode:      proto.OpCreateEcDataPartition,
			PartitionID: partitionId,
			Data:        body,
			Size:        uint32(len(body)),
			StartT:      time.Now().UnixNano(),
		},
	}

	err = e.Prepare(p)
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

	return e.space.Partition(partitionId)
}

func startFakeServiceForTest(t *testing.T, host string, wg *sync.WaitGroup, handler func(p *repl.Packet)) {
	fmt.Println(fmt.Sprintf("host:%v listening", host))
	l, err := net.Listen("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	wg.Done()

	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		if handler != nil {
			request := repl.NewPacket()
			if err = request.ReadFromConnFromCli(conn, proto.NoReadDeadlineTime); err != nil {
				t.Fatal(err)
			}

			handler(request)
			if err = request.WriteToConn(conn); err != nil {
				t.Fatal(err)
			}
		}

		continue
	}

}

func fakeCreateExtentPacketHandle(request *repl.Packet) {
	request.ExtentID = fakeExtentId
	request.ResultCode = proto.OpOk
}

func fakeStartAllEcNode(t *testing.T, e *EcNode, ep *EcPartition, handler func(p *repl.Packet)) {
	wg := sync.WaitGroup{}
	for _, host := range ep.Hosts {
		if host == e.localServerAddr {
			continue
		}

		wg.Add(1)
		go startFakeServiceForTest(t, host, &wg, handler)
	}
	wg.Wait()
}
