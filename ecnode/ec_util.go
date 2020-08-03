package ecnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"net/http"
)

func DoRequest(request *repl.Packet, addr string, timeoutSec int) (err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}

	defer func() {
		err = conn.Close()
		if err != nil {
			log.LogErrorf("close tcp connection fail, host(%v) error(%v)", addr, err)
		}
	}()

	err = request.WriteToConn(conn)
	if err != nil {
		err = errors.New(fmt.Sprintf("write to host(%v) error(%v)", addr, err))
		return
	}

	err = request.ReadFromConn(conn, timeoutSec) // read the response
	if err != nil {
		err = errors.New(fmt.Sprintf("read from host(%v) error(%v)", addr, err))
		return
	}

	return
}

func Response(w http.ResponseWriter, code int, data interface{}, msg string) (err error) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return
	}

	_, err = w.Write(jsonBody)
	return
}
