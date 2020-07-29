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

package ecnode

import (
	"net/http"
	"strconv"

	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/log"
)

func (e *EcNode) getDiskAPI(w http.ResponseWriter, r *http.Request) {
	return
}

func (e *EcNode) getPartitionsAPI(w http.ResponseWriter, r *http.Request) {
	log.LogDebugf("action[getPartitionsAPI]")

	partitions := make([]interface{}, 0)
	e.space.RangePartitions(func(ep *EcPartition) bool {
		partitions = append(partitions, ep.EcPartitionMetaData)
		return true
	})

	result := &struct {
		Partitions     []interface{} `json:"partitions"`
		PartitionCount int           `json:"partitionCount"`
	}{
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	e.buildSuccessResp(w, result)
	return
}

func (e *EcNode) getPartitionAPI(w http.ResponseWriter, r *http.Request) {
	return
}

func (e *EcNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		extentInfo  *storage.ExtentInfo
	)
	if err = r.ParseForm(); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		e.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := e.space.Partition(partitionID)
	if partition == nil {
		e.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().Watermark(uint64(extentID)); err != nil {
		e.buildFailureResp(w, 500, err.Error())
		return
	}

	e.buildSuccessResp(w, extentInfo)
	return
}

func (e *EcNode) getBlockCrcAPI(w http.ResponseWriter, r *http.Request) {
	return
}

func (e *EcNode) getStatAPI(w http.ResponseWriter, r *http.Request) {
	return
}

func (e *EcNode) getRaftStatusAPI(w http.ResponseWriter, r *http.Request) {
	return
}

func (e *EcNode) buildSuccessResp(w http.ResponseWriter, data interface{}) {
	err := Response(w, http.StatusOK, data, "")
	if err != nil {
		log.LogErrorf("response fail. error:%v, data:%v", err, data)
	}
}

func (e *EcNode) buildFailureResp(w http.ResponseWriter, code int, msg string) {
	err := Response(w, code, nil, msg)
	if err != nil {
		log.LogErrorf("response fail. error:%v, code:%v msg:%v", err, code, msg)
	}
}
