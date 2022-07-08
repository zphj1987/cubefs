// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cubefs/cubefs/blobstore/scheduler/base (interfaces: IAllocVunit)

// Package base is a generated GoMock package.
package base

import (
	context "context"
	reflect "reflect"

	proto "github.com/cubefs/cubefs/blobstore/common/proto"
	client "github.com/cubefs/cubefs/blobstore/scheduler/client"
	gomock "github.com/golang/mock/gomock"
)

// MockAllocVunit is a mock of IAllocVunit interface.
type MockAllocVunit struct {
	ctrl     *gomock.Controller
	recorder *MockAllocVunitMockRecorder
}

// MockAllocVunitMockRecorder is the mock recorder for MockAllocVunit.
type MockAllocVunitMockRecorder struct {
	mock *MockAllocVunit
}

// NewMockAllocVunit creates a new mock instance.
func NewMockAllocVunit(ctrl *gomock.Controller) *MockAllocVunit {
	mock := &MockAllocVunit{ctrl: ctrl}
	mock.recorder = &MockAllocVunitMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAllocVunit) EXPECT() *MockAllocVunitMockRecorder {
	return m.recorder
}

// AllocVolumeUnit mocks base method.
func (m *MockAllocVunit) AllocVolumeUnit(arg0 context.Context, arg1 proto.Vuid) (*client.AllocVunitInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllocVolumeUnit", arg0, arg1)
	ret0, _ := ret[0].(*client.AllocVunitInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocVolumeUnit indicates an expected call of AllocVolumeUnit.
func (mr *MockAllocVunitMockRecorder) AllocVolumeUnit(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllocVolumeUnit", reflect.TypeOf((*MockAllocVunit)(nil).AllocVolumeUnit), arg0, arg1)
}