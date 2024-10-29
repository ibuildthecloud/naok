// Code generated by MockGen. DO NOT EDIT.
// Source: k8s.io/apimachinery/pkg/watch (interfaces: Interface)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package tablelistconvert -destination ./watch_mocks_test.go k8s.io/apimachinery/pkg/watch Interface
//

// Package tablelistconvert is a generated GoMock package.
package tablelistconvert

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
	watch "k8s.io/apimachinery/pkg/watch"
)

// MockInterface is a mock of Interface interface.
type MockInterface struct {
	ctrl     *gomock.Controller
	recorder *MockInterfaceMockRecorder
}

// MockInterfaceMockRecorder is the mock recorder for MockInterface.
type MockInterfaceMockRecorder struct {
	mock *MockInterface
}

// NewMockInterface creates a new mock instance.
func NewMockInterface(ctrl *gomock.Controller) *MockInterface {
	mock := &MockInterface{ctrl: ctrl}
	mock.recorder = &MockInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockInterface) EXPECT() *MockInterfaceMockRecorder {
	return m.recorder
}

// ResultChan mocks base method.
func (m *MockInterface) ResultChan() <-chan watch.Event {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResultChan")
	ret0, _ := ret[0].(<-chan watch.Event)
	return ret0
}

// ResultChan indicates an expected call of ResultChan.
func (mr *MockInterfaceMockRecorder) ResultChan() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResultChan", reflect.TypeOf((*MockInterface)(nil).ResultChan))
}

// Stop mocks base method.
func (m *MockInterface) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockInterfaceMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockInterface)(nil).Stop))
}
