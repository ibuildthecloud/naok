// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/steve/pkg/stores/sqlpartition (interfaces: Partitioner,UnstructuredStore)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package sqlpartition -destination partition_mocks_test.go github.com/rancher/steve/pkg/stores/sqlpartition Partitioner,UnstructuredStore
//

// Package sqlpartition is a generated GoMock package.
package sqlpartition

import (
	reflect "reflect"

	types "github.com/rancher/apiserver/pkg/types"
	partition "github.com/rancher/steve/pkg/sqlcache/partition"
	gomock "go.uber.org/mock/gomock"
	unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	watch "k8s.io/apimachinery/pkg/watch"
)

// MockPartitioner is a mock of Partitioner interface.
type MockPartitioner struct {
	ctrl     *gomock.Controller
	recorder *MockPartitionerMockRecorder
}

// MockPartitionerMockRecorder is the mock recorder for MockPartitioner.
type MockPartitionerMockRecorder struct {
	mock *MockPartitioner
}

// NewMockPartitioner creates a new mock instance.
func NewMockPartitioner(ctrl *gomock.Controller) *MockPartitioner {
	mock := &MockPartitioner{ctrl: ctrl}
	mock.recorder = &MockPartitionerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPartitioner) EXPECT() *MockPartitionerMockRecorder {
	return m.recorder
}

// All mocks base method.
func (m *MockPartitioner) All(arg0 *types.APIRequest, arg1 *types.APISchema, arg2, arg3 string) ([]partition.Partition, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "All", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]partition.Partition)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// All indicates an expected call of All.
func (mr *MockPartitionerMockRecorder) All(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "All", reflect.TypeOf((*MockPartitioner)(nil).All), arg0, arg1, arg2, arg3)
}

// Store mocks base method.
func (m *MockPartitioner) Store() UnstructuredStore {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Store")
	ret0, _ := ret[0].(UnstructuredStore)
	return ret0
}

// Store indicates an expected call of Store.
func (mr *MockPartitionerMockRecorder) Store() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Store", reflect.TypeOf((*MockPartitioner)(nil).Store))
}

// MockUnstructuredStore is a mock of UnstructuredStore interface.
type MockUnstructuredStore struct {
	ctrl     *gomock.Controller
	recorder *MockUnstructuredStoreMockRecorder
}

// MockUnstructuredStoreMockRecorder is the mock recorder for MockUnstructuredStore.
type MockUnstructuredStoreMockRecorder struct {
	mock *MockUnstructuredStore
}

// NewMockUnstructuredStore creates a new mock instance.
func NewMockUnstructuredStore(ctrl *gomock.Controller) *MockUnstructuredStore {
	mock := &MockUnstructuredStore{ctrl: ctrl}
	mock.recorder = &MockUnstructuredStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockUnstructuredStore) EXPECT() *MockUnstructuredStoreMockRecorder {
	return m.recorder
}

// ByID mocks base method.
func (m *MockUnstructuredStore) ByID(arg0 *types.APIRequest, arg1 *types.APISchema, arg2 string) (*unstructured.Unstructured, []types.Warning, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ByID", arg0, arg1, arg2)
	ret0, _ := ret[0].(*unstructured.Unstructured)
	ret1, _ := ret[1].([]types.Warning)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ByID indicates an expected call of ByID.
func (mr *MockUnstructuredStoreMockRecorder) ByID(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ByID", reflect.TypeOf((*MockUnstructuredStore)(nil).ByID), arg0, arg1, arg2)
}

// Create mocks base method.
func (m *MockUnstructuredStore) Create(arg0 *types.APIRequest, arg1 *types.APISchema, arg2 types.APIObject) (*unstructured.Unstructured, []types.Warning, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", arg0, arg1, arg2)
	ret0, _ := ret[0].(*unstructured.Unstructured)
	ret1, _ := ret[1].([]types.Warning)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Create indicates an expected call of Create.
func (mr *MockUnstructuredStoreMockRecorder) Create(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockUnstructuredStore)(nil).Create), arg0, arg1, arg2)
}

// Delete mocks base method.
func (m *MockUnstructuredStore) Delete(arg0 *types.APIRequest, arg1 *types.APISchema, arg2 string) (*unstructured.Unstructured, []types.Warning, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1, arg2)
	ret0, _ := ret[0].(*unstructured.Unstructured)
	ret1, _ := ret[1].([]types.Warning)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Delete indicates an expected call of Delete.
func (mr *MockUnstructuredStoreMockRecorder) Delete(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockUnstructuredStore)(nil).Delete), arg0, arg1, arg2)
}

// ListByPartitions mocks base method.
func (m *MockUnstructuredStore) ListByPartitions(arg0 *types.APIRequest, arg1 *types.APISchema, arg2 []partition.Partition) ([]unstructured.Unstructured, int, string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListByPartitions", arg0, arg1, arg2)
	ret0, _ := ret[0].([]unstructured.Unstructured)
	ret1, _ := ret[1].(int)
	ret2, _ := ret[2].(string)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// ListByPartitions indicates an expected call of ListByPartitions.
func (mr *MockUnstructuredStoreMockRecorder) ListByPartitions(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListByPartitions", reflect.TypeOf((*MockUnstructuredStore)(nil).ListByPartitions), arg0, arg1, arg2)
}

// Update mocks base method.
func (m *MockUnstructuredStore) Update(arg0 *types.APIRequest, arg1 *types.APISchema, arg2 types.APIObject, arg3 string) (*unstructured.Unstructured, []types.Warning, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Update", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*unstructured.Unstructured)
	ret1, _ := ret[1].([]types.Warning)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Update indicates an expected call of Update.
func (mr *MockUnstructuredStoreMockRecorder) Update(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockUnstructuredStore)(nil).Update), arg0, arg1, arg2, arg3)
}

// WatchByPartitions mocks base method.
func (m *MockUnstructuredStore) WatchByPartitions(arg0 *types.APIRequest, arg1 *types.APISchema, arg2 types.WatchRequest, arg3 []partition.Partition) (chan watch.Event, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WatchByPartitions", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(chan watch.Event)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WatchByPartitions indicates an expected call of WatchByPartitions.
func (mr *MockUnstructuredStoreMockRecorder) WatchByPartitions(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WatchByPartitions", reflect.TypeOf((*MockUnstructuredStore)(nil).WatchByPartitions), arg0, arg1, arg2, arg3)
}
