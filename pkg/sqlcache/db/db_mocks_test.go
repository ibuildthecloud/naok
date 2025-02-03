// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/steve/pkg/sqlcache/db (interfaces: Rows,Connection,Encryptor,Decryptor,TXClient)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package db -destination ./db_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db Rows,Connection,Encryptor,Decryptor,TXClient
//

// Package db is a generated GoMock package.
package db

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	transaction "github.com/rancher/steve/pkg/sqlcache/db/transaction"
	gomock "go.uber.org/mock/gomock"
)

// MockRows is a mock of Rows interface.
type MockRows struct {
	ctrl     *gomock.Controller
	recorder *MockRowsMockRecorder
}

// MockRowsMockRecorder is the mock recorder for MockRows.
type MockRowsMockRecorder struct {
	mock *MockRows
}

// NewMockRows creates a new mock instance.
func NewMockRows(ctrl *gomock.Controller) *MockRows {
	mock := &MockRows{ctrl: ctrl}
	mock.recorder = &MockRowsMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRows) EXPECT() *MockRowsMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockRows) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockRowsMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRows)(nil).Close))
}

// Err mocks base method.
func (m *MockRows) Err() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Err")
	ret0, _ := ret[0].(error)
	return ret0
}

// Err indicates an expected call of Err.
func (mr *MockRowsMockRecorder) Err() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Err", reflect.TypeOf((*MockRows)(nil).Err))
}

// Next mocks base method.
func (m *MockRows) Next() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Next")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Next indicates an expected call of Next.
func (mr *MockRowsMockRecorder) Next() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Next", reflect.TypeOf((*MockRows)(nil).Next))
}

// Scan mocks base method.
func (m *MockRows) Scan(arg0 ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range arg0 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Scan", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Scan indicates an expected call of Scan.
func (mr *MockRowsMockRecorder) Scan(arg0 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Scan", reflect.TypeOf((*MockRows)(nil).Scan), arg0...)
}

// MockConnection is a mock of Connection interface.
type MockConnection struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionMockRecorder
}

// MockConnectionMockRecorder is the mock recorder for MockConnection.
type MockConnectionMockRecorder struct {
	mock *MockConnection
}

// NewMockConnection creates a new mock instance.
func NewMockConnection(ctrl *gomock.Controller) *MockConnection {
	mock := &MockConnection{ctrl: ctrl}
	mock.recorder = &MockConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnection) EXPECT() *MockConnectionMockRecorder {
	return m.recorder
}

// BeginTx mocks base method.
func (m *MockConnection) BeginTx(arg0 context.Context, arg1 *sql.TxOptions) (*sql.Tx, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BeginTx", arg0, arg1)
	ret0, _ := ret[0].(*sql.Tx)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BeginTx indicates an expected call of BeginTx.
func (mr *MockConnectionMockRecorder) BeginTx(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BeginTx", reflect.TypeOf((*MockConnection)(nil).BeginTx), arg0, arg1)
}

// Close mocks base method.
func (m *MockConnection) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockConnectionMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockConnection)(nil).Close))
}

// Exec mocks base method.
func (m *MockConnection) Exec(arg0 string, arg1 ...any) (sql.Result, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(sql.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exec indicates an expected call of Exec.
func (mr *MockConnectionMockRecorder) Exec(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockConnection)(nil).Exec), varargs...)
}

// Prepare mocks base method.
func (m *MockConnection) Prepare(arg0 string) (*sql.Stmt, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Prepare", arg0)
	ret0, _ := ret[0].(*sql.Stmt)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Prepare indicates an expected call of Prepare.
func (mr *MockConnectionMockRecorder) Prepare(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Prepare", reflect.TypeOf((*MockConnection)(nil).Prepare), arg0)
}

// MockEncryptor is a mock of Encryptor interface.
type MockEncryptor struct {
	ctrl     *gomock.Controller
	recorder *MockEncryptorMockRecorder
}

// MockEncryptorMockRecorder is the mock recorder for MockEncryptor.
type MockEncryptorMockRecorder struct {
	mock *MockEncryptor
}

// NewMockEncryptor creates a new mock instance.
func NewMockEncryptor(ctrl *gomock.Controller) *MockEncryptor {
	mock := &MockEncryptor{ctrl: ctrl}
	mock.recorder = &MockEncryptorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEncryptor) EXPECT() *MockEncryptorMockRecorder {
	return m.recorder
}

// Encrypt mocks base method.
func (m *MockEncryptor) Encrypt(arg0 []byte) ([]byte, []byte, uint32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encrypt", arg0)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].([]byte)
	ret2, _ := ret[2].(uint32)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// Encrypt indicates an expected call of Encrypt.
func (mr *MockEncryptorMockRecorder) Encrypt(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encrypt", reflect.TypeOf((*MockEncryptor)(nil).Encrypt), arg0)
}

// MockDecryptor is a mock of Decryptor interface.
type MockDecryptor struct {
	ctrl     *gomock.Controller
	recorder *MockDecryptorMockRecorder
}

// MockDecryptorMockRecorder is the mock recorder for MockDecryptor.
type MockDecryptorMockRecorder struct {
	mock *MockDecryptor
}

// NewMockDecryptor creates a new mock instance.
func NewMockDecryptor(ctrl *gomock.Controller) *MockDecryptor {
	mock := &MockDecryptor{ctrl: ctrl}
	mock.recorder = &MockDecryptorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDecryptor) EXPECT() *MockDecryptorMockRecorder {
	return m.recorder
}

// Decrypt mocks base method.
func (m *MockDecryptor) Decrypt(arg0, arg1 []byte, arg2 uint32) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decrypt", arg0, arg1, arg2)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Decrypt indicates an expected call of Decrypt.
func (mr *MockDecryptorMockRecorder) Decrypt(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decrypt", reflect.TypeOf((*MockDecryptor)(nil).Decrypt), arg0, arg1, arg2)
}

// MockTXClient is a mock of TXClient interface.
type MockTXClient struct {
	ctrl     *gomock.Controller
	recorder *MockTXClientMockRecorder
}

// MockTXClientMockRecorder is the mock recorder for MockTXClient.
type MockTXClientMockRecorder struct {
	mock *MockTXClient
}

// NewMockTXClient creates a new mock instance.
func NewMockTXClient(ctrl *gomock.Controller) *MockTXClient {
	mock := &MockTXClient{ctrl: ctrl}
	mock.recorder = &MockTXClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTXClient) EXPECT() *MockTXClientMockRecorder {
	return m.recorder
}

// Cancel mocks base method.
func (m *MockTXClient) Cancel() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cancel")
	ret0, _ := ret[0].(error)
	return ret0
}

// Cancel indicates an expected call of Cancel.
func (mr *MockTXClientMockRecorder) Cancel() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cancel", reflect.TypeOf((*MockTXClient)(nil).Cancel))
}

// Commit mocks base method.
func (m *MockTXClient) Commit() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit")
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockTXClientMockRecorder) Commit() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockTXClient)(nil).Commit))
}

// Exec mocks base method.
func (m *MockTXClient) Exec(arg0 string, arg1 ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Exec indicates an expected call of Exec.
func (mr *MockTXClientMockRecorder) Exec(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockTXClient)(nil).Exec), varargs...)
}

// Stmt mocks base method.
func (m *MockTXClient) Stmt(arg0 *sql.Stmt) transaction.Stmt {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stmt", arg0)
	ret0, _ := ret[0].(transaction.Stmt)
	return ret0
}

// Stmt indicates an expected call of Stmt.
func (mr *MockTXClientMockRecorder) Stmt(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stmt", reflect.TypeOf((*MockTXClient)(nil).Stmt), arg0)
}

// StmtExec mocks base method.
func (m *MockTXClient) StmtExec(arg0 transaction.Stmt, arg1 ...any) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StmtExec", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// StmtExec indicates an expected call of StmtExec.
func (mr *MockTXClientMockRecorder) StmtExec(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StmtExec", reflect.TypeOf((*MockTXClient)(nil).StmtExec), varargs...)
}
