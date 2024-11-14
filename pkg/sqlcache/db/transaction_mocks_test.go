// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/steve/pkg/sqlcache/db/transaction (interfaces: Stmt,SQLTx)
//
// Generated by this command:
//
//	mockgen --build_flags=--mod=mod -package db -destination ./transaction_mocks_test.go github.com/rancher/steve/pkg/sqlcache/db/transaction Stmt,SQLTx
//

// Package db is a generated GoMock package.
package db

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockStmt is a mock of Stmt interface.
type MockStmt struct {
	ctrl     *gomock.Controller
	recorder *MockStmtMockRecorder
}

// MockStmtMockRecorder is the mock recorder for MockStmt.
type MockStmtMockRecorder struct {
	mock *MockStmt
}

// NewMockStmt creates a new mock instance.
func NewMockStmt(ctrl *gomock.Controller) *MockStmt {
	mock := &MockStmt{ctrl: ctrl}
	mock.recorder = &MockStmtMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStmt) EXPECT() *MockStmtMockRecorder {
	return m.recorder
}

// Exec mocks base method.
func (m *MockStmt) Exec(arg0 ...any) (sql.Result, error) {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range arg0 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(sql.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exec indicates an expected call of Exec.
func (mr *MockStmtMockRecorder) Exec(arg0 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockStmt)(nil).Exec), arg0...)
}

// Query mocks base method.
func (m *MockStmt) Query(arg0 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{}
	for _, a := range arg0 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Query", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Query indicates an expected call of Query.
func (mr *MockStmtMockRecorder) Query(arg0 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockStmt)(nil).Query), arg0...)
}

// QueryContext mocks base method.
func (m *MockStmt) QueryContext(arg0 context.Context, arg1 ...any) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryContext", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryContext indicates an expected call of QueryContext.
func (mr *MockStmtMockRecorder) QueryContext(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryContext", reflect.TypeOf((*MockStmt)(nil).QueryContext), varargs...)
}

// MockSQLTx is a mock of SQLTx interface.
type MockSQLTx struct {
	ctrl     *gomock.Controller
	recorder *MockSQLTxMockRecorder
}

// MockSQLTxMockRecorder is the mock recorder for MockSQLTx.
type MockSQLTxMockRecorder struct {
	mock *MockSQLTx
}

// NewMockSQLTx creates a new mock instance.
func NewMockSQLTx(ctrl *gomock.Controller) *MockSQLTx {
	mock := &MockSQLTx{ctrl: ctrl}
	mock.recorder = &MockSQLTxMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSQLTx) EXPECT() *MockSQLTxMockRecorder {
	return m.recorder
}

// Commit mocks base method.
func (m *MockSQLTx) Commit() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit")
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockSQLTxMockRecorder) Commit() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockSQLTx)(nil).Commit))
}

// Exec mocks base method.
func (m *MockSQLTx) Exec(arg0 string, arg1 ...any) (sql.Result, error) {
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
func (mr *MockSQLTxMockRecorder) Exec(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockSQLTx)(nil).Exec), varargs...)
}

// Rollback mocks base method.
func (m *MockSQLTx) Rollback() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback")
	ret0, _ := ret[0].(error)
	return ret0
}

// Rollback indicates an expected call of Rollback.
func (mr *MockSQLTxMockRecorder) Rollback() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockSQLTx)(nil).Rollback))
}

// Stmt mocks base method.
func (m *MockSQLTx) Stmt(arg0 *sql.Stmt) *sql.Stmt {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stmt", arg0)
	ret0, _ := ret[0].(*sql.Stmt)
	return ret0
}

// Stmt indicates an expected call of Stmt.
func (mr *MockSQLTxMockRecorder) Stmt(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stmt", reflect.TypeOf((*MockSQLTx)(nil).Stmt), arg0)
}