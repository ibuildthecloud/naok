/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package informer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/rancher/steve/pkg/sqlcache/partition"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestNewListOptionIndexer(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "NewListOptionIndexer() with no errors returned, should return no error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		// create field table
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		// create field table indexes
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableIndexFmt, id, id)).Return(nil)
		txClient.EXPECT().Commit().Return(nil)

		loi, err := NewListOptionIndexer(fields, store, true)
		assert.Nil(t, err)
		assert.NotNil(t, loi)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from NewIndexer(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, false)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error returned from Begin(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, false)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Exec() when creating fields table, should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from create-labels, should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewListOptionIndexer() with error from Commit(), should return an error", test: func(t *testing.T) {
		txClient := NewMockTXClient(gomock.NewController(t))
		store := NewMockStore(gomock.NewController(t))
		fields := [][]string{{"something"}}
		id := "somename"
		stmt := &sql.Stmt{}
		// logic for NewIndexer(), only interested in if this results in error or not
		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		store.EXPECT().GetName().Return(id).AnyTimes()
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		store.EXPECT().RegisterAfterUpsert(gomock.Any())
		store.EXPECT().Prepare(gomock.Any()).Return(stmt).AnyTimes()
		// end NewIndexer() logic

		store.EXPECT().RegisterAfterUpsert(gomock.Any()).Times(2)
		store.EXPECT().RegisterAfterDelete(gomock.Any()).Times(2)

		store.EXPECT().BeginTx(gomock.Any(), true).Return(txClient, nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsTableFmt, id, `"metadata.name" TEXT, "metadata.creationTimestamp" TEXT, "metadata.namespace" TEXT, "something" TEXT`)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.name", id, "metadata.name")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.namespace", id, "metadata.namespace")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, "metadata.creationTimestamp", id, "metadata.creationTimestamp")).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createFieldsIndexFmt, id, fields[0][0], id, fields[0][0])).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableFmt, id, id)).Return(nil)
		txClient.EXPECT().Exec(fmt.Sprintf(createLabelsTableIndexFmt, id, id)).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewListOptionIndexer(fields, store, true)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestListByOptions(t *testing.T) {
	type testCase struct {
		description           string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedList          *unstructured.UnstructuredList
		returnList            []any
		expectedContToken     string
		expectedErr           error
	}

	testObject := testStoreObject{Id: "something", Val: "a"}
	unstrTestObjectMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&testObject)
	assert.Nil(t, err)

	var tests []testCase
	tests = append(tests, testCase{
		description: "ListByOptions() with no errors returned, should not return an error",
		listOptions: ListOptions{},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions() with an empty filter, should not return an error",
		listOptions: ListOptions{
			Filters: []OrFilter{{[]Filter{}}},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with ChunkSize set should set limit in prepared sql.Stmt",
		listOptions: ListOptions{ChunkSize: 2},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  LIMIT ?`,
		expectedStmtArgs: []interface{}{2},
		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE))`,
		expectedCountStmtArgs: []any{},
		returnList:            []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:          &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken:     "",
		expectedErr:           nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Resume set should set offset in prepared sql.Stmt",
		listOptions: ListOptions{Resume: "4"},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  OFFSET ?`,
		expectedStmtArgs: []interface{}{4},
		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE))`,
		expectedCountStmtArgs: []any{},
		returnList:            []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:          &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken:     "",
		expectedErr:           nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter should select where that filter is true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Op set top NotEq should select where that filter is not true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somevalue"},
						Op:      NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" NOT LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with 1 filter with Partial set to true should select where that partial match on that filter's value is true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.somefield" LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with 1 OrFilter set with multiple filters should select where any of those filters are true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"someothervalue"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"somethirdvalue"},
						Op:      NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    ((f."metadata.somefield" LIKE ? ESCAPE '\') OR (f."metadata.somefield" LIKE ? ESCAPE '\') OR (f."metadata.somefield" NOT LIKE ? ESCAPE '\')) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"%somevalue%", "%someothervalue%", "%somethirdvalue%"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with multiple OrFilters set should select where all OrFilters contain one filter that is true in prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"value1"},
						Op:      Eq,
						Partial: false,
					},
					{
						Field:   []string{"status", "someotherfield"},
						Matches: []string{"value2"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"value3"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test4",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    ((f."metadata.somefield" = ?) OR (f."status.someotherfield" != ?)) AND
    (f."metadata.somefield" = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"value1", "value2", "value3", "test4"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with labels filter should select the label in the prepared sql.Stmt",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "guard.cattle.io"},
						Matches: []string{"lodgepole"},
						Op:      Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test41",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"guard.cattle.io", "%lodgepole%", "test41"},
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions with two labels filters should use a self-join",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"milk"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "horses"},
						Matches: []string{"saddles"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test42",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    (lt1.label = ? AND lt1.value = ?) AND
    (lt2.label = ? AND lt2.value = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"cows", "milk", "horses", "saddles", "test42"},
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions with a mix of one label and one non-label query can still self-join",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "cows"},
						Matches: []string{"butter"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "somefield"},
						Matches: []string{"wheat"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "test43",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value = ?) AND
    (f."metadata.somefield" = ?) AND
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"cows", "butter", "wheat", "test43"},
		returnList:        []any{},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{}}, Items: []unstructured.Unstructured{}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions with only one Sort.Field set should sort on that field only, in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}},
				Orders: []SortOrder{ASC},
			},
		},
		partitions: []partition.Partition{},
		ns:         "test5",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.somefield" ASC`,
		expectedStmtArgs:  []any{"test5"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "sort one field descending",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}},
				Orders: []SortOrder{DESC},
			},
		},
		partitions: []partition.Partition{},
		ns:         "test5a",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.namespace" = ?) AND
    (FALSE)
  ORDER BY f."metadata.somefield" DESC`,
		expectedStmtArgs:  []any{"test5a"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in ascending order first and then sort on the second field in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}, {"status", "someotherfield"}},
				Orders: []SortOrder{ASC, ASC},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.somefield" ASC, f."status.someotherfield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions sorting on two fields should sort on the first field in descending order first and then sort on the second field in ascending order in prepared sql.Stmt",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}, {"status", "someotherfield"}},
				Orders: []SortOrder{DESC, ASC},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.somefield" DESC, f."status.someotherfield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})

	tests = append(tests, testCase{
		description: "ListByOptions sorting when # fields != # sort orders should return an error",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}, {"status", "someotherfield"}},
				Orders: []SortOrder{DESC, ASC, ASC},
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.somefield" DESC, f."status.someotherfield" ASC`,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       fmt.Errorf("sort fields length 2 != sort orders length 3"),
	})

	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.PageSize set should set limit to PageSize in prepared sql.Stmt",
		listOptions: ListOptions{
			Pagination: Pagination{
				PageSize: 10,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  LIMIT ?`,
		expectedStmtArgs: []any{10},
		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE))`,
		expectedCountStmtArgs: []any{},
		returnList:            []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:          &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken:     "",
		expectedErr:           nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.Page and no PageSize set should not add anything to prepared sql.Stmt",
		listOptions: ListOptions{
			Pagination: Pagination{
				Page: 2,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with Pagination.Page and PageSize set limit to PageSize and offset to PageSize * (Page - 1) in prepared sql.Stmt",
		listOptions: ListOptions{
			Pagination: Pagination{
				PageSize: 10,
				Page:     2,
			},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE)
  ORDER BY f."metadata.name" ASC 
  LIMIT ?
  OFFSET ?`,
		expectedStmtArgs: []any{10, 10},

		expectedCountStmt: `SELECT COUNT(*) FROM (SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (FALSE))`,
		expectedCountStmtArgs: []any{},

		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Namespace Partition should select only items where metadata.namespace is equal to Namespace and all other conditions are met in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Namespace: "somens",
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.namespace" = ? AND FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"somens"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a All Partition should select all items that meet all other conditions in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				All: true,
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Passthrough Partition should select all items that meet all other conditions prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Passthrough: true,
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  ORDER BY f."metadata.name" ASC `,
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	tests = append(tests, testCase{
		description: "ListByOptions with a Names Partition should select only items where metadata.name equals an items in Names and all other conditions are met in prepared sql.Stmt",
		partitions: []partition.Partition{
			{
				Names: sets.New[string]("someid", "someotherid"),
			},
		},
		ns: "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.name" IN (?, ?))
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs:  []any{"someid", "someotherid"},
		returnList:        []any{&unstructured.Unstructured{Object: unstrTestObjectMap}, &unstructured.Unstructured{Object: unstrTestObjectMap}},
		expectedList:      &unstructured.UnstructuredList{Object: map[string]interface{}{"items": []map[string]interface{}{unstrTestObjectMap, unstrTestObjectMap}}, Items: []unstructured.Unstructured{{Object: unstrTestObjectMap}, {Object: unstrTestObjectMap}}},
		expectedContToken: "",
		expectedErr:       nil,
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			txClient := NewMockTXClient(gomock.NewController(t))
			store := NewMockStore(gomock.NewController(t))
			stmts := NewMockStmt(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.somefield", "status.someotherfield"},
			}
			if test.description == "ListByOptions with multiple OrFilters set should select where all OrFilters contain one filter that is true in prepared sql.Stmt" {
				fmt.Printf("stop here")
			}
			queryInfo, err := lii.constructQuery(test.listOptions, test.partitions, test.ns, "something")
			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, test.expectedStmt, queryInfo.query)
			if test.expectedStmtArgs == nil {
				test.expectedStmtArgs = []any{}
			}
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)

			stmt := &sql.Stmt{}
			rows := &sql.Rows{}
			objType := reflect.TypeOf(testObject)
			store.EXPECT().BeginTx(gomock.Any(), false).Return(txClient, nil)
			txClient.EXPECT().Stmt(gomock.Any()).Return(stmts).AnyTimes()
			store.EXPECT().Prepare(test.expectedStmt).Do(func(a ...any) {
				fmt.Println(a)
			}).Return(stmt)
			if args := test.expectedStmtArgs; args != nil {
				stmts.EXPECT().QueryContext(gomock.Any(), gomock.Any()).Return(rows, nil).AnyTimes()
			} else if strings.Contains(test.expectedStmt, "LIMIT") {
				stmts.EXPECT().QueryContext(gomock.Any(), args...).Return(rows, nil)
				txClient.EXPECT().Stmt(gomock.Any()).Return(stmts)
				stmts.EXPECT().QueryContext(gomock.Any()).Return(rows, nil)
			} else {
				stmts.EXPECT().QueryContext(gomock.Any()).Return(rows, nil)
			}
			store.EXPECT().GetType().Return(objType)
			store.EXPECT().GetShouldEncrypt().Return(false)
			store.EXPECT().ReadObjects(rows, objType, false).Return(test.returnList, nil)
			store.EXPECT().CloseStmt(stmt).Return(nil)

			if test.expectedCountStmt != "" {
				store.EXPECT().Prepare(test.expectedCountStmt).Return(stmt)
				//store.EXPECT().QueryForRows(context.TODO(), stmt, test.expectedCountStmtArgs...).Return(rows, nil)
				store.EXPECT().ReadInt(rows).Return(len(test.expectedList.Items), nil)
				store.EXPECT().CloseStmt(stmt).Return(nil)
			}
			txClient.EXPECT().Commit()
			list, total, contToken, err := lii.executeQuery(context.TODO(), queryInfo)
			if test.expectedErr == nil {
				assert.Nil(t, err)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
			assert.Equal(t, test.expectedList, list)
			assert.Equal(t, len(test.expectedList.Items), total)
			assert.Equal(t, test.expectedContToken, contToken)
		})
	}
}

func TestConstructQuery(t *testing.T) {
	type testCase struct {
		description           string
		listOptions           ListOptions
		partitions            []partition.Partition
		ns                    string
		expectedCountStmt     string
		expectedCountStmtArgs []any
		expectedStmt          string
		expectedStmtArgs      []any
		expectedErr           error
	}

	var tests []testCase
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles IN statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"somevalue"},
						Op:      In,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.queryField1" IN (?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOT-IN statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"somevalue"},
						Op:      NotIn,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  WHERE
    (f."metadata.queryField1" NOT IN (?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles EXISTS statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    Exists,
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedErr: errors.New("NULL and NOT NULL tests aren't supported for non-label queries"),
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOT-EXISTS statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field: []string{"metadata", "queryField1"},
						Op:    NotExists,
					},
				},
			},
		},
		},
		partitions:  []partition.Partition{},
		ns:          "",
		expectedErr: errors.New("NULL and NOT NULL tests aren't supported for non-label queries"),
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelEqualFull"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelEqualFull", "somevalue"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles == statements for label statements, match partial",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelEqualPartial"},
						Matches: []string{"somevalue"},
						Op:      Eq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles != statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNotEqualFull"},
						Matches: []string{"somevalue"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNotEqualFull", "labelNotEqualFull", "somevalue"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles != statements for label statements, match partial",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNotEqualPartial"},
						Matches: []string{"somevalue"},
						Op:      NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value NOT LIKE ? ESCAPE '\')) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNotEqualPartial", "labelNotEqualPartial", "%somevalue%"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles multiple != statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "notEqual1"},
						Matches: []string{"value1"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "notEqual2"},
						Matches: []string{"value2"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value != ?)) AND
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt2i1 ON o1.key = lt2i1.key
		WHERE lt2i1.label = ?)) OR (lt2.label = ? AND lt2.value != ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"notEqual1", "notEqual1", "value1", "notEqual2", "notEqual2", "value2"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles IN statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelIN"},
						Matches: []string{"somevalue1", "someValue2"},
						Op:      In,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value IN (?, ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelIN", "somevalue1", "someValue2"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOTIN statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNOTIN"},
						Matches: []string{"somevalue1", "someValue2"},
						Op:      NotIn,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) OR (lt1.label = ? AND lt1.value NOT IN (?, ?))) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNOTIN", "labelNOTIN", "somevalue1", "someValue2"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles EXISTS statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelEXISTS"},
						Matches: []string{},
						Op:      Exists,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelEXISTS"},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "TestConstructQuery: handles NOTEXISTS statements for label statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "labelNOTEXISTS"},
						Matches: []string{},
						Op:      NotExists,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (o.key NOT IN (SELECT o1.key FROM "something" o1
		JOIN "something_fields" f1 ON o1.key = f1.key
		LEFT OUTER JOIN "something_labels" lt1i1 ON o1.key = lt1i1.key
		WHERE lt1i1.label = ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"labelNOTEXISTS"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles LessThan statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "numericThing"},
						Matches: []string{"5"},
						Op:      Lt,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value < ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"numericThing", float64(5)},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "TestConstructQuery: handles GreaterThan statements",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				[]Filter{
					{
						Field:   []string{"metadata", "labels", "numericThing"},
						Matches: []string{"35"},
						Op:      Gt,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    (lt1.label = ? AND lt1.value > ?) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"numericThing", float64(35)},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "multiple filters with a positive label test and a negative non-label test still outer-join",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "junta"},
						Matches: []string{"esther"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"golgi"},
						Op:      NotEq,
						Partial: true,
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  WHERE
    ((lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') OR (f."metadata.queryField1" NOT LIKE ? ESCAPE '\')) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"junta", "%esther%", "%golgi%"},
		expectedErr:      nil,
	})
	tests = append(tests, testCase{
		description: "multiple filters and or-filters with a positive label test and a negative non-label test still outer-join and have correct AND/ORs",
		listOptions: ListOptions{Filters: []OrFilter{
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "nectar"},
						Matches: []string{"stash"},
						Op:      Eq,
						Partial: true,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Matches: []string{"landlady"},
						Op:      NotEq,
						Partial: false,
					},
				},
			},
			{
				Filters: []Filter{
					{
						Field:   []string{"metadata", "labels", "lawn"},
						Matches: []string{"reba", "coil"},
						Op:      In,
					},
					{
						Field:   []string{"metadata", "queryField1"},
						Op:      Gt,
						Matches: []string{"2"},
					},
				},
			},
		},
		},
		partitions: []partition.Partition{},
		ns:         "",
		expectedStmt: `SELECT DISTINCT o.object, o.objectnonce, o.dekid FROM "something" o
  JOIN "something_fields" f ON o.key = f.key
  LEFT OUTER JOIN "something_labels" lt1 ON o.key = lt1.key
  LEFT OUTER JOIN "something_labels" lt2 ON o.key = lt2.key
  WHERE
    ((lt1.label = ? AND lt1.value LIKE ? ESCAPE '\') OR (f."metadata.queryField1" != ?)) AND
    ((lt2.label = ? AND lt2.value IN (?, ?)) OR (f."metadata.queryField1" > ?)) AND
    (FALSE)
  ORDER BY f."metadata.name" ASC `,
		expectedStmtArgs: []any{"nectar", "%stash%", "landlady", "lawn", "reba", "coil", float64(2)},
		expectedErr:      nil,
	})

	tests = append(tests, testCase{
		description: "ConstructQuery: sorting when # fields < # sort orders should return an error",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}, {"status", "someotherfield"}},
				Orders: []SortOrder{DESC, ASC, ASC},
			},
		},
		partitions:       []partition.Partition{},
		ns:               "",
		expectedStmt:     "",
		expectedStmtArgs: []any{},
		expectedErr:      fmt.Errorf("sort fields length 2 != sort orders length 3"),
	})

	tests = append(tests, testCase{
		description: "ConstructQuery: sorting when # fields > # sort orders should return an error",
		listOptions: ListOptions{
			Sort: Sort{
				Fields: [][]string{{"metadata", "somefield"}, {"status", "someotherfield"}, {"metadata", "labels", "a1"}, {"metadata", "labels", "a2"}},
				Orders: []SortOrder{DESC, ASC, ASC},
			},
		},
		partitions:       []partition.Partition{},
		ns:               "",
		expectedStmt:     "",
		expectedStmtArgs: []any{},
		expectedErr:      fmt.Errorf("sort fields length 4 != sort orders length 3"),
	})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			store := NewMockStore(gomock.NewController(t))
			i := &Indexer{
				Store: store,
			}
			lii := &ListOptionIndexer{
				Indexer:       i,
				indexedFields: []string{"metadata.queryField1", "status.queryField2"},
			}
			queryInfo, err := lii.constructQuery(test.listOptions, test.partitions, test.ns, "something")
			if test.expectedErr != nil {
				assert.Equal(t, test.expectedErr, err)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, test.expectedStmt, queryInfo.query)
			assert.Equal(t, test.expectedStmtArgs, queryInfo.params)
			assert.Equal(t, test.expectedCountStmt, queryInfo.countQuery)
			assert.Equal(t, test.expectedCountStmtArgs, queryInfo.countParams)
		})
	}
}