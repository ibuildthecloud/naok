/*
Copyright 2024 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package informer

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

func TestSyntheticWatcher(t *testing.T) {
	dynamicClient := NewMockResourceInterface(gomock.NewController(t))
	var err error
	cs1 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs1",
			UID:             "1",
			ResourceVersion: "rv1.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs1"}},
	}
	cs2 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs2",
			UID:             "2",
			ResourceVersion: "rv2.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs2"}},
	}
	cs3 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs3",
			UID:             "3",
			ResourceVersion: "rv3.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs3"}},
	}
	cs4 := v1.ComponentStatus{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ComponentStatus",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cs4",
			UID:             "4",
			ResourceVersion: "rv4.1",
		},
		Conditions: []v1.ComponentCondition{v1.ComponentCondition{Type: "Healthy", Status: v1.ConditionTrue, Message: "hi from cs4"}},
	}
	list, err := makeCSList(cs1, cs2, cs3, cs4)
	assert.Nil(t, err)
	dynamicClient.EXPECT().List(gomock.Any(), gomock.Any()).Return(list, nil)
	cs1b := cs1.DeepCopy()
	cs1b.ObjectMeta.ResourceVersion = "rv1.2"
	cs2b := cs2.DeepCopy()
	cs2b.ObjectMeta.ResourceVersion = "rv2.2"
	list2, err := makeCSList(*cs1b, *cs2b, cs4)
	assert.Nil(t, err)
	dynamicClient.EXPECT().List(gomock.Any(), gomock.Any()).AnyTimes().Return(list2, nil)

	sw := newSyntheticWatcher()
	pollingInterval := 10 * time.Millisecond
	watchFunc := func(options metav1.ListOptions) (watch.Interface, error) {
		return sw.watch(dynamicClient, options, pollingInterval)
	}
	options := metav1.ListOptions{}
	w, err := watchFunc(options)
	assert.Nil(t, err)
	errChan := make(chan error)
	things := make([]thingType, 0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		things, err = handleAnyWatch(w, errChan, sw.stopChan)
		wg.Done()
	}()

	go func() {
		time.Sleep(30 * time.Millisecond)
		sw.stopChan <- struct{}{}
		wg.Done()
	}()
	wg.Wait()
	// Verify we get what we expected to see
	assert.Len(t, things, 8)
	for i, _ := range list.Items {
		assert.Equal(t, "added-result", things[i].eventName)
	}
	assert.Equal(t, "modified-result", things[len(list.Items)].eventName)
	assert.Equal(t, "modified-result", things[len(list.Items)+1].eventName)
	assert.Equal(t, "deleted-result", things[len(list.Items)+2].eventName)
	assert.Equal(t, "stop", things[7].eventName)
	// And make sure the events are coming in roughly every interval apart.
	timeDelta := things[4].createdAt.Sub(things[0].createdAt)
	assert.Greater(t, float64(timeDelta), 0.9*float64(pollingInterval))
}

func makeCSList(objs ...v1.ComponentStatus) (*unstructured.UnstructuredList, error) {
	unList := make([]unstructured.Unstructured, len(objs))
	for i, cs := range objs {
		unst, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cs)
		if err != nil {
			return nil, err
		}
		unList[i] = unstructured.Unstructured{Object: unst}
	}
	//NOTE: watch out for references as we go....
	list := &unstructured.UnstructuredList{
		Items: unList,
	}
	return list, nil
}

type thingType struct {
	createdAt time.Time
	eventName string
	payload   interface{}
}

func makeThing(eventName string, payload interface{}) thingType {
	return thingType{
		createdAt: time.Now(),
		eventName: eventName,
		payload:   payload,
	}
}

// Grab a skeleton watch processor from k8s.io/client-go@v0.31.2/tools/cache/reflector.go
func handleAnyWatch(w watch.Interface,
	errCh chan error,
	stopCh <-chan struct{},
) ([]thingType, error) {
	things := make([]thingType, 0)
loop:
	for {
		select {
		case <-stopCh:
			things = append(things, makeThing("stop", nil))
			return things, nil
		case err := <-errCh:
			things = append(things, makeThing("error", err))
			return things, err
		case event, ok := <-w.ResultChan():
			if !ok {
				things = append(things, makeThing("bad-result", nil))
				break loop
			}
			//things = append(things, makeThing("result", &event))
			//meta, err := meta.Accessor(event.Object)
			//if err != nil {
			//	things = append(things, makeThing("unmeta-object", fmt.Errorf("%s: unable to understand watch event %#v", name, event)))
			//	continue
			//}
			//resourceVersion := meta.GetResourceVersion()
			switch event.Type {
			case watch.Added:
				things = append(things, makeThing("added-result", &event))
			case watch.Modified:
				things = append(things, makeThing("modified-result", &event))
			case watch.Deleted:
				things = append(things, makeThing("deleted-result", &event))
			case watch.Bookmark:
				things = append(things, makeThing("bookmark-result", &event))
			default:
				things = append(things, makeThing("unexpected-result", &event))
			}
		}
	}
	return things, nil
}
