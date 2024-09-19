package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
)

type testQueue struct {
	items []string
}

func (q *testQueue) Get(_ context.Context) (string, error) {
	if len(q.items) == 0 {
		return "", ErrNoMessage
	}
	res := q.items[0]
	q.items = q.items[1:]
	return res, nil
}

func (q *testQueue) Put(message string) error {
	q.items = append(q.items, message)
	return nil
}

func (q *testQueue) Len() int {
	return 0
}

func (q *testQueue) Stop() {
}

func TestQueueManagerBasic(t *testing.T) {
	manager := newQueueManager(
		QueueManagerConfig{
			MaxQueueNum:           100,
			MaxMessageNumPerQueue: 10_000,
		},
		func(_ int) queue {
			return &testQueue{}
		},
	)
	ctx := context.Background()
	for i := range 50 {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			tc := struct {
				name    string
				message string
			}{
				name:    fmt.Sprintf("name%d", i),
				message: fmt.Sprintf("message%d", i),
			}
			_, err := manager.Get(ctx, tc.name, 1)
			if !errors.Is(err, ErrNoMessage) {
				t.Errorf("wrong error: got [%v] want [%v]", err, ErrNoMessage)
			}
			err = manager.Put(tc.name, tc.message)
			if err != nil {
				t.Errorf("unexpected error at Put [%v]", err)
			}
			message, err := manager.Get(ctx, tc.name, 1)
			if err != nil {
				t.Errorf("unexpected error at Get [%v]", err)
			}
			if message != tc.message {
				t.Errorf("wrong message: got [%v] want [%v]", message, tc.message)
			}
		})
	}
}

func TestQueueManagerMaxQueueNum(t *testing.T) {
	const N = 100
	manager := newQueueManager(
		QueueManagerConfig{
			MaxQueueNum:           N,
			MaxMessageNumPerQueue: 10_000,
		},
		func(_ int) queue {
			return &testQueue{}
		},
	)
	for i := range N {
		tc := struct {
			name    string
			message string
		}{
			name:    fmt.Sprintf("name%d", i),
			message: fmt.Sprintf("message%d", i),
		}
		err := manager.Put(tc.name, tc.message)
		if err != nil {
			t.Errorf("unexpected error at Put [%v]", err)
		}
	}
	err := manager.Put("extra_queue", "")
	if !errors.Is(err, ErrTooManyItems) {
		t.Errorf("wrong error: got [%v] want [%v]", err, ErrTooManyItems)
	}
}
