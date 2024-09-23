package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQueueBasic задает простой базовый сценарий тестирования очереди:
// * в очередь помещаются N сообщений
// * читаются N сообщений, проверяется, что прочитанные сообщения равны записанным
// * N+1 запрос на чтение возвращает, что в очереди нет сообщений
// Операции выполняются последовательно в одной горутине
func TestQueueBasic(t *testing.T) {
	const N = 10
	q := newQueue(N)
	defer q.Stop()

	for i := range N {
		err := q.Put(fmt.Sprintf("message%d", i))
		if err != nil {
			t.Errorf("Unexpected exception: %v", err)
		}
	}
	err := q.Put("some_more_message")
	if !errors.Is(err, ErrTooManyItems) {
		t.Errorf("wrong error: got [%v] want [%v]", err, ErrTooManyItems)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	for i := range N {
		message, err := q.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected exception: %v", err)
		}
		expectedMessage := fmt.Sprintf("message%d", i)
		if message != expectedMessage {
			t.Errorf("wrong message: got [%v] want [%v]", message, expectedMessage)
		}
	}
	message, err := q.Get(ctx)
	if !errors.Is(err, ErrNoMessage) {
		t.Errorf("wrong error: got [%v] want [%v]", err, ErrNoMessage)
	}
	if message != "" {
		t.Errorf("message expected to be empty but got [%v]", message)
	}
}

func TestQueueMultiGorutine(t *testing.T) {
	var mutex sync.Mutex
	var counter atomic.Int32
	var errValue atomic.Value
	const M = 5
	const N = 10_000
	messages := make(map[string]int, N*M) // надо заранее подготовиться вместить в мапу все сообщения
	for i := range N * M {
		// фиксируем ожидаемые сообщения
		messages[fmt.Sprintf("message%d", i+1)] = 1
	}
	q := newQueueImpl(N * M)
	defer q.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	writer := func() {
		for range N {
			i := counter.Add(1)
			err := q.Put(fmt.Sprintf("message%d", i))
			if err != nil && errValue.Load() != nil {
				errValue.Store(err)
			}
		}
		wg.Done()
	}
	reader := func() {
		for range N {
			message, err := q.Get(ctx)
			if err != nil && errValue.Load() != nil {
				errValue.Store(err)
			}
			mutex.Lock()
			// Отмечаем прочитанное сообщение инкрементом
			// Изначально для каждого соообщения в мапу была записана 1
			// Каждое сообщение должно быть прочитано один раз, то есть в мапу будут записаны 2
			messages[message]++
			mutex.Unlock()
		}
		wg.Done()
	}
	wg.Add(2 * M)
	for range M {
		go writer()
		go reader()
	}
	wg.Wait()
	if v := errValue.Load(); v != nil {
		t.Logf("Unexpected error: %v", v)
	}
	q.Stop()
	if len(messages) != N*M {
		// Убедимся, что не были прочитаны незапланированые сообщения
		t.Errorf("Unexpected message number: got %v want %v", len(messages), N*M)
	}
	for message, num := range messages {
		if num != 2 {
			t.Errorf("Not all messages was read correctly. Message message [%s] num %d", message, num)
		}
	}
}
