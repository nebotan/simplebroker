package queue

import (
	"context"
	"sync"
	"time"
)

// QueueManager задает интерфейс менеджера очередей.
// Очередь доступна по имени.
type QueueManager interface {
	// Get извлекает из очереди, заданной name, сообщение, вызывая метод Get очереди.
	Get(ctx context.Context, name string, timeout int) (string, error)
	// Put кладет в очередь, заданную name, сообщение, вызывая матод Put очереди
	// Может вернуть ошибку ErrTooManyItems, если срабатывает лимит на
	// количество очередей
	Put(name, message string) error
	// Stop останавливает очереди
	Stop()
}

type QueueManagerConfig struct {
	MaxQueueNum           int
	MaxMessageNumPerQueue int
}

// NewQueueManager создает менеджер очередей
func NewQueueManager(config QueueManagerConfig) QueueManager {
	// Наружу выставляем версию со стандартной фабрикой очередей
	return newQueueManager(config, newQueue)
}

// newQueueManager создает менеджер очередей и позволяет мокать очереди для юнит тестов
func newQueueManager(config QueueManagerConfig, factory func(int) queue) QueueManager {
	return &queueManagerImpl{
		config:  config,
		queues:  make(map[string]queue),
		factory: factory,
	}
}

type queueManagerImpl struct {
	config QueueManagerConfig
	queues map[string]queue
	// Чтение мапы с очередями должно быть много чаще, чем запись
	mutex   sync.RWMutex
	factory func(int) queue
}

func (q *queueManagerImpl) Get(ctx context.Context, name string, timeout int) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	var foundQueue queue
	func() {
		q.mutex.RLock()
		defer q.mutex.RUnlock()
		foundQueue = q.queues[name]
	}()
	if foundQueue == nil {
		return "", ErrNoMessage
	}
	return foundQueue.Get(ctx)
}

func (q *queueManagerImpl) Put(name, message string) error {
	var foundQueue queue
	func() {
		q.mutex.RLock()
		defer q.mutex.RUnlock()
		foundQueue = q.queues[name]
	}()
	if foundQueue == nil {
		err := func() error {
			q.mutex.Lock()
			defer q.mutex.Unlock()
			foundQueue = q.queues[name]
			// Проверим, вдруг очереди не было в Read Lock, а при входе в данный Lock очередь уже есть
			if foundQueue != nil {
				return nil
			}
			// Проверяем лимит на число очередей
			if len(q.queues) >= q.config.MaxQueueNum {
				return ErrTooManyItems
			}
			foundQueue = q.factory(q.config.MaxMessageNumPerQueue)
			q.queues[name] = foundQueue
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return foundQueue.Put(message)
}

func (q *queueManagerImpl) Stop() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for _, v := range q.queues {
		v.Stop()
	}
}
