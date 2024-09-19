package queue

import (
	"container/list"
	"context"
)

// queue опеределяет интерфейс для работы с очередью сообщений
type queue interface {
	// Get извлекает сообщение из начала очереди
	// Если очередь пуста, то ждет в течении timeout или пока contex не отменят и возвращает ошибку ErrNoMessage
	Get(ctx context.Context) (string, error)
	// Put помещает новое сообщение в конец очереди.
	// Может вернуть ошибку ErrTooManyItems, если срабатывает лимит на
	// количество сообщений в одной очереди
	Put(message string) error
	// Stop оставает процессинг в горутине, которая обрабатывает запросы к очереди
	Stop()
}

// queueImpl задает реализацию интерфейса для работы с очередью сообщений
// queueImpl создается через метод newQueue, в котором запускается отдельная горутина для обработки операций с очередью.
type queueImpl struct {
	messages             *listAdapter[string]          // linked list для сообщений в порядке их поступления
	maxMessageNum        int                           // ограничение на мксимальное количество сообщений в очереди
	getWaitStatuses      *listAdapter[*getWaitStatus]  // очередь на ожидание сообщений в порядке поступленния запросов (Get)
	messageCh            chan *messageWithConfirmation // канал для приема новых сообщений (Put)
	getWaitStatusCh      chan *getWaitStatus           // канал для приёма ожидающий запросов на чтение
	expiredGetElementsCh chan *list.Element            // канал для просроченных запросов на чтение сообщений (Get)
	done                 chan struct{}                 // закрытие данного канала означает запрос на прекращение работы очереди
}

type messageWithConfirmation struct {
	message      string
	confirmation chan error
}

func newMessageWithConfirmation(message string) *messageWithConfirmation {
	return &messageWithConfirmation{
		message:      message,
		confirmation: make(chan error, 1), // чтобы не блокировать писателя
	}
}

// newQueue создает новую очередь, скрывая детали реализации за интерфейсом queue
func newQueue(maxMessageNum int) queue {
	return newQueueImpl(maxMessageNum)
}

// newQueueImpl создает новую очередь
func newQueueImpl(maxMessageNum int) *queueImpl {
	res := &queueImpl{
		messages:             newListAdapter[string](),
		maxMessageNum:        maxMessageNum,
		getWaitStatuses:      newListAdapter[*getWaitStatus](),
		messageCh:            make(chan *messageWithConfirmation),
		getWaitStatusCh:      make(chan *getWaitStatus),
		expiredGetElementsCh: make(chan *list.Element),
		done:                 make(chan struct{}),
	}
	// Запуск отдельной новой горутины для обработки запросов к очереди через каналы,
	// что позволяет работать с очередью без блокировок.
	go res.dispatch()
	return res
}

// listAdapter это generic wrapper вокруг двусвязного списка из стандартной библиотеки
type listAdapter[T any] struct {
	data *list.List
}

func newListAdapter[T any]() *listAdapter[T] {
	return &listAdapter[T]{
		data: list.New(),
	}
}

func (a *listAdapter[T]) Push(v T) *list.Element {
	return a.data.PushBack(v)
}

func (a *listAdapter[T]) Pop() T {
	return a.data.Remove(a.data.Front()).(T)
}

func (a *listAdapter[T]) Empty() bool {
	return a.data.Len() == 0
}

func (a *listAdapter[T]) Len() int {
	return a.data.Len()
}

func (a *listAdapter[T]) Peek() T {
	return a.data.Front().Value.(T)
}

type getWaitStatus struct {
	msgCh         chan string
	createdElemCh chan *list.Element
	errCh         chan error
}

func newGetWaitStatus() *getWaitStatus {
	return &getWaitStatus{
		// Для общения с ожидающим клиентом используем буферизованный канал емкостью 1,
		// чтобы не блокировать пишущую горутину
		msgCh:         make(chan string, 1),
		createdElemCh: make(chan *list.Element, 1),
		errCh:         make(chan error, 1),
	}
}

func (q *queueImpl) Get(ctx context.Context) (res string, err error) {
	ws := newGetWaitStatus()
	// Отправляем запрос на ожидание
	q.getWaitStatusCh <- ws
	go func() {
		<-ctx.Done()
		// Контекст истек, сообщаем в главную горутину, что данную запись на ожидаение можно удалять из очереди на ожидание
		expiredGetElem := <-ws.createdElemCh
		q.expiredGetElementsCh <- expiredGetElem
		// Главная горутина обработает полченную запись и запишет в канал ws.errCh ошибку
	}()
	// Ожидаем от горутины диспетчера приход либо сообщения, либо ошибки
	select {
	case res = <-ws.msgCh: // Запрошенное сообщение
	case err = <-ws.errCh: // Например, запрос просрочен
	}
	return
}

// Put помещает сообщение в очередь
func (q *queueImpl) Put(message string) error {
	msg := newMessageWithConfirmation(message)
	// отправляем запрос на добавление нового сообщения
	q.messageCh <- msg
	// Получаем подтверждение принятия сообщения
	return <-msg.confirmation
}

// Stop останавливает горутину, которая обрабатывает запросы пользователя
func (q *queueImpl) Stop() {
	close(q.done)
}

// dispatch разбирает и обратаывает входящие запросы к очереди из главной горутины
func (q *queueImpl) dispatch() {
	for {
		select {
		case <-q.done:
			// Прекращаем обработку по приходу Stop.
			return
		case newMsg := <-q.messageCh:
			// Прием нового сообщения на запись в очередь
			var err error
			if q.messages.Len() >= q.maxMessageNum {
				// Отказываемся принимать это сообщение, чтобы не превысить лимит на число сообщений в очереди
				err = ErrTooManyItems
			} else {
				q.messages.Push(newMsg.message)
			}
			// Подтверждаем принятое сообщение
			newMsg.confirmation <- err
			// Доставляем сообщения
			q.deliverMessages()
		case waitStatus := <-q.getWaitStatusCh:
			// Прием запроса на чтение сообщения из очереди
			createdElem := q.getWaitStatuses.Push(waitStatus)
			waitStatus.createdElemCh <- createdElem
			// Доставляем сообщения в ожидающие запросы
			q.deliverMessages()
		case elem := <-q.expiredGetElementsCh:
			ws := elem.Value.(*getWaitStatus)
			// Сообщаем, что сообщения не дождались
			ws.errCh <- ErrNoMessage
			// Удаляем просроченный запрос за O(1)
			q.getWaitStatuses.data.Remove(elem)
		}
	}
}

// deliverMessages доставляет сообщения в ожидающие Get запросы
func (q *queueImpl) deliverMessages() {
	for !(q.getWaitStatuses.Empty() || q.messages.Empty()) {
		q.getWaitStatuses.Pop().msgCh <- q.messages.Pop()
	}
}
