package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/nebotan/simplebroker/queue"
)

type messageDto struct {
	Message string `json:"message"`
}

var (
	errorLogger = log.New(os.Stderr, "[ERROR]:HTTP:", log.Ldate|log.Ltime|log.Lmicroseconds)
)

func Setup(queueManager queue.QueueManager, defaultTimeout int) {
	http.Handle("/queue/{queue}", createHandler(queueManager, defaultTimeout))
}

func createHandler(queueManager queue.QueueManager, defaultTimeout int) http.Handler {
	return &handlerImpl{
		queueManager:   queueManager,
		defaultTimeout: defaultTimeout,
	}
}

type handlerImpl struct {
	queueManager   queue.QueueManager
	defaultTimeout int
}

func (h *handlerImpl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.serveGet(w, r)
	case http.MethodPut:
		h.servePut(w, r)
	default:
		http.Error(w, "", http.StatusBadRequest)
		return
	}
}

func (h *handlerImpl) serveGet(w http.ResponseWriter, r *http.Request) {
	// name := r.PathValue("queue") // При использовании httptest без поднятия сервера PathValue не работает
	name := getName(r) // Самописная ф-ция для извлечения из Path имени очереди
	timeout := h.defaultTimeout
	isValid := func() bool {
		if name == "" {
			return false
		}
		timeoutAsStr := r.URL.Query().Get("timeout")
		if timeoutAsStr != "" {
			v, err := strconv.Atoi(timeoutAsStr)
			if err != nil {
				errorLogger.Printf("GET timeout [%s] parse error:%v\n", timeoutAsStr, err)
				return false
			}
			if v <= 0 {
				return false
			}
			timeout = v
		}
		return true
	}()
	if !isValid {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	message, err := h.queueManager.Get(r.Context(), name, timeout)
	if err != nil {
		if errors.Is(err, queue.ErrNoMessage) {
			http.Error(w, "", http.StatusNotFound)
		} else {
			errorLogger.Println("GET QueueManager error:", err)
			http.Error(w, "", http.StatusInternalServerError)
		}
		return
	}
	if err := json.NewEncoder(w).Encode(messageDto{Message: message}); err != nil {
		errorLogger.Println("GET Body JSON encode error:", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (h *handlerImpl) servePut(w http.ResponseWriter, r *http.Request) {
	// name := r.PathValue("queue") // При использовании httptest без поднятия сервера PathValue не работает
	name := getName(r) // Самописная ф-ция для извлечения из Path имени очереди
	var m messageDto
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		errorLogger.Println("PUT Body JSON decode error:", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	if err := h.queueManager.Put(name, m.Message); err != nil {
		if errors.Is(err, queue.ErrTooManyItems) {
			// Мы уперлись в ограничение на число очередей или на число элементов в очереди,
			// поэтому отдаём  StatusTooManyRequests
			http.Error(w, "", http.StatusTooManyRequests)
		}
		errorLogger.Println("PUT QueueManager error:", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func getName(r *http.Request) string {
	// Имя очереди ожидается в последнем компоненте пути
	// В пути должно быть минимум 2 компонента
	// Пустое имя трактуется вызывающим кодом, как некорретный запрос
	pathComponets := strings.Split(r.URL.Path, "/")
	if len(pathComponets) < 2 {
		return ""
	}
	if len(pathComponets[len(pathComponets)-2]) == 0 {
		return ""
	}
	return pathComponets[len(pathComponets)-1]
}
