package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nebotan/simplebroker/queue"
)

type GetIn struct {
	callsNum int
	name     string
	timeout  int
}

type PutIn struct {
	callsNum      int
	name, message string
}

type GetOut struct {
	message string
	err     error
}

type PutOut struct {
	err error
}

type MockQueueManager struct {
	getIn  GetIn
	putIn  PutIn
	getOut GetOut
	putOut PutOut
}

func (m *MockQueueManager) Get(ctx context.Context, name string, timeout int) (string, error) {
	m.getIn.callsNum++
	m.getIn.name = name
	m.getIn.timeout = timeout
	return m.getOut.message, m.getOut.err
}
func (m *MockQueueManager) Put(name, message string) error {
	m.putIn.callsNum++
	m.putIn.name = name
	m.putIn.message = message
	return m.putOut.err
}

func (m *MockQueueManager) Stop() {
}

/**


 */

func TestValidGetRequests(t *testing.T) {
	testCases := []struct {
		description    string
		httpCode       int
		name           string
		timeout        int
		defaultTimeout int
		message        string
		err            error
	}{
		{
			description: "OK with explicit timeout",
			httpCode:    http.StatusOK,
			name:        "name1",
			timeout:     5,
			message:     "message1",
		},
		{
			description: "No message",
			httpCode:    http.StatusNotFound,
			name:        "name2",
			timeout:     7,
			message:     "message2",
			err:         queue.ErrNoMessage,
		},
		{
			description:    "OK with default timeout",
			httpCode:       http.StatusOK,
			name:           "name3",
			defaultTimeout: 10,
			message:        "message3",
		},
		{
			description: "Some error",
			httpCode:    http.StatusInternalServerError,
			name:        "name4",
			timeout:     9,
			message:     "message4",
			err:         errors.New("Some error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			manager := &MockQueueManager{getOut: GetOut{message: tc.message, err: tc.err}}
			handler := createHandler(manager, tc.defaultTimeout)

			w := httptest.NewRecorder()
			var url string
			if tc.timeout != 0 {
				url = fmt.Sprintf("/queue/%s?timeout=%d", tc.name, tc.timeout)
			} else {
				url = fmt.Sprintf("/queue/%s", tc.name)
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			handler.ServeHTTP(w, req)

			if w.Code != tc.httpCode {
				t.Errorf("wrong status code: got %v want %v", w.Code, tc.httpCode)
			}
			if manager.getIn.callsNum != 1 {
				t.Errorf("wrong GET calls number: got %v want %v", manager.getIn.callsNum, 1)
			}
			if manager.putIn.callsNum != 0 {
				t.Errorf("wrong PUT calls number: got %v want %v", manager.putIn.callsNum, 0)
			}
			if manager.getIn.name != tc.name {
				t.Errorf("wrong status code: got %v want %v", manager.getIn.name, tc.name)
			}
			effectiveTimeout := tc.timeout
			if effectiveTimeout == 0 {
				effectiveTimeout = tc.defaultTimeout
			}
			if manager.getIn.timeout != effectiveTimeout {
				t.Errorf("wrong timeout : got %v want %v", manager.getIn.timeout, effectiveTimeout)
			}
			if manager.getOut.message != tc.message {
				t.Errorf("wrong message: got %v want %v", manager.getOut.message, tc.message)
			}
			if !errors.Is(manager.getOut.err, tc.err) {
				t.Errorf("wrong error: got %v want %v", manager.getOut.message, tc.message)
			}

			res := w.Result()
			defer res.Body.Close()
			if w.Code == http.StatusOK {
				var dto messageDto
				if err := json.NewDecoder(res.Body).Decode(&dto); err != nil {
					t.Errorf("json decoding error: %v", err)
				}
				if dto.Message != tc.message {
					t.Errorf("wrong message: got %v want %v", dto.Message, tc.message)
				}
			} else {
				bytes, _ := io.ReadAll(res.Body)
				if string(bytes) != "\n" {
					t.Errorf("responce body is expected to be empty with \n but got: [%s]", string(bytes))
				}
			}
		})
	}
}

func TestInvalidGetRequests(t *testing.T) {
	testCases := []struct {
		description string
		url         string
	}{
		{
			description: "Name is empty",
			url:         "/queue/",
		},
		{
			description: "Name is missed",
			url:         "/queue",
		},
		{
			description: "Timeout is not a number",
			url:         "/queue/name1?timeout=some_string",
		},
		{
			description: "Timeout is negative",
			url:         "/queue/name2?timeout=-1",
		},
		{
			description: "Timeout is zero",
			url:         "/queue/name3?timeout=0",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			manager := &MockQueueManager{}
			const defaultTimeout = 10
			handler := createHandler(manager, defaultTimeout)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.url, nil)
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("wrong status code: got %v want %v", w.Code, http.StatusBadRequest)
			}

			// QueueManager не должен быть вызван в случае некорректного запроса
			if manager.getIn.callsNum != 0 {
				t.Errorf("wrong GET calls number: got %v want %v", manager.getIn.callsNum, 0)
			}
			if manager.putIn.callsNum != 0 {
				t.Errorf("wrong PUT calls number: got %v want %v", manager.putIn.callsNum, 0)
			}
		})
	}
}

func TestValidPutRequests(t *testing.T) {
	testCases := []struct {
		description string
		httpCode    int
		name        string
		message     string
		err         error
	}{
		{
			description: "OK Happy path",
			httpCode:    http.StatusOK,
			name:        "name1",
			message:     "message1",
		},
		{
			description: "Too many requests",
			httpCode:    http.StatusTooManyRequests,
			err:         queue.ErrTooManyItems,
		},
		{
			description: "Some unexpected error",
			httpCode:    http.StatusInternalServerError,
			err:         errors.New("Some unxepected error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			manager := &MockQueueManager{
				putOut: PutOut{
					err: tc.err,
				},
			}
			const defaultTimeout = 10
			handler := createHandler(manager, defaultTimeout)

			w := httptest.NewRecorder()
			body := strings.NewReader(fmt.Sprintf(`{"message": "%s"}`, tc.message))
			req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/queue/%s", tc.name), body)
			handler.ServeHTTP(w, req)

			if w.Code != tc.httpCode {
				t.Errorf("wrong status code: got %v want %v", w.Code, tc.httpCode)
			}

			if manager.getIn.callsNum != 0 {
				t.Errorf("wrong GET calls number: got %v want %v", manager.getIn.callsNum, 0)
			}
			if manager.putIn.callsNum != 1 {
				t.Errorf("wrong PUT calls number: got %v want %v", manager.putIn.callsNum, 1)
			}

			if manager.putIn.name != tc.name {
				t.Errorf("wrong message: got %v want %v", manager.putIn.name, tc.name)
			}
			if manager.putIn.message != tc.message {
				t.Errorf("wrong message: got %v want %v", manager.putIn.message, tc.message)
			}
			if !errors.Is(manager.putOut.err, tc.err) {
				t.Errorf("wrong error: got %v want %v", manager.getOut.err, tc.err)
			}
		})
	}
}

func TestInvalidPutRequests(t *testing.T) {
	testCases := []struct {
		description string
		url         string
		body        string
	}{
		{
			description: "Name is empty",
			url:         "/queue/",
		},
		{
			description: "Name is missed",
			url:         "/queue",
		},
		{
			description: "JSON with invalid syntax 1",
			url:         "/queue/name1",
			body:        `{some strange things`,
		},
		{
			description: "JSON with invalid syntax 2",
			url:         "/queue/name2",
			body:        `{"message": "message2}`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			manager := &MockQueueManager{}
			const defaultTimeout = 10
			handler := createHandler(manager, defaultTimeout)

			w := httptest.NewRecorder()
			body := strings.NewReader(tc.body)
			req := httptest.NewRequest(http.MethodPut, tc.url, body)
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("wrong status code: got %v want %v", w.Code, http.StatusBadRequest)
			}

			// QueueManager не должен быть вызван в случае некорректного запроса
			if manager.getIn.callsNum != 0 {
				t.Errorf("wrong GET calls number: got %v want %v", manager.getIn.callsNum, 0)
			}
			if manager.putIn.callsNum != 0 {
				t.Errorf("wrong PUT calls number: got %v want %v", manager.putIn.callsNum, 0)
			}
		})
	}
}
