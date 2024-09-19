package queue

import (
	"errors"
)

var (
	ErrNoMessage    = errors.New("No message")
	ErrTooManyItems = errors.New("Too many items")
)
