package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nebotan/simplebroker/handler"
	"github.com/nebotan/simplebroker/queue"
)

func main() {
	port := flag.Int("port", 8080, "HTTP port number")
	defaultTimeout := flag.Int("timeout", 5, "default timeout in seconds")
	maxQueueNum := flag.Int("maxQueueNum", 100, "maximum number of queues")
	maxMessageNumPerQueue := flag.Int("maxMessageNumPerQueue", 10_000, "maximum number of messages in any queue")
	flag.Parse()

	queueManager := queue.NewQueueManager(
		queue.QueueManagerConfig{
			MaxQueueNum:           *maxQueueNum,
			MaxMessageNumPerQueue: *maxMessageNumPerQueue,
		})
	handler.Setup(queueManager, *defaultTimeout)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: nil,
	}
	go func() {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[ERROR]: HTTP server error: %v\n", err)
		}
	}()

	signalCh := make(chan os.Signal, 2)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh

	queueManager.Stop()
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownRelease()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[ERROR]: HTTP server shutdown error: %v\n", err)
	}
}
