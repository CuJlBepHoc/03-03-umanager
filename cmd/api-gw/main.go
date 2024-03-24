package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"

	"gitlab.com/robotomize/gb-golang/homework/03-03-umanager/internal/env"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := runMain(ctx); err != nil {
		log.Fatal(err)
	}
}

func runMain(ctx context.Context) error {
	e, err := env.Setup(ctx)
	if err != nil {
		return fmt.Errorf("setup.Setup: %w", err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	httpServer := e.ApiGWHTTPServer

	go func() {
		<-ctx.Done()
		// если посылаем сигнал завершения то завершаем работу нашего сервера
		httpServer.Close()
	}()

	go func() {
		defer wg.Done()

		slog.Info(fmt.Sprintf("api-gw http was started %s", e.Config.ApiGWService.Addr))
		if err := httpServer.ListenAndServe(); err != nil {
			slog.Error("api-gw http server", slog.Any("err", err))
			return
		}

		httpServer.Close()
	}()

	wg.Wait()

	return nil
}
