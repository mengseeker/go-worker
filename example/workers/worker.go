package workers

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/mengseeker/go-worker"
	"go.uber.org/zap"
)

var (
	runner    *worker.RedisRunner
	logger, _ = zap.NewDevelopment()
)

// 初始化worker
func Initialize(redisUrl string) error {
	opt, err := redis.ParseURL(redisUrl)
	if err != nil {
		return err
	}
	cli := redis.NewClient(opt)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err = cli.Ping(ctx).Err()
	if err != nil {
		return err
	}
	runner, err = worker.NewRunner(cli, 3, logger.Sugar())
	if err != nil {
		return err
	}
	RegistryWorkers()
	return nil
}

func RegistryWorkers() {
	var err error
	workers := []worker.Worker{
		&ExampleWorker{},
	}
	for _, w := range workers {
		err = runner.RegistryWorker(w)
		if err != nil {
			panic(fmt.Errorf("register worker %s error: %s", w.WorkerName(), err))
		}
	}
}

// 启动worker循环
func Run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	go func() {
		<-ctx.Done()
		<-time.After(time.Second)
		logger.Info("waiting for shutdown, press Ctrl + c to force exit")
		e, ec := signal.NotifyContext(context.Background(), os.Interrupt)
		defer ec()
		<-e.Done()
		os.Exit(1)
	}()
	return runner.Run(ctx)
}

func DeclareWorker(w worker.Worker, opts ...worker.Option) (string, error) {
	m, err := runner.Declare(w, opts...)
	if err != nil {
		return "", err
	}
	return m.ID, nil
}
