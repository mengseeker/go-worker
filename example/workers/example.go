package workers

import (
	"github.com/mengseeker/go-worker"
)

type ExampleWorker struct {
}

func (w ExampleWorker) WorkerName() string {
	return "ExampleWorker"
}

func (w ExampleWorker) Perform(ctx worker.Context) error {
	return nil
}

type CronWorker struct{}

func (w CronWorker) WorkerName() string {
	return "CronWorker"
}

func (w CronWorker) Perform(ctx worker.Context) error {
	return nil
}

func (w CronWorker) Timer() worker.Times {
	return worker.Secondly().Interval(5)
}
