package workers

import "github.com/mengseeker/go-worker"

type ExampleWorker struct {
}

func (w ExampleWorker) WorkerName() string {
	return "ExampleWorker"
}

func (w ExampleWorker) Perform(ctx worker.Context) error {
	return nil
}
