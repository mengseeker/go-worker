package worker

type Worker interface {
	WorkerName() string
	Perform(ctx Context) error
}

type TimerWorker interface {
	Worker
	Timer() Times
}
