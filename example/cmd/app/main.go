package main

import "github.com/mengseeker/go-worker/example/workers"

func main() {
	if err := workers.Initialize("redis://localhost:6379"); err != nil {
		panic(err)
	}
	job := workers.ExampleWorker{}
	if _, err := workers.DeclareWorker(job); err != nil {
		panic(err)
	}
}
