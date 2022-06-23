package main

import "github.com/mengseeker/go-worker/example/workers"

func main() {
	if err := workers.Initialize("redis://localhost:6379"); err != nil {
		panic(err)
	}
	if err := workers.Run(); err != nil {
		panic(err)
	}
}
