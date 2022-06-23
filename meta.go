package worker

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Queue = string

const (
	QueueHigh Queue = "High"
	QueueLow  Queue = "Low"

	DefaultRetryCount = 13
)

type Meta struct {
	ID        string
	Name      string
	PerformAt *time.Time
	Retry     int
	Queue     Queue

	CreatedAt  time.Time
	RetryCount int
	Success    bool
	Error      string
	Raw        []byte
}

func NewMetaByWorker(w Worker, opts ...Option) (*Meta, error) {
	raw, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}
	m := Meta{
		ID:    uuid.NewString(),
		Name:  w.WorkerName(),
		Raw:   raw,
		Retry: DefaultRetryCount,
		Queue: QueueLow,

		CreatedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(&m)
	}
	return &m, nil
}

func (m *Meta) String() string {
	return fmt.Sprintf("%s[%s] %d/%d", m.Name, m.ID, m.RetryCount, m.Retry)
}

type Option func(c *Meta)

// retry < 0 means retry count unlimits
// default WorkerDefaultRetryCount
func WithRetry(retry int) Option {
	return func(c *Meta) {
		c.Retry = retry
	}
}

// set worker execute time
func WithPerformAt(performAt time.Time) Option {
	return func(c *Meta) {
		c.PerformAt = &performAt
	}
}

// set worker Queue
// default low
func WithQueue(q Queue) Option {
	return func(c *Meta) {
		c.Queue = q
	}
}
