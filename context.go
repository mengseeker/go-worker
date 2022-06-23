package worker

import "context"

type Context interface {
	context.Context
	Meta() *Meta
}

type ContextKey string

const (
	ContextKeyMeta ContextKey = "meta"
)

type workContext struct {
	context.Context
}

func NewContext(ctx context.Context, m *Meta) Context {
	ctx = context.WithValue(ctx, ContextKeyMeta, m)
	return &workContext{ctx}
}

func (ctx workContext) Meta() *Meta {
	m, _ := ctx.Value(ContextKeyMeta).(*Meta)
	return m
}
