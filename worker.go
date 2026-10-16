package raptor

import (
	"context"
	"encoding/json"
)

type Worker interface {
	Execute(context.Context, *Job) error
}

// ValueWorkerFunc adapts fn into a Worker that decodes each job's payload into
// a T. A payload that fails to decode is a Permanent failure, so the job goes
// straight to the dead jobs.
func ValueWorkerFunc[T any](fn func(context.Context, *Job, T) error) Worker {
	return &valueWorkerFunc[T]{fn: fn}
}

type valueWorkerFunc[T any] struct {
	fn func(context.Context, *Job, T) error
}

func (w *valueWorkerFunc[T]) Execute(ctx context.Context, job *Job) error {
	var value T
	if err := json.Unmarshal(job.Payload, &value); err != nil {
		return Permanent(err)
	}

	return w.fn(ctx, job, value)
}
