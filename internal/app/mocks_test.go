package app

import (
	"context"
	"sync"

	"github.com/sp/job-runner/internal/jobs"
)

// fakeTarget lets each test decide what Execute does.
type fakeTarget struct {
	executeFunc func(ctx context.Context, name string, input []byte) ([]byte, error)
}

func (f fakeTarget) Execute(ctx context.Context, name string, input []byte) ([]byte, error) {
	return f.executeFunc(ctx, name, input)
}

// fakeJobsClient returns a batch of jobs on GetJobs call and
// records every posted result. done is closed once each job has a result.
// mu is a mutex used to prevent concurrent reads/writes
type fakeJobsClient struct {
	mu      sync.Mutex
	batch   []jobs.Job
	fetched bool
	posted  map[string]jobs.Result
	done    chan struct{}
}

func newFakeJobsClient(batch []jobs.Job) *fakeJobsClient {
	return &fakeJobsClient{
		batch:  batch,
		posted: make(map[string]jobs.Result),
		done:   make(chan struct{}),
	}
}

func (f *fakeJobsClient) GetJobs(ctx context.Context) ([]jobs.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fetched {
		return nil, nil
	}
	f.fetched = true
	return f.batch, nil
}

func (f *fakeJobsClient) PostResult(ctx context.Context, jobID string, result jobs.Result) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.posted[jobID] = result
	if len(f.posted) == len(f.batch) {
		close(f.done)
	}
	return nil
}
