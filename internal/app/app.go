package app

import (
	"context"
	"fmt"
	"time"
)

type jobrunner struct {
	scriptTimeout time.Duration
}

// New App returns a new Job Runner app.
//
// scriptTimeout represents the maximum [time.Duration]
// to wait for a script to run against a target system.
func NewApp(scriptTimeout time.Duration) *jobrunner {
	return &jobrunner{
		scriptTimeout: scriptTimeout,
	}
}

// Run starts the Job Runner.
//
// The Job Runner polls the Jobs microservice for work, creates workers to handle
// multiple concurrent jobs, & posts results back to the Jobs microservice.
//
// maxJobs represents the maximum number of workers.
func (a *jobrunner) Run(ctx context.Context, maxJobs int) (err error) {
	fmt.Printf("Starting Job Runner with %d workers\n", maxJobs)
	return
}
