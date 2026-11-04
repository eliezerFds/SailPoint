package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sp/job-runner/internal/jobs"
	"github.com/sp/job-runner/internal/target"
)

type jobrunner struct {
	scriptTimeout time.Duration
}

func NewApp(scriptTimeout time.Duration) *jobrunner {
	return &jobrunner{
		scriptTimeout: scriptTimeout,
	}
}

func (a *jobrunner) Run(ctx context.Context, maxJobs int, pollInterval time.Duration) (err error) {
	slog.Info("Starting Job Runner", "workers", maxJobs, "pollInterval", pollInterval)

	jobsToProcess := make(chan jobs.Job, maxJobs)

	go func() {
		defer close(jobsToProcess)
		for {
			retrievedJobs, err := jobs.GetJobs(ctx)
			if err != nil {
				slog.Error("Failed to retrieve jobs", "error", err)
			} else {
				slog.Info("Retrieved jobs", "count", len(retrievedJobs))
				for _, job := range retrievedJobs {
					select {
					case jobsToProcess <- job:
					case <-ctx.Done():
						return
					}
				}
			}

			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for range maxJobs {
		wg.Add(1)
		go func() {

			for job := range jobsToProcess {
				var result jobs.Result
				output, err := a.runScript(ctx, job)
				if err != nil {
					slog.Error("Job failed", "job", job.ID, "script", job.Script, "error", err)
					result = jobs.Result{Success: false, Error: err.Error()}
				} else {
					result = jobs.Result{Success: true, Output: output}
				}

				if err := jobs.PostResult(ctx, job.ID, result); err != nil {
					slog.Error("Failed to post result", "job", job.ID, "error", err)
				}
			}
			wg.Done()
		}()
	}

	return
}

func (a *jobrunner) runScript(ctx context.Context, job jobs.Job) (map[string]string, error) {
	input, err := json.Marshal(job.Input)
	if err != nil {
		return nil, fmt.Errorf("encode input: %w", err)
	}

	execCtx, cancel := context.WithTimeout(ctx, a.scriptTimeout)
	defer cancel()

	raw, err := target.Execute(execCtx, job.Script, input)
	if err != nil {
		return nil, err
	}

	var output map[string]string
	if err := json.Unmarshal(raw, &output); err != nil {
		return nil, fmt.Errorf("decode output: %w", err)
	}
	return output, nil
}
