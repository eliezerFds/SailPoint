package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sp/job-runner/internal/jobs"
	"github.com/sp/job-runner/internal/logging"
)

const (
	maxPollBackoff   = 30 * time.Second
	maxPostAttempts  = 3
	postRetryBackoff = 200 * time.Millisecond
)

// JobsClient fetches jobs from the Jobs service and posts their results back.
type JobsClient interface {
	GetJobs(ctx context.Context) ([]jobs.Job, error)
	PostResult(ctx context.Context, jobID string, result jobs.Result) error
}

// Target runs a named script against a target system.
type Target interface {
	Execute(ctx context.Context, name string, input []byte) ([]byte, error)
}

// JobRunner polls the Jobs service for work, runs each job's script
// using the target system and posts the result back to jobs.
type JobRunner struct {
	jobsClient    JobsClient
	target        Target
	scriptTimeout time.Duration
}

// NewJobRunner returns a JobRunner that uses jobsClient and targetService and provides a timeout.
func NewJobRunner(jobsClient JobsClient, targetService Target, scriptTimeout time.Duration) *JobRunner {
	return &JobRunner{
		jobsClient:    jobsClient,
		target:        targetService,
		scriptTimeout: scriptTimeout,
	}
}

// Run polls jobs for a tasks with an interval. It blocks until ctx is cancelled, then stops polling,
// lets in-progress jobs finish and post their results, and returns.
func (a *JobRunner) Run(ctx context.Context, maxJobs int, pollInterval time.Duration) error {
	switch {
	case maxJobs < 1:
		return fmt.Errorf("maxJobs must be at least 1, got %d", maxJobs)
	case pollInterval <= 0:
		return fmt.Errorf("pollInterval must be positive, got %v", pollInterval)
	case a.scriptTimeout <= 0:
		return fmt.Errorf("scriptTimeout must be positive, got %v", a.scriptTimeout)
	}

	slog.Info("Starting Job Runner", logging.KeyWorkers, maxJobs, logging.KeyPollInterval, pollInterval.String(), logging.KeyScriptTimeout, a.scriptTimeout.String())

	workCtx := context.WithoutCancel(ctx)

	jobsToProcess := make(chan jobs.Job, maxJobs)

	// Wait until the service is shutting down
	go func() {
		<-ctx.Done()
		slog.Info("Shutdown requested, finishing in-progress jobs")
	}()

	// Fetch jobs from the jobs service
	go func() {
		log := slog.With(logging.KeyComponent, "poller")
		defer close(jobsToProcess)
		defer log.Info("Stopped polling for jobs")
		wait := pollInterval
		for {
			retrievedJobs, err := a.jobsClient.GetJobs(ctx)
			// Decide how long to wait before the next poll: back off after a failure,
			// reset after a success.
			switch {
			case err != nil && ctx.Err() != nil:
				log.Debug("Poll interrupted by shutdown", logging.KeyJobCount, len(retrievedJobs))
			case err != nil:
				wait = min(wait*2, maxPollBackoff)
				log.Warn("Failed to get jobs", logging.KeyJobCount, len(retrievedJobs), logging.KeyRetryIn, wait.String(), logging.KeyError, err)
			default:
				wait = pollInterval
				log.Debug("Retrieved jobs", logging.KeyJobCount, len(retrievedJobs))
			}

			// Hand over every job we were given, even alongside an error or during
			// shutdown: they have already been taken from the Jobs service, and
			// workers keep reading until this goroutine closes the channel.
			for _, job := range retrievedJobs {
				jobsToProcess <- job
			}

			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup

	// Start <maxJobs> number of workers to process the jobs concurrently
	for i := range maxJobs {
		wg.Go(func() {
			workerLog := slog.With(logging.KeyComponent, "worker", logging.KeyWorkerID, i)

			// Processes jobs if the worker is free and there are jobs to process.
			// If there are no jobs, it just blocks and waits
			for job := range jobsToProcess {
				log := workerLog.With(logging.KeyJobID, job.ID, logging.KeyScript, job.Script)
				log.Debug("Processing job")

				var result jobs.Result
				output, err := a.processJob(workCtx, job)
				if err != nil {
					log.Warn("Failed to process job", logging.KeyError, err)
					result = jobs.Result{Success: false, Error: err.Error()}
				} else {
					result = jobs.Result{Success: true, Output: output}
				}

				if err := a.postResult(workCtx, job.ID, result); err != nil {
					log.Error("Failed to post result", logging.KeyError, err)
				}
			}
		})
	}

	wg.Wait()
	slog.Info("Job Runner stopped")
	return nil
}

// postResult posts a job's result with exponential backoff.
func (a *JobRunner) postResult(ctx context.Context, jobID string, result jobs.Result) error {
	backoff := postRetryBackoff
	for attempt := 1; ; attempt++ {
		err := a.jobsClient.PostResult(ctx, jobID, result)
		if err == nil {
			return nil
		}
		if attempt == maxPostAttempts {
			return fmt.Errorf("post result after %d attempts: %w", attempt, err)
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return fmt.Errorf("post result after %d attempts: %w", attempt, errors.Join(err, ctx.Err()))
		}
		backoff *= 2
	}
}

// Each worker will process a retrieved job by unmarshalling the input to bytes and sending it to the target service
func (a *JobRunner) processJob(ctx context.Context, job jobs.Job) (map[string]string, error) {
	input, err := json.Marshal(job.Input)
	if err != nil {
		return nil, fmt.Errorf("encode input: %w", err)
	}

	execCtx, cancel := context.WithTimeout(ctx, a.scriptTimeout)
	defer cancel()

	raw, err := a.target.Execute(execCtx, job.Script, input)
	if err != nil {
		return nil, fmt.Errorf("run script: %w", err)
	}

	var output map[string]string
	if err := json.Unmarshal(raw, &output); err != nil {
		return nil, fmt.Errorf("decode output: %w", err)
	}
	return output, nil
}
