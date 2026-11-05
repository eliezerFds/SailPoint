package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sp/job-runner/internal/jobs"
	"github.com/sp/job-runner/internal/logging"
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

type jobrunner struct {
	jobsClient    JobsClient
	target        Target
	scriptTimeout time.Duration
}

func NewApp(jobsClient JobsClient, targetService Target, scriptTimeout time.Duration) *jobrunner {
	return &jobrunner{
		jobsClient:    jobsClient,
		target:        targetService,
		scriptTimeout: scriptTimeout,
	}
}

func (a *jobrunner) Run(ctx context.Context, maxJobs int, pollInterval time.Duration) (err error) {
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
		for {
			retrievedJobs, err := a.jobsClient.GetJobs(ctx)
			if err != nil {
				log.Warn("Failed to get jobs", logging.KeyError, err)
			} else {
				log.Debug("Retrieved jobs", logging.KeyJobCount, len(retrievedJobs))
				for _, job := range retrievedJobs {
					select {
					case jobsToProcess <- job:
					case <-ctx.Done():
						return
					}
				}
			}

			// Stop polling for <pollInterval> amount of time
			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup

	// Start <maxJobs> number of workers to process the jobs concurrently
	for i := range maxJobs {
		wg.Add(1)
		// Creates the worker
		go func() {
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

				if err := a.jobsClient.PostResult(workCtx, job.ID, result); err != nil {
					log.Error("Failed to post result", logging.KeyError, err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	slog.Info("Job Runner stopped")
	return
}

// Each worker will process a retrieved job by unmarshalling the input to bytes and sending it to the target service
func (a *jobrunner) processJob(ctx context.Context, job jobs.Job) (map[string]string, error) {
	input, err := json.Marshal(job.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to encode input: %w", err)
	}

	execCtx, cancel := context.WithTimeout(ctx, a.scriptTimeout)
	defer cancel()

	raw, err := a.target.Execute(execCtx, job.Script, input)
	if err != nil {
		return nil, fmt.Errorf("failed to execute target: %w", err)
	}

	var output map[string]string
	if err := json.Unmarshal(raw, &output); err != nil {
		return nil, fmt.Errorf("failed to decode output: %w", err)
	}
	return output, nil
}
