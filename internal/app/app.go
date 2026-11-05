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
	slog.Info("Starting Job Runner", logging.KeyWorkers, maxJobs, logging.KeyPollInterval, pollInterval.String(), logging.KeyScriptTimeout, a.scriptTimeout.String())

	jobsToProcess := make(chan jobs.Job, maxJobs)

	go func() {
		log := slog.With(logging.KeyComponent, "poller")
		defer close(jobsToProcess)
		for {
			retrievedJobs, err := jobs.GetJobs(ctx)
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

			select {
			case <-time.After(pollInterval):
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for i := range maxJobs {
		wg.Add(1)
		go func() {
			workerLog := slog.With(logging.KeyComponent, "worker", logging.KeyWorkerID, i)

			for job := range jobsToProcess {
				log := workerLog.With(logging.KeyJobID, job.ID, logging.KeyScript, job.Script)
				log.Debug("Processing job")

				var result jobs.Result
				output, err := a.processJob(ctx, job)
				if err != nil {
					log.Warn("Failed to process job", logging.KeyError, err)
					result = jobs.Result{Success: false, Error: err.Error()}
				} else {
					result = jobs.Result{Success: true, Output: output}
				}

				if err := jobs.PostResult(ctx, job.ID, result); err != nil {
					log.Error("Failed to post result", logging.KeyError, err)
				}
			}
			wg.Done()
		}()
	}

	return
}

func (a *jobrunner) processJob(ctx context.Context, job jobs.Job) (map[string]string, error) {
	input, err := json.Marshal(job.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to encode input: %w", err)
	}

	execCtx, cancel := context.WithTimeout(ctx, a.scriptTimeout)
	defer cancel()

	raw, err := target.Execute(execCtx, job.Script, input)
	if err != nil {
		return nil, fmt.Errorf("failed to execute target: %w", err)
	}

	var output map[string]string
	if err := json.Unmarshal(raw, &output); err != nil {
		return nil, fmt.Errorf("failed to decode output: %w", err)
	}
	return output, nil
}
