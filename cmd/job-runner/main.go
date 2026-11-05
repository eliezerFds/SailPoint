package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sp/job-runner/internal/app"
	"github.com/sp/job-runner/internal/jobs"
	"github.com/sp/job-runner/internal/logging"
	"github.com/sp/job-runner/internal/target"
)

const (
	scriptTimeoutSeconds     = 5
	pollIntervalMilliseconds = 1
	maxJobs                  = 10
)

func main() {
	level, levelErr := logging.ParseLevel(os.Getenv("LOG_LEVEL"))
	logging.Setup(os.Stdout, level)
	if levelErr != nil {
		slog.Warn("Using default log level", logging.KeyError, levelErr)
	}

	// Context that will allow us to detect shutdown and safely shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := app.NewApp(jobsClient{}, targetService{}, scriptTimeoutSeconds*time.Second)

	err := app.Run(ctx, maxJobs, pollIntervalMilliseconds*time.Millisecond)
	if err != nil {
		slog.Error("Job Runner exited with error", logging.KeyError, err)
		os.Exit(1)
	}
}

// Mocks the jobs service to allow us to test
type jobsClient struct{}

func (jobsClient) GetJobs(ctx context.Context) ([]jobs.Job, error) {
	return jobs.GetJobs(ctx)
}

func (jobsClient) PostResult(ctx context.Context, jobID string, result jobs.Result) error {
	return jobs.PostResult(ctx, jobID, result)
}

// Mocks the target service to allow us to test
type targetService struct{}

func (targetService) Execute(ctx context.Context, name string, input []byte) ([]byte, error) {
	return target.Execute(ctx, name, input)
}
