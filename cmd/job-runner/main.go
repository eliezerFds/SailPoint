package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/sp/job-runner/internal/app"
	"github.com/sp/job-runner/internal/logging"
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

	ctx := context.Background()

	app := app.NewApp(scriptTimeoutSeconds * time.Second)

	err := app.Run(ctx, maxJobs, pollIntervalMilliseconds*time.Millisecond)
	if err != nil {
		slog.Error("Job Runner exited with error", logging.KeyError, err)
		os.Exit(1)
	}
}
