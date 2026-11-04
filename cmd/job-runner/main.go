package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sp/job-runner/internal/app"
)

const (
	scriptTimeoutSeconds     = 1
	pollIntervalMilliseconds = 1
	maxJobs                  = 3
)

func main() {
	ctx := context.Background()

	app := app.NewApp(scriptTimeoutSeconds * time.Second)

	err := app.Run(ctx, maxJobs, pollIntervalMilliseconds*time.Millisecond)
	if err != nil {
		fmt.Printf("Error running Job Runner: %v", err)
	}
}
