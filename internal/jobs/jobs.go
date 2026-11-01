package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"

	"github.com/sp/job-runner/internal/faker"
)

var (
	errConn = errors.New("connection dropped")
)

// A Job represents a unit of work retrieved from the Jobs microservice.
type Job struct {
	ID     string            `json:"id"`
	Script string            `json:"script"`
	Input  map[string]string `json:"input"`
}

// A Result represents the result of a Job execution.
type Result struct {
	Success bool              `json:"success"`
	Error   string            `json:"error"`
	Output  map[string]string `json:"output"`
}

// GetJobs simulates a request for Jobs.
// Dummy Job data is generated using faker.
//
// The real implementation would send a request to the Jobs microservice.
//
// The provided context is used to interrupt the request if the context
// becomes done before the request completes.
func GetJobs(ctx context.Context) (jobs []Job, err error) {
	slog.Info("Retrieving jobs")

	err = faker.RemoteCall()
	if err != nil {
		return jobs, fmt.Errorf("get jobs: %w", errConn)
	}

	for i := 0; i < rand.Intn(20); i++ {
		jobs = append(jobs, Job{ID: faker.JobID(), Script: faker.JobName(), Input: faker.JobInput()})
	}

	select {
	case <-ctx.Done():
		return jobs, fmt.Errorf("get jobs: %w", ctx.Err())
	default:
		return jobs, nil
	}
}

// PostResult simulates posting a Job Result.
//
// The real implementation would post results to the Jobs microservice.
//
// The provided context is used to interrupt the request if the context
// becomes done before the request completes.
func PostResult(ctx context.Context, jobID string, result Result) error {
	slog.Info("Posting result for job", "id", jobID)

	err := faker.RemoteCall()
	if err != nil {
		return fmt.Errorf("post result: %w", errConn)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
