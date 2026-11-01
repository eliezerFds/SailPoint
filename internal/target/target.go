package target

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/sp/job-runner/internal/faker"
)

var (
	errInvalidScript = errors.New("invalid script")
)

// Execute simulates running a named script against a remote target.
//
// The provided context is used to interrupt the execution if the context
// becomes done before the script completes.
func Execute(ctx context.Context, name string, input []byte) (output []byte, err error) {
	slog.Info("Executing script", "name", name)

	err = faker.RemoteCall()
	if err != nil {
		return nil, fmt.Errorf("execution: %w", errInvalidScript)
	}

	output, err = json.Marshal(faker.JobOutput())
	if err != nil {
		return nil, fmt.Errorf("execution output: %w", err)
	}

	select {
	case <-ctx.Done():
		return output, fmt.Errorf("execute script: %w", ctx.Err())
	default:
		return output, nil
	}
}
