package logging

import (
	"fmt"
	"io"
	"log/slog"
)

const (
	KeyComponent     = "component"
	KeyError         = "error"
	KeyJobCount      = "job_count"
	KeyJobID         = "job_id"
	KeyPollInterval  = "poll_interval"
	KeyRetryIn       = "retry_in"
	KeyScript        = "script"
	KeyScriptTimeout = "script_timeout"
	KeyWorkerID      = "worker_id"
	KeyWorkers       = "workers"
)

// Setup makes a JSON logger writing to w at the given level the default slog
// logger, so every package that calls slog uses it.
func Setup(w io.Writer, level slog.Level) {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
}

// ParseLevel accepts debug, info, warn or error.
func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		return slog.LevelInfo, nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo, fmt.Errorf("invalid log level %q: %w", s, err)
	}
	return level, nil
}
