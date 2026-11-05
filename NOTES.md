# Notes

## Running it

From the exercise folder:

```
(cd ./go && go run ./cmd/job-runner/main.go)
```

- `LOG_LEVEL=debug` shows per job logs. Default is info.
- Ctrl+C (or SIGTERM) shuts it down cleanly.
- Tests: `go test -race -timeout 30s ./...`

Needs Go 1.25+ (`wg.Go`, ranging over an int). The go.mod is already on 1.27 so this shouldn't be a problem.

## How it works

- One poller goroutine calls `GetJobs` and pushes each job onto a channel buffered to `maxJobs`. If the workers are all busy the poller blocks, so it never pulls more work than it can run.
- 10 workers read from that channel. Each one encodes the input, runs the script with a 5s timeout, decodes the output and posts the result. Failed scripts get posted as `Success: false` so the jobs service still hears about them.
- I went with a worker pool over batching 10 at a time. With batches every round waits for the slowest job and the other workers sit idle.
- `app` depends on two small interfaces, `JobsClient` and `Target`. `main` passes in thin adapters around the provided packages and the tests pass in fakes. The adapters live in main.go because the README run command only compiles that one file.
- Logging is `slog` with a JSON handler, set up once in `internal/logging`. Key names are constants in that package so they're the same everywhere. Workers attach `worker_id`, `job_id` and `script` with `slog.With`.

## Error handling

- Failed `GetJobs` calls back off: the wait doubles each time up to 30s, then resets after a good poll.
- `PostResult` gets 3 attempts (200ms then 400ms between them) before I log it as lost.
- Neither of these stops the runner. A bad job or a bad call gets logged and it moves on.

## Shutdown

On Ctrl+C the poller stops and closes the channel. Workers finish whatever they're on plus anything still buffered, and still post results. They run on `context.WithoutCancel(ctx)` so the signal doesn't kill jobs half way through.

The provided `GetJobs` can hand back a batch together with a `context canceled` error. I still process those jobs, since the service has already given them to us.

## Trade-offs / things I know about

- A post retry can send the same result twice if the first one landed but the response got lost. The service would need to dedupe on job ID.
- No overall shutdown deadline and a second Ctrl+C doesn't force quit. Worst case is still bounded by the script timeout.

## What I'd do next

- Tests for the gaps above, starting with the concurrency limit.
- Shutdown deadline plus force quit on a second signal.
- Put the logger in the context so every log line, including the provided packages, carries the job ID.
- Metrics for jobs processed, failed, timed out and posts lost.
