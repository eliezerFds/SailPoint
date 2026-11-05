package app

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/sp/job-runner/internal/jobs"
)

func TestProcessJob_Success(t *testing.T) {
	exec := fakeExecutor{
		ExecuteFunc: func(ctx context.Context, name string, input []byte) ([]byte, error) {
			if got, want := string(input), `{"user":"alice"}`; got != want {
				t.Errorf("input = %s, want %s", got, want)
			}
			return []byte(`{"status":"success","user":"alice"}`), nil
		},
	}
	a := NewApp(nil, exec, time.Second)

	job := jobs.Job{ID: "1", Script: "create_user", Input: map[string]string{"user": "alice"}}
	output, err := a.processJob(context.Background(), job)
	if err != nil {
		t.Fatalf("processJob() error = %v, want nil", err)
	}

	want := map[string]string{"status": "success", "user": "alice"}
	if !maps.Equal(output, want) {
		t.Errorf("output = %v, want %v", output, want)
	}
}

func TestProcessJob_ExecuteError(t *testing.T) {
	errScript := errors.New("invalid script")
	exec := fakeExecutor{
		ExecuteFunc: func(ctx context.Context, name string, input []byte) ([]byte, error) {
			return nil, errScript
		},
	}
	a := NewApp(nil, exec, time.Second)

	_, err := a.processJob(context.Background(), jobs.Job{ID: "1", Script: "delete_user"})
	if !errors.Is(err, errScript) {
		t.Errorf("processJob() error = %v, want %v", err, errScript)
	}
}

func TestProcessJob_Timeout(t *testing.T) {
	exec := fakeExecutor{
		ExecuteFunc: func(ctx context.Context, name string, input []byte) ([]byte, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	a := NewApp(nil, exec, 10*time.Millisecond)

	_, err := a.processJob(context.Background(), jobs.Job{ID: "1", Script: "lock_user"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("processJob() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestRun_PostsResultForEachJob(t *testing.T) {
	client := newFakeJobsClient([]jobs.Job{
		{ID: "ok", Script: "create_user"},
		{ID: "bad", Script: "delete_user"},
	})
	exec := fakeExecutor{
		ExecuteFunc: func(ctx context.Context, name string, input []byte) ([]byte, error) {
			if name == "delete_user" {
				return nil, errors.New("invalid script")
			}
			return []byte(`{"status":"success"}`), nil
		},
	}
	a := NewApp(client, exec, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-client.done
		cancel()
	}()

	if err := a.Run(ctx, 2, time.Millisecond); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	if got := client.posted["ok"]; !got.Success || got.Output["status"] != "success" {
		t.Errorf(`result for "ok" = %+v, want Success true with status "success"`, got)
	}
	if got := client.posted["bad"]; got.Success || got.Error == "" {
		t.Errorf(`result for "bad" = %+v, want Success false with an error`, got)
	}
}
