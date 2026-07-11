//go:build linux

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

func TestT916RunGitHubJobReapsDetachedProcessesBeforeArtifactCollection(t *testing.T) {
	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is required")
	}
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })
	workspace := t.TempDir()
	readyPath := filepath.Join(workspace, "detached.ready")
	helperLogPath := filepath.Join(workspace, "detached.log")

	var jobsCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 52)
		case writeAcceptedRunnerJITACK(t, w, r):
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000003":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":28460000003,"path":".github/workflows/dogfood.yml@refs/heads/main","head_branch":"main","head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000003/jobs":
			jobsCalls++
			w.WriteHeader(http.StatusOK)
			if jobsCalls == 1 {
				_, _ = w.Write([]byte(`{"jobs":[{"id":84330000003,"run_id":28460000003,"status":"queued","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
				return
			}
			if _, err := os.Stat(readyPath); err != nil {
				_, _ = w.Write([]byte(`{"jobs":[{"id":84330000003,"run_id":28460000003,"status":"in_progress","runner_id":52,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4"}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"jobs":[{"id":84330000003,"run_id":28460000003,"status":"completed","conclusion":"success","runner_id":52,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4"}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	artifactDir := filepath.Join(runnerDir, "_work", "workflow-compute", "workflow-compute", "build")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	artifactPath := filepath.Join(artifactDir, "result.txt")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nprintf 'before-cleanup\\n' > \"$DETACHED_ARTIFACT\"\n\"$DETACHED_SETSID\" \"$DETACHED_HELPER_BINARY\" -test.run=^TestT916DetachedArtifactWriterHelper$ >/dev/null 2>&1 &\nwhile [ ! -f \"$DETACHED_READY\" ]; do sleep 0.01; done\n")
	writeFunctionalRunnerListener(t, runnerDir)
	t.Setenv("DETACHED_SETSID", setsid)
	t.Setenv("DETACHED_ARTIFACT", artifactPath)
	t.Setenv("DETACHED_READY", readyPath)
	t.Setenv("DETACHED_HELPER_LOG", helperLogPath)
	t.Setenv("DETACHED_HELPER_BINARY", os.Args[0])
	t.Setenv("GO_WANT_DETACHED_ARTIFACT_HELPER", "1")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	setRunnerProcessEnvironmentExtras(t, "DETACHED_SETSID", "DETACHED_ARTIFACT", "DETACHED_READY", "DETACHED_HELPER_LOG", "DETACHED_HELPER_BINARY", "GO_WANT_DETACHED_ARTIFACT_HELPER")
	t.Chdir(workspace)

	req := internal.EphemeralRunnerJobRequest{
		Mode:             internal.EphemeralRunnerJobModeAttachToQueued,
		Environment:      "stg",
		OS:               "linux",
		WorkerID:         "worker-0123456789abcdef",
		TaskID:           "task-abcdef9876543210",
		Organization:     "GoCodeAlone",
		Repository:       "GoCodeAlone/workflow-compute",
		Workflow:         "dogfood.yml",
		Ref:              "main",
		WorkflowRunID:    28460000003,
		WorkflowJobID:    84330000003,
		ArtifactPaths:    []string{"build/result.txt"},
		RunnerGroup:      "ephemeral",
		RequirePreflight: true,
	}
	spec, err := internal.BuildEphemeralRunnerJobSpec(req)
	if err != nil {
		t.Fatalf("build runner spec: %v", err)
	}
	driver, err := newRunnerDriver(req, &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()})
	if err != nil {
		t.Fatalf("create production runner driver: %v", err)
	}
	result, err := driver.RunGitHubJob(t.Context(), internal.EphemeralRunnerJobModeAttachToQueued, spec)
	if err != nil {
		t.Fatalf("run attached GitHub job: %v", err)
	}
	if len(result.WorkloadArtifacts) != 1 || result.WorkloadArtifacts[0] != workloadArtifactArchiveName {
		t.Fatalf("workload artifacts = %#v", result.WorkloadArtifacts)
	}
	archive := readTarGzFiles(t, filepath.Join(workspace, workloadArtifactArchiveName))
	if got := string(archive["build/result.txt"]); got != "detached-stopped\n" {
		pidData, readErr := os.ReadFile(readyPath)
		detachedPID, parseErr := strconv.Atoi(strings.TrimSpace(string(pidData)))
		process, findErr := os.FindProcess(detachedPID)
		running := readErr == nil && parseErr == nil && findErr == nil && processIsRunning(process, detachedPID)
		helperLog, logErr := os.ReadFile(helperLogPath)
		t.Fatalf("artifact was collected before detached process cleanup: %q pid=%d running=%t read_err=%v parse_err=%v find_err=%v helper_log=%q log_err=%v", got, detachedPID, running, readErr, parseErr, findErr, helperLog, logErr)
	}
}

func TestT916DetachedArtifactWriterHelper(t *testing.T) {
	if os.Getenv("GO_WANT_DETACHED_ARTIFACT_HELPER") != "1" {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	logPath := os.Getenv("DETACHED_HELPER_LOG")
	if err := os.WriteFile(logPath, []byte("started\n"), 0o600); err != nil {
		t.Fatalf("write detached helper log: %v", err)
	}
	if err := os.WriteFile(os.Getenv("DETACHED_READY"), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatalf("write detached helper ready marker: %v", err)
	}
	select {
	case <-signals:
		file, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = file.WriteString("sigterm\n")
			_ = file.Close()
		}
	case <-time.After(10 * time.Second):
		t.Fatal("detached helper did not receive SIGTERM")
	}
	if err := os.WriteFile(os.Getenv("DETACHED_ARTIFACT"), []byte("detached-stopped\n"), 0o600); err != nil {
		t.Fatalf("write detached helper artifact: %v", err)
	}
}

func TestT916DetachedRunnerSessionIsReapedBeforeArtifactCollection(t *testing.T) {
	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is required")
	}
	if err := prepareRunnerProcessIsolation(); err != nil {
		t.Fatalf("prepare process isolation: %v", err)
	}
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "detached.pid")
	script := filepath.Join(dir, "runner.sh")
	writeExecutable(t, script, "#!/bin/sh\n\"$DETACHED_SETSID\" sh -c 'echo $$ > \"$DETACHED_PID_FILE\"; exec sleep 60' >/dev/null 2>&1 &\nexit 0\n")
	t.Setenv("DETACHED_SETSID", setsid)
	t.Setenv("DETACHED_PID_FILE", pidFile)
	setRunnerProcessEnvironmentExtras(t, "DETACHED_SETSID", "DETACHED_PID_FILE")
	running, err := startCommand(context.Background(), script, dir)
	if err != nil {
		t.Fatalf("start runner: %v", err)
	}
	if err := running.wait(); err != nil {
		t.Fatalf("wait runner: %v", err)
	}
	var detachedPID int
	var lastPIDError error
	for range 100 {
		data, readErr := os.ReadFile(pidFile)
		if readErr != nil {
			lastPIDError = readErr
			time.Sleep(10 * time.Millisecond)
			continue
		}
		detachedPID, err = strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || detachedPID <= 0 {
			lastPIDError = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}
	if detachedPID <= 0 {
		t.Fatalf("detached runner process did not start: %v", lastPIDError)
	}
	process, err := os.FindProcess(detachedPID)
	if err != nil {
		t.Fatalf("find detached process: %v", err)
	}
	t.Cleanup(func() { _ = process.Kill() })
	if err := cleanupDetachedRunnerProcesses(); err != nil {
		t.Fatalf("cleanup detached runner processes: %v", err)
	}
	if processIsRunning(process, detachedPID) {
		t.Fatalf("detached runner process %d survived cleanup", detachedPID)
	}
}
