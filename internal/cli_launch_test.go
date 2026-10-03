package internal

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGitHubCLIPublishedWFCTLGlobalLaunch(t *testing.T) {
	wfctl, archivePath := os.Getenv("WFCTL_TEST_BINARY"), os.Getenv("GITHUB_TEST_ARCHIVE")
	if wfctl == "" || archivePath == "" {
		t.Skip("set WFCTL_TEST_BINARY and GITHUB_TEST_ARCHIVE for published-host launch proof")
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		t.Fatal(err)
	}
	tarReader := tar.NewReader(reader)
	entries := make(map[string]bool)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries[header.Name] = true
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !entries["github"] || !entries["plugin.json"] {
		t.Fatal("single-entry archive is not normalized")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/github.tar.gz" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(archive)
	}))
	defer server.Close()
	fake := newPolicyAPIFake(t)
	dir := t.TempDir()
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "XDG_STATE_HOME=" + filepath.Join(dir, "state"), "XDG_DATA_HOME=" + filepath.Join(dir, "data"), "WFCTL_GLOBAL_PLUGIN_DIR=" + filepath.Join(dir, "global"), "WFCTL_PLUGIN_DIR=" + filepath.Join(dir, "project-plugins"), "GITHUB_TOKEN=known-private-value", "WFCTL_PLUGIN_INSTALL_QUIET=1", "WFCTL_NO_UPDATE_CHECK=1"}
	run := func(args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, wfctl, args...)
		cmd.Dir, cmd.Env = dir, env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("published wfctl %s failed: %v\n%s", strings.Join(args, " "), err, out)
		}
		if bytes.Contains(out, []byte("known-private-value")) {
			t.Fatal("launch output disclosed credential")
		}
		t.Logf("wfctl %s\n%s", strings.Join(args, " "), out)
		return out
	}
	if strings.TrimSpace(string(run("version"))) != "v0.86.0" {
		t.Fatal("launch proof requires the released wfctl v0.86.0")
	}
	digest := sha256.Sum256(archive)
	install := func() {
		run("plugin", "install", "-g", "--url", server.URL+"/github.tar.gz", "--sha256", hex.EncodeToString(digest[:]))
	}
	install()
	for _, path := range []string{"plugin.json", "github"} {
		if _, err := os.Stat(filepath.Join(dir, "global", "github", path)); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Contains(run("github", "--help"), []byte("runner-policy")) {
		t.Fatal("global CLI contribution was not launched")
	}
	data, err := json.Marshal(runnerPolicyFixture())
	if err != nil {
		t.Fatal(err)
	}
	policyPath := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(policyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	run("github", "runner-policy", "reconcile", "plan", "--file", policyPath, "--transaction", "launch-1", "--api-url", fake.server.URL)
	run("github", "runner-policy", "reconcile", "apply", "--transaction", "launch-1", "--api-url", fake.server.URL)
	fake.mu.Lock()
	writes := len(fake.writes)
	fake.mu.Unlock()
	run("github", "runner-policy", "reconcile", "apply", "--transaction", "launch-1", "--api-url", fake.server.URL)
	fake.mu.Lock()
	if len(fake.writes) != writes {
		fake.mu.Unlock()
		t.Fatal("cross-process identical retry mutated GitHub")
	}
	fake.mu.Unlock()
	run("github", "runner-policy", "reconcile", "rollback", "--transaction", "launch-1", "--api-url", fake.server.URL)
	fake.mu.Lock()
	if fake.variable != nil || len(fake.repoIDs) != 1 || fake.repoIDs[0] != 17 || !fake.group.RestrictedToWorkflows {
		fake.mu.Unlock()
		t.Fatal("launched rollback did not retain safe restrictions")
	}
	for _, write := range fake.writes {
		if strings.Contains(write, "/runners") || strings.Contains(write, "/runner-groups/1") || strings.Contains(write, "registration-token") {
			fake.mu.Unlock()
			t.Fatal("launch changed protected state")
		}
	}
	fake.mu.Unlock()
	audit, err := os.ReadFile(filepath.Join(dir, "state", "wfctl", "plugins", "github", runnerPolicyAuditFile))
	if err != nil || bytes.Contains(audit, []byte("known-private-value")) {
		t.Fatalf("default operator audit missing or unsafe: %v", err)
	}
	run("plugin", "remove", "-g", "github")
	if _, err := os.Stat(filepath.Join(dir, "global", "github")); !os.IsNotExist(err) {
		t.Fatal("global remove did not remove the installation")
	}
	install()
	run("github", "--help")
	t.Run("ConcurrentTransactions", func(t *testing.T) {
		testPublishedPolicyConcurrency(t, wfctl, dir, env, policyPath)
	})
}

func testPublishedPolicyConcurrency(t *testing.T, wfctl, dir string, env []string, policyPath string) {
	t.Helper()
	for _, operation := range []string{"plan", "apply", "rollback"} {
		t.Run(operation, func(t *testing.T) {
			arrived, release := make(chan struct{}, 2), make(chan struct{})
			var gate atomic.Bool
			fake := newPolicyAPIFake(t)
			fake.server.Close()
			fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if gate.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/actions/variables/WORKFLOW_COMPUTE_STAGING_URL") {
					select {
					case arrived <- struct{}{}:
					default:
					}
					<-release
				}
				fake.serve(w, r)
			}))
			t.Cleanup(fake.server.Close)
			var releaseOnce atomic.Bool
			unblock := func() {
				if releaseOnce.CompareAndSwap(false, true) {
					close(release)
				}
			}
			defer unblock()
			stateDir := t.TempDir()
			auditPath := filepath.Join(stateDir, runnerPolicyAuditFile)
			if err := os.WriteFile(auditPath, nil, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			run := func(phase string) ([]byte, error) {
				args := []string{"github", "runner-policy", "reconcile", phase, "--transaction", "concurrent", "--state-dir", stateDir, "--api-url", fake.server.URL}
				if phase == "plan" {
					args = append(args, "--file", policyPath)
				}
				cmd := exec.CommandContext(ctx, wfctl, args...)
				cmd.Dir, cmd.Env = dir, env
				return cmd.CombinedOutput()
			}
			check := func(out []byte, err error) {
				t.Helper()
				if err != nil || bytes.Contains(out, []byte("known-private-value")) {
					t.Fatalf("published wfctl transaction failed or disclosed credential: %v\n%s", err, out)
				}
				t.Logf("published wfctl: %s", out)
			}
			if operation != "plan" {
				check(run("plan"))
			}
			if operation == "rollback" {
				check(run("apply"))
			}
			gate.Store(true)
			type result struct {
				out []byte
				err error
			}
			results := make(chan result, 2)
			start := func() {
				go func() {
					out, err := run(operation)
					results <- result{out, err}
				}()
			}
			start()
			select {
			case <-arrived:
			case <-ctx.Done():
				t.Fatal("first published CLI did not reach readback barrier")
			}
			start()
			// An unlocked second process reaches the barrier too. A locked one waits
			// before loading authority, so release the first after a bounded wait.
			select {
			case <-arrived:
			case <-time.After(2 * time.Second):
			}
			unblock()
			for range 2 {
				r := <-results
				check(r.out, r.err)
			}
			gate.Store(false)
			audit, err := os.ReadFile(auditPath)
			if err != nil {
				t.Fatal(err)
			}
			if plans := bytes.Count(audit, []byte(`"phase":"planned"`)); plans != 1 {
				t.Errorf("concurrent published plans recorded %d initial authorities, want 1", plans)
			}
			if operation == "plan" {
				applyOut, applyErr := run("apply")
				rollbackOut, rollbackErr := run("rollback")
				t.Logf("post-plan apply: %v %s; rollback: %v %s", applyErr, applyOut, rollbackErr, rollbackOut)
				check(applyOut, applyErr)
				check(rollbackOut, rollbackErr)
			} else if operation != "rollback" {
				check(run("rollback"))
			}
			audit, err = os.ReadFile(auditPath)
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []string{"applied", "rolled-back-restrictive"} {
				if count := bytes.Count(audit, []byte(`"phase":"`+phase+`"`)); count != 1 {
					t.Fatalf("concurrent %s recorded %d terminal events, want 1", operation, count)
				}
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.writes) != 4 || fake.variable != nil || !fake.group.RestrictedToWorkflows || len(fake.repoIDs) != 1 || fake.repoIDs[0] != 17 {
				t.Fatalf("concurrent transaction repeated mutations or failed restrictive rollback: writes=%v", fake.writes)
			}
		})
	}
}
