package internal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

type githubCLI struct {
	stdout io.Writer
	stderr io.Writer
	getenv func(string) string
}

var _ sdk.CLIProvider = (*githubCLI)(nil)

func NewCLI() sdk.CLIProvider { return newCLI(os.Stdout, os.Stderr) }

func newCLI(stdout, stderr io.Writer) *githubCLI {
	return &githubCLI{stdout: stdout, stderr: stderr, getenv: os.Getenv}
}

func (c *githubCLI) RunCLI(args []string) int {
	if err := c.run(context.Background(), args); err != nil {
		_, _ = fmt.Fprintln(c.stderr, err)
		return 1
	}
	return 0
}

func (c *githubCLI) help() error {
	_, err := fmt.Fprintln(c.stdout, "Usage: wfctl github runner-policy reconcile <plan|apply|rollback> [--file policy.json] [--transaction id] [--state-dir directory] [--api-url URL]\n\nplan captures readback and a durable transaction; apply and rollback resume its exact journal. GITHUB_TOKEN is read only from the operator environment.")
	return err
}

func (c *githubCLI) run(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "github" {
		return errors.New("usage: wfctl github runner-policy reconcile <plan|apply|rollback>")
	}
	if len(args) == 1 {
		return c.help()
	}
	for _, prefix := range []int{1, 2, 3} {
		if len(args) == prefix+1 && (args[prefix] == "--help" || args[prefix] == "-h") {
			if prefix >= 2 && args[1] != "runner-policy" || prefix >= 3 && args[2] != "reconcile" {
				return errors.New("unknown github command")
			}
			return c.help()
		}
	}
	if len(args) < 4 || args[1] != "runner-policy" || args[2] != "reconcile" {
		return errors.New("unknown github command; expected runner-policy reconcile")
	}
	mode := args[3]
	if mode != "plan" && mode != "apply" && mode != "rollback" {
		return errors.New("unknown reconcile mode; expected plan, apply, or rollback")
	}
	fs := flag.NewFlagSet("github runner-policy reconcile", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filePath := fs.String("file", "", "strict desired-state JSON")
	tx := fs.String("transaction", "", "durable exact transaction ID")
	stateDir := fs.String("state-dir", "", "operator state directory")
	apiURL := fs.String("api-url", defaultGitHubAPIBaseURL, "GitHub API base URL")
	if err := fs.Parse(args[4:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return c.help()
		}
		return errors.New("invalid runner-policy flags; credentials are accepted only through GITHUB_TOKEN")
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected runner-policy positional arguments")
	}
	if mode == "rollback" && (*tx == "" || *filePath != "") {
		return errors.New("rollback requires --transaction and rejects --file")
	}
	if mode == "plan" && *filePath == "" || mode == "apply" && *filePath == "" && *tx == "" {
		return errors.New("plan requires --file; apply requires --file or --transaction")
	}
	var desired RunnerPolicyDesired
	if *filePath != "" {
		file, err := os.Open(*filePath)
		if err != nil {
			return errors.New("policy file could not be opened")
		}
		desired, err = decodeRunnerPolicy(file)
		_ = file.Close()
		if err != nil {
			return err
		}
	}
	if *tx == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return errors.New("transaction ID could not be generated")
		}
		*tx = hex.EncodeToString(random[:])
	}
	if *stateDir == "" {
		base := c.getenv("XDG_STATE_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return errors.New("operator state directory could not be resolved")
			}
			base = filepath.Join(home, ".local", "state")
		}
		*stateDir = filepath.Join(base, "wfctl", "plugins", "github")
	}
	client, err := newRunnerPolicyClient(*apiURL, c.getenv("GITHUB_TOKEN"))
	if err != nil {
		return err
	}
	reconciler := newRunnerPolicyReconciler(client, *stateDir)
	var result RunnerPolicyResult
	if *filePath != "" {
		result, err = reconciler.Plan(ctx, *tx, desired)
		if err != nil {
			return err
		}
	}
	if mode == "apply" {
		result, err = reconciler.Apply(ctx, *tx)
	}
	if mode == "rollback" {
		result, err = reconciler.Rollback(ctx, *tx)
	}
	if err != nil {
		return err
	}
	if err := json.NewEncoder(c.stdout).Encode(result); err != nil {
		return errors.New("policy result could not be written")
	}
	return nil
}
