package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "github-actions-runner-job failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	specOnly := flag.Bool("spec", false, "emit the deterministic runner spec without starting a runner")
	flag.Parse()
	if !*specOnly {
		return fmt.Errorf("runner execution is not available without a host-provided execution driver; use --spec only for preflight/spec generation")
	}

	var req internal.EphemeralRunnerJobRequest
	decoder := json.NewDecoder(os.Stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	spec, err := internal.BuildEphemeralRunnerJobSpec(req)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"runner_name":  spec.RunnerName,
		"labels":       spec.Labels,
		"runner_group": spec.RunnerGroup,
	})
}
