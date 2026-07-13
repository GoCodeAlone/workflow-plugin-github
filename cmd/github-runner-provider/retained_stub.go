package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal/retainedprovider"
)

type retainedProviderCommandDependencies struct {
	GOOS        string
	HomeDir     func() (string, error)
	ReadConfig  func(string, string) (retainedprovider.Config, error)
	Refresh     func(context.Context, retainedprovider.Config) (retainedprovider.Status, error)
	ServeActive func(context.Context, retainedprovider.Config) error
}

func runRetainedProviderCommand(ctx context.Context, logger *slog.Logger, args []string, stdout io.Writer) error {
	runner := retainedprovider.OSCommandRunner{}
	refresher := retainedprovider.Refresher{Runner: runner, ExecutablePath: os.Executable, Now: func() time.Time { return time.Now().UTC() }}
	return runRetainedProviderCommandWithDependencies(ctx, logger, args, stdout, retainedProviderCommandDependencies{
		GOOS:        runtime.GOOS,
		HomeDir:     os.UserHomeDir,
		ReadConfig:  retainedprovider.ReadConfigFile,
		Refresh:     refresher.Refresh,
		ServeActive: refresher.ServeActive,
	})
}

func runRetainedProviderCommandWithDependencies(ctx context.Context, _ *slog.Logger, args []string, stdout io.Writer, dependencies retainedProviderCommandDependencies) error {
	if dependencies.GOOS != "linux" {
		return fmt.Errorf("retained provider lifecycle is unsupported on %s", dependencies.GOOS)
	}
	if len(args) == 0 {
		return errors.New("retained provider subcommand is required")
	}
	switch args[0] {
	case "refresh", "serve-active":
	default:
		return fmt.Errorf("unknown retained provider subcommand %q", args[0])
	}
	flags := flag.NewFlagSet("github-runner-provider retained "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "absolute retained provider config path")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("retained provider command does not accept positional arguments")
	}
	if strings.TrimSpace(*configPath) == "" {
		return errors.New("-config is required")
	}
	home, err := dependencies.HomeDir()
	if err != nil {
		return fmt.Errorf("resolve user home: %w", err)
	}
	config, err := dependencies.ReadConfig(*configPath, home)
	if err != nil {
		return err
	}
	if args[0] == "serve-active" {
		return dependencies.ServeActive(ctx, config)
	}
	status, err := dependencies.Refresh(ctx, config)
	if err != nil {
		return err
	}
	return retainedprovider.WriteStatus(stdout, status)
}
