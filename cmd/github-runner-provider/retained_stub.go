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
	LookupEnv   func(string) (string, bool)
	ReadConfig  func(string, string) (retainedprovider.Config, error)
	Install     func(context.Context, string, retainedprovider.Config, retainedprovider.Credentials) (retainedprovider.Status, error)
	Refresh     func(context.Context, retainedprovider.Config) (retainedprovider.Status, error)
	ServeActive func(context.Context, retainedprovider.Config) error
	Status      func(context.Context, string, retainedprovider.Config) (retainedprovider.Status, error)
	Uninstall   func(context.Context, string, retainedprovider.Config, bool) (retainedprovider.Status, error)
	Recover     func(context.Context, string, retainedprovider.Config, string) (retainedprovider.Status, error)
}

func runRetainedProviderCommand(ctx context.Context, logger *slog.Logger, args []string, stdout io.Writer) error {
	runner := retainedprovider.OSCommandRunner{}
	refresher := retainedprovider.Refresher{Runner: runner, ExecutablePath: os.Executable, Now: func() time.Time { return time.Now().UTC() }}
	installer := retainedprovider.Installer{Runner: runner, ExecutablePath: os.Executable, Now: func() time.Time { return time.Now().UTC() }}
	return runRetainedProviderCommandWithDependencies(ctx, logger, args, stdout, retainedProviderCommandDependencies{
		GOOS:        runtime.GOOS,
		HomeDir:     os.UserHomeDir,
		LookupEnv:   os.LookupEnv,
		ReadConfig:  retainedprovider.ReadConfigFile,
		Install:     installer.Install,
		Refresh:     refresher.Refresh,
		ServeActive: refresher.ServeActive,
		Status:      installer.Status,
		Uninstall:   installer.Uninstall,
		Recover:     installer.Recover,
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
	case "install", "refresh", "serve-active", "status", "uninstall", "recover":
	default:
		return errors.New("unknown retained provider subcommand")
	}
	flags := flag.NewFlagSet("github-runner-provider retained "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "absolute retained provider config path")
	var purge *bool
	var confirmation *string
	if args[0] == "uninstall" {
		purge = flags.Bool("purge", false, "remove retained provider state and credentials")
	}
	if args[0] == "recover" {
		confirmation = flags.String("confirm", "", "exact legacy provider transaction id")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("retained provider command does not accept positional arguments")
	}
	if strings.TrimSpace(*configPath) == "" {
		return errors.New("-config is required")
	}
	if args[0] == "recover" && strings.TrimSpace(*confirmation) == "" {
		return errors.New("-confirm is required for retained provider recovery")
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
	var status retainedprovider.Status
	switch args[0] {
	case "install":
		githubToken, githubFound := dependencies.LookupEnv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN")
		providerToken, providerFound := dependencies.LookupEnv("GITHUB_RUNNER_PROVIDER_TOKEN")
		if !githubFound || !providerFound || strings.TrimSpace(githubToken) == "" || strings.TrimSpace(providerToken) == "" {
			return errors.New("provider and GitHub credentials are required in the retained install environment")
		}
		status, err = dependencies.Install(ctx, home, config, retainedprovider.Credentials{GitHubToken: githubToken, ProviderToken: providerToken})
	case "refresh":
		status, err = dependencies.Refresh(ctx, config)
	case "status":
		status, err = dependencies.Status(ctx, home, config)
	case "uninstall":
		status, err = dependencies.Uninstall(ctx, home, config, *purge)
	case "recover":
		status, err = dependencies.Recover(ctx, home, config, *confirmation)
	}
	if err != nil {
		return err
	}
	return retainedprovider.WriteStatus(stdout, status)
}
