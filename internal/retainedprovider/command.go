package retainedprovider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultCommandOutputBytes = 1 << 20
	controlCommandTimeout     = 30 * time.Second
	containerStartTimeout     = time.Minute
	providerProbeTimeout      = 2 * time.Minute
	providerBuildTimeout      = 10 * time.Minute
)

type Command struct {
	Path  string
	Args  []string
	Env   []string
	Dir   string
	Stdin []byte
}

type CommandRunner interface {
	Run(context.Context, Command) ([]byte, error)
	Exec(Command) error
}

type OSCommandRunner struct {
	MaxOutputBytes int
}

func runBoundedCommand(ctx context.Context, runner CommandRunner, command Command) ([]byte, error) {
	timeout := controlCommandTimeout
	if filepath.Base(command.Path) == "podman" && len(command.Args) > 0 {
		switch command.Args[0] {
		case "build":
			timeout = providerBuildTimeout
		case "run":
			timeout = containerStartTimeout
			for _, argument := range command.Args {
				if argument == "probe" {
					timeout = providerProbeTimeout
					break
				}
			}
		}
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return runner.Run(bounded, command)
}

func (runner OSCommandRunner) Run(ctx context.Context, command Command) ([]byte, error) {
	if err := validateCommand(command); err != nil {
		return nil, err
	}
	limit := runner.MaxOutputBytes
	if limit <= 0 {
		limit = defaultCommandOutputBytes
	}
	stdout := &boundedCommandBuffer{remaining: limit}
	process := exec.CommandContext(ctx, command.Path, command.Args...)
	process.Stdout = stdout
	process.Stderr = io.Discard
	process.Dir = command.Dir
	process.Env = commandEnvironment(command.Env)
	if command.Stdin != nil {
		process.Stdin = bytes.NewReader(command.Stdin)
	}
	if err := process.Run(); err != nil {
		return nil, fmt.Errorf("command %q failed: %w", filepath.Base(command.Path), redactCommandError(err))
	}
	if stdout.exceeded {
		return nil, fmt.Errorf("command %q output exceeds %d bytes", filepath.Base(command.Path), limit)
	}
	return stdout.Bytes(), nil
}

func (runner OSCommandRunner) Exec(command Command) error {
	if err := validateCommand(command); err != nil {
		return err
	}
	if command.Dir != "" || command.Stdin != nil {
		return errors.New("foreground exec does not support a working directory or stdin")
	}
	environment := commandEnvironment(command.Env)
	return replaceProcess(command.Path, append([]string{command.Path}, command.Args...), environment)
}

func commandEnvironment(explicit []string) []string {
	if explicit != nil {
		return append([]string(nil), explicit...)
	}
	allowed := []string{
		"HOME", "USER", "LOGNAME", "PATH", "SHELL", "TMPDIR",
		"LANG", "LC_ALL", "LC_CTYPE",
		"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR",
		"DBUS_SESSION_BUS_ADDRESS",
		"CONTAINERS_CONF", "CONTAINERS_STORAGE_CONF", "CONTAINERS_REGISTRIES_CONF",
	}
	environment := make([]string, 0, len(allowed))
	for _, key := range allowed {
		if value, exists := os.LookupEnv(key); exists && !strings.ContainsAny(value, "\r\n\x00") {
			environment = append(environment, key+"="+value)
		}
	}
	return environment
}

func validateCommand(command Command) error {
	if !filepath.IsAbs(command.Path) || containsControl(command.Path) {
		return errors.New("command path must be absolute and safe")
	}
	for _, value := range command.Args {
		if strings.ContainsRune(value, 0) {
			return errors.New("command argument contains NUL")
		}
	}
	for _, value := range command.Env {
		if strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("command environment contains unsupported characters")
		}
	}
	return nil
}

func redactCommandError(err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return fmt.Errorf("exit status %d", exitErr.ExitCode())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.New("process could not be started")
}

type boundedCommandBuffer struct {
	bytes.Buffer
	remaining int
	exceeded  bool
}

func (buffer *boundedCommandBuffer) Write(data []byte) (int, error) {
	count := len(data)
	if len(data) > buffer.remaining {
		data = data[:buffer.remaining]
		buffer.exceeded = true
	}
	buffer.remaining -= len(data)
	_, _ = buffer.Buffer.Write(data)
	return count, nil
}
