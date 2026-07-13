package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
)

func runRetainedProviderCommand(context.Context, *slog.Logger, []string, io.Writer) error {
	return errors.New("retained provider lifecycle is not implemented")
}
