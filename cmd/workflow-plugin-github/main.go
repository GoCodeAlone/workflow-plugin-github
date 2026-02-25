// Command workflow-plugin-github is a workflow engine external plugin that
// provides GitHub integration: webhook handling and GitHub Actions workflow
// management. It runs as a subprocess and communicates with the host workflow
// engine via the go-plugin protocol.
package main

import (
	"github.com/GoCodeAlone/workflow-plugin-github/internal"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func main() {
	sdk.Serve(internal.NewGitHubPlugin())
}
