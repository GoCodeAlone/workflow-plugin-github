package internal

import (
	"bytes"
	"strings"
	"testing"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

func TestGitHubCLIHelpAndStrictDispatch(t *testing.T) {
	for _, args := range [][]string{{"github", "--help"}, {"github", "runner-policy", "--help"}, {"github", "runner-policy", "reconcile", "--help"}} {
		var out, errs bytes.Buffer
		cli := newCLI(&out, &errs)
		var provider sdk.CLIProvider = cli
		if code := sdk.DispatchArgs(append([]string{"github", "--wfctl-cli"}, args...), NewGitHubPlugin(), provider, nil, nil, &out); code != 0 {
			t.Fatalf("CLI help failed: %d: %s", code, errs.String())
		}
		if !strings.Contains(out.String(), "runner-policy") || errs.Len() != 0 {
			t.Fatalf("unexpected help: %q / %q", out.String(), errs.String())
		}
	}
	for _, args := range [][]string{nil, {"other"}, {"github", "unknown"}, {"github", "runner-policy", "reconcile", "unknown"}, {"github", "runner-policy", "reconcile", "plan", "--token", "known-private-value"}} {
		var out, errs bytes.Buffer
		if code := newCLI(&out, &errs).RunCLI(args); code == 0 || strings.Contains(out.String()+errs.String(), "known-private-value") {
			t.Fatalf("unsafe dispatch accepted/disclosed: %d: %q / %q", code, out.String(), errs.String())
		}
	}
}
