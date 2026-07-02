package internal

import (
	"testing"

	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
)

func TestContractRegistry_NonNil(t *testing.T) {
	p := &githubPlugin{}
	reg := p.ContractRegistry()
	if reg == nil {
		t.Fatal("ContractRegistry() returned nil")
	}
	if reg.FileDescriptorSet == nil {
		t.Fatal("ContractRegistry().FileDescriptorSet is nil")
	}
	if len(reg.FileDescriptorSet.File) == 0 {
		t.Fatal("ContractRegistry().FileDescriptorSet.File is empty")
	}
	if len(reg.Contracts) == 0 {
		t.Fatal("ContractRegistry().Contracts is empty")
	}
}

func TestContractRegistry_CoversAllModuleTypes(t *testing.T) {
	p := &githubPlugin{}
	reg := p.ContractRegistry()

	wantModules := []string{"git.webhook", "github.app", "github.runner_provider"}
	found := map[string]bool{}
	for _, c := range reg.Contracts {
		if c.Kind == pb.ContractKind_CONTRACT_KIND_MODULE {
			found[c.ModuleType] = true
			if c.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
				t.Errorf("module %s contract mode = %v, want STRICT_PROTO", c.ModuleType, c.Mode)
			}
			if c.ConfigMessage == "" {
				t.Errorf("module %s contract has empty ConfigMessage", c.ModuleType)
			}
		}
	}
	for _, want := range wantModules {
		if !found[want] {
			t.Errorf("no contract descriptor found for module type %s", want)
		}
	}
}

func TestContractRegistry_CoversAllStepTypes(t *testing.T) {
	p := &githubPlugin{}
	reg := p.ContractRegistry()

	wantSteps := []string{
		"step.gh_action_trigger",
		"step.gh_action_status",
		"step.gh_pr_create",
		"step.gh_pr_merge",
		"step.gh_pr_comment",
		"step.gh_pr_review",
		"step.gh_issue_create",
		"step.gh_issue_close",
		"step.gh_issue_label",
		"step.gh_release_create",
		"step.gh_release_upload",
		"step.gh_upstream_release_monitor",
		"step.gh_repo_dispatch",
		"step.gh_deployment_create",
		"step.gh_secret_set",
		"step.gh_graphql",
	}

	found := map[string]bool{}
	for _, c := range reg.Contracts {
		if c.Kind == pb.ContractKind_CONTRACT_KIND_STEP {
			found[c.StepType] = true
			if c.Mode != pb.ContractMode_CONTRACT_MODE_STRICT_PROTO {
				t.Errorf("step %s contract mode = %v, want STRICT_PROTO", c.StepType, c.Mode)
			}
			if c.ConfigMessage == "" {
				t.Errorf("step %s contract has empty ConfigMessage", c.StepType)
			}
			if c.InputMessage == "" {
				t.Errorf("step %s contract has empty InputMessage", c.StepType)
			}
			if c.OutputMessage == "" {
				t.Errorf("step %s contract has empty OutputMessage", c.StepType)
			}
		}
	}
	for _, want := range wantSteps {
		if !found[want] {
			t.Errorf("no contract descriptor found for step type %s", want)
		}
	}
}

func TestT916_GitHubPluginDoesNotExposeComputeGatewayOrSyntheticCheckSteps(t *testing.T) {
	p := &githubPlugin{}
	for _, stepType := range p.StepTypes() {
		switch stepType {
		case "step.gh_compute_gateway", "step.gh_create_check":
			t.Fatalf("github plugin must not expose %s; compute submission/check ownership belongs outside this plugin", stepType)
		}
	}

	reg := p.ContractRegistry()
	for _, contract := range reg.Contracts {
		switch contract.StepType {
		case "step.gh_compute_gateway", "step.gh_create_check":
			t.Fatalf("github plugin contract registry must not expose %s", contract.StepType)
		}
	}
}

func TestContractRegistry_ContractCount(t *testing.T) {
	p := &githubPlugin{}
	reg := p.ContractRegistry()
	// 3 modules + 16 steps = 19 total
	if len(reg.Contracts) != 19 {
		t.Errorf("expected 19 contracts (3 modules + 16 steps), got %d", len(reg.Contracts))
	}
}
