package internal

import (
	githubv1 "github.com/GoCodeAlone/workflow-plugin-github/gen"
	pb "github.com/GoCodeAlone/workflow/plugin/external/proto"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// ContractRegistry returns the typed contract descriptors for all github module
// and step types. The workflow engine calls this via the sdk.ContractProvider
// interface to resolve proto message types for strict validation.
func (p *githubPlugin) ContractRegistry() *pb.ContractRegistry {
	return githubContractRegistry
}

// githubContractRegistry declares STRICT_PROTO contracts for all github module
// and step types. The FileDescriptorSet includes google.protobuf.Struct
// (used in free-form payload/data fields) so the engine can resolve all message
// types by full name.
var githubContractRegistry = &pb.ContractRegistry{
	FileDescriptorSet: &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(structpb.File_google_protobuf_struct_proto),
			protodesc.ToFileDescriptorProto(githubv1.File_github_proto),
		},
	},
	Contracts: []*pb.ContractDescriptor{
		// ── modules ──────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "git.webhook",
			ConfigMessage: githubProtoPkg + "WebhookModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "github.app",
			ConfigMessage: githubProtoPkg + "GitHubAppModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_MODULE,
			ModuleType:    "github.runner_provider",
			ConfigMessage: githubProtoPkg + "RunnerProviderModuleConfig",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		// ── steps ────────────────────────────────────────────────────────────
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_action_trigger",
			ConfigMessage: githubProtoPkg + "ActionTriggerConfig",
			InputMessage:  githubProtoPkg + "ActionTriggerInput",
			OutputMessage: githubProtoPkg + "ActionTriggerOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_action_status",
			ConfigMessage: githubProtoPkg + "ActionStatusConfig",
			InputMessage:  githubProtoPkg + "ActionStatusInput",
			OutputMessage: githubProtoPkg + "ActionStatusOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_pr_create",
			ConfigMessage: githubProtoPkg + "PRCreateConfig",
			InputMessage:  githubProtoPkg + "PRCreateInput",
			OutputMessage: githubProtoPkg + "PRCreateOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_pr_merge",
			ConfigMessage: githubProtoPkg + "PRMergeConfig",
			InputMessage:  githubProtoPkg + "PRMergeInput",
			OutputMessage: githubProtoPkg + "PRMergeOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_pr_comment",
			ConfigMessage: githubProtoPkg + "PRCommentConfig",
			InputMessage:  githubProtoPkg + "PRCommentInput",
			OutputMessage: githubProtoPkg + "PRCommentOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_pr_review",
			ConfigMessage: githubProtoPkg + "PRReviewConfig",
			InputMessage:  githubProtoPkg + "PRReviewInput",
			OutputMessage: githubProtoPkg + "PRReviewOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_issue_create",
			ConfigMessage: githubProtoPkg + "IssueCreateConfig",
			InputMessage:  githubProtoPkg + "IssueCreateInput",
			OutputMessage: githubProtoPkg + "IssueCreateOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_issue_close",
			ConfigMessage: githubProtoPkg + "IssueCloseConfig",
			InputMessage:  githubProtoPkg + "IssueCloseInput",
			OutputMessage: githubProtoPkg + "IssueCloseOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_issue_label",
			ConfigMessage: githubProtoPkg + "IssueLabelConfig",
			InputMessage:  githubProtoPkg + "IssueLabelInput",
			OutputMessage: githubProtoPkg + "IssueLabelOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_release_create",
			ConfigMessage: githubProtoPkg + "ReleaseCreateConfig",
			InputMessage:  githubProtoPkg + "ReleaseCreateInput",
			OutputMessage: githubProtoPkg + "ReleaseCreateOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_release_upload",
			ConfigMessage: githubProtoPkg + "ReleaseUploadConfig",
			InputMessage:  githubProtoPkg + "ReleaseUploadInput",
			OutputMessage: githubProtoPkg + "ReleaseUploadOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_upstream_release_monitor",
			ConfigMessage: githubProtoPkg + "UpstreamReleaseMonitorConfig",
			InputMessage:  githubProtoPkg + "UpstreamReleaseMonitorInput",
			OutputMessage: githubProtoPkg + "UpstreamReleaseMonitorOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_repo_dispatch",
			ConfigMessage: githubProtoPkg + "RepoDispatchConfig",
			InputMessage:  githubProtoPkg + "RepoDispatchInput",
			OutputMessage: githubProtoPkg + "RepoDispatchOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_deployment_create",
			ConfigMessage: githubProtoPkg + "DeploymentCreateConfig",
			InputMessage:  githubProtoPkg + "DeploymentCreateInput",
			OutputMessage: githubProtoPkg + "DeploymentCreateOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_secret_set",
			ConfigMessage: githubProtoPkg + "SecretSetConfig",
			InputMessage:  githubProtoPkg + "SecretSetInput",
			OutputMessage: githubProtoPkg + "SecretSetOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
		{
			Kind:          pb.ContractKind_CONTRACT_KIND_STEP,
			StepType:      "step.gh_graphql",
			ConfigMessage: githubProtoPkg + "GraphQLConfig",
			InputMessage:  githubProtoPkg + "GraphQLInput",
			OutputMessage: githubProtoPkg + "GraphQLOutput",
			Mode:          pb.ContractMode_CONTRACT_MODE_STRICT_PROTO,
		},
	},
}

// githubProtoPkg is the proto package prefix for all github typed messages.
const githubProtoPkg = "workflow.plugin.github.v1."

// Compile-time assertion: githubPlugin implements sdk.ContractProvider.
var _ sdk.ContractProvider = (*githubPlugin)(nil)
