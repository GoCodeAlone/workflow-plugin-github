package retainedprovider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	ConfigProtocolVersion = "retained-provider.config.v1"
	GitHubPluginID        = "workflow-plugin-github"
	maxConfigBytes        = 1 << 20
)

var (
	safeIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	gitRefPattern         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	workflowPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$`)
)

// Config is the non-secret, versioned retained-provider installation contract.
type Config struct {
	ProtocolVersion        string   `json:"protocol_version"`
	WorkerID               string   `json:"worker_id"`
	ProfileID              string   `json:"profile_id"`
	PluginID               string   `json:"plugin_id"`
	ComponentID            string   `json:"component_id"`
	ComputeAgentPath       string   `json:"compute_agent_path"`
	SupervisorConfigPath   string   `json:"supervisor_config_path"`
	LocalStatusPath        string   `json:"local_status_path"`
	InstallRoot            string   `json:"install_root"`
	SystemdDir             string   `json:"systemd_dir"`
	AgentUnit              string   `json:"agent_unit"`
	PodmanPath             string   `json:"podman_path"`
	ProviderURL            string   `json:"provider_url"`
	StableContainer        string   `json:"stable_container"`
	CandidateContainer     string   `json:"candidate_container"`
	ContainerNetwork       string   `json:"container_network"`
	Organization           string   `json:"organization"`
	Repository             string   `json:"repository"`
	Workflow               string   `json:"workflow"`
	Ref                    string   `json:"ref"`
	RunnerName             string   `json:"runner_name"`
	RunnerGroup            string   `json:"runner_group"`
	Labels                 []string `json:"labels"`
	RefreshIntervalSeconds int      `json:"refresh_interval_seconds"`
}

func DecodeConfig(reader io.Reader, home string) (Config, error) {
	var config Config
	data, err := io.ReadAll(io.LimitReader(reader, maxConfigBytes+1))
	if err != nil {
		return Config{}, fmt.Errorf("read retained provider config: %w", err)
	}
	if len(data) > maxConfigBytes {
		return Config{}, fmt.Errorf("retained provider config exceeds %d bytes", maxConfigBytes)
	}
	if err := decodeStrictJSON(bytes.NewReader(data), &config); err != nil {
		return Config{}, fmt.Errorf("decode retained provider config: %w", err)
	}
	if err := config.Validate(home); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) Validate(home string) error {
	if config.ProtocolVersion != ConfigProtocolVersion {
		return fmt.Errorf("protocol_version must be %q", ConfigProtocolVersion)
	}
	for field, value := range map[string]string{
		"worker_id":           config.WorkerID,
		"profile_id":          config.ProfileID,
		"component_id":        config.ComponentID,
		"organization":        config.Organization,
		"runner_name":         config.RunnerName,
		"runner_group":        config.RunnerGroup,
		"agent_unit":          strings.TrimSuffix(config.AgentUnit, ".service"),
		"stable_container":    config.StableContainer,
		"candidate_container": config.CandidateContainer,
	} {
		if !safeIdentifierPattern.MatchString(value) {
			return fmt.Errorf("%s contains an unsafe identifier", field)
		}
	}
	if !strings.HasSuffix(config.AgentUnit, ".service") {
		return fmt.Errorf("agent_unit must end in .service")
	}
	if config.PluginID != GitHubPluginID {
		return fmt.Errorf("plugin_id must be %q", GitHubPluginID)
	}
	if config.StableContainer == config.CandidateContainer {
		return fmt.Errorf("candidate_container must differ from stable_container")
	}
	if config.ContainerNetwork != "bridge" {
		return fmt.Errorf("container_network must be bridge")
	}
	if !filepath.IsAbs(config.PodmanPath) || containsControl(config.PodmanPath) {
		return fmt.Errorf("podman_path must be an absolute safe path")
	}
	for field, path := range map[string]string{
		"compute_agent_path":     config.ComputeAgentPath,
		"supervisor_config_path": config.SupervisorConfigPath,
		"local_status_path":      config.LocalStatusPath,
		"install_root":           config.InstallRoot,
		"systemd_dir":            config.SystemdDir,
	} {
		if err := ValidateUserPath(home, path, false); err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
	}
	providerURL, err := url.Parse(config.ProviderURL)
	if err != nil || providerURL.Scheme != "https" || providerURL.Host == "" || providerURL.User != nil || providerURL.RawQuery != "" || providerURL.Fragment != "" {
		return fmt.Errorf("provider_url must be an HTTPS URL without credentials, query, or fragment")
	}
	if providerURL.Hostname() != config.StableContainer {
		return fmt.Errorf("provider_url host must match stable_container")
	}
	parts := strings.Split(config.Repository, "/")
	if len(parts) != 2 || parts[0] != config.Organization || !safeIdentifierPattern.MatchString(parts[1]) {
		return fmt.Errorf("repository must be organization/name for the configured organization")
	}
	if !workflowPattern.MatchString(config.Workflow) || strings.Contains(config.Workflow, "..") || containsControl(config.Workflow) {
		return fmt.Errorf("workflow contains an unsafe path")
	}
	if !gitRefPattern.MatchString(config.Ref) {
		return fmt.Errorf("ref must be a full lowercase commit SHA")
	}
	if len(config.Labels) == 0 {
		return fmt.Errorf("labels must not be empty")
	}
	seenLabels := make(map[string]struct{}, len(config.Labels))
	for _, label := range config.Labels {
		if !safeIdentifierPattern.MatchString(label) {
			return fmt.Errorf("labels contains an unsafe label")
		}
		if _, exists := seenLabels[label]; exists {
			return fmt.Errorf("labels contains duplicate %q", label)
		}
		seenLabels[label] = struct{}{}
	}
	if config.RefreshIntervalSeconds < 60 || config.RefreshIntervalSeconds > 86_400 {
		return fmt.Errorf("refresh_interval_seconds must be between 60 and 86400")
	}
	return nil
}

func decodeStrictJSON(reader io.Reader, target any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func containsControl(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0
}
