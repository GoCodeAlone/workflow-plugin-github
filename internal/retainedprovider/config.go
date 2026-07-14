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
	ConfigProtocolVersion    = "retained-provider.config.v1"
	GitHubPluginID           = "workflow-plugin-github"
	providerContainerNetwork = "wfcompute-github-provider"
	maxConfigBytes           = 1 << 20
	providerProbeValueBytes  = 100
	MaxProviderProbeLabels   = 64
)

var (
	safeIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	dnsLabelPattern       = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
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
	ProviderMarkerPath     string   `json:"provider_marker_path"`
	InstallRoot            string   `json:"install_root"`
	SystemdDir             string   `json:"systemd_dir"`
	AgentUnit              string   `json:"agent_unit"`
	PodmanPath             string   `json:"podman_path"`
	SystemctlPath          string   `json:"systemctl_path"`
	LoginctlPath           string   `json:"loginctl_path"`
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

func ReadConfigFile(path, home string) (Config, error) {
	if err := ValidateUserPath(home, path, true); err != nil {
		return Config{}, fmt.Errorf("config path: %w", err)
	}
	var config Config
	if err := ReadStrictJSONFile(path, &config); err != nil {
		return Config{}, fmt.Errorf("read retained provider config: %w", err)
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
	if len(config.RunnerName) > providerProbeValueBytes {
		return fmt.Errorf("runner_name must be at most %d bytes", providerProbeValueBytes)
	}
	if config.PluginID != GitHubPluginID {
		return fmt.Errorf("plugin_id must be %q", GitHubPluginID)
	}
	managedContainerNames := []string{
		config.StableContainer,
		config.CandidateContainer,
		config.StableContainer + "-probe",
		config.CandidateContainer + "-probe",
	}
	seenContainerNames := make(map[string]struct{}, len(managedContainerNames))
	for _, name := range managedContainerNames {
		if !dnsLabelPattern.MatchString(name) {
			return fmt.Errorf("managed container name %q must be a DNS label", name)
		}
		if _, exists := seenContainerNames[name]; exists {
			return fmt.Errorf("managed container names must be distinct")
		}
		seenContainerNames[name] = struct{}{}
	}
	if config.ContainerNetwork != providerContainerNetwork {
		return fmt.Errorf("container_network must be %s", providerContainerNetwork)
	}
	for _, tool := range []struct {
		field string
		path  string
		base  string
	}{
		{field: "podman_path", path: config.PodmanPath, base: "podman"},
		{field: "systemctl_path", path: config.SystemctlPath, base: "systemctl"},
		{field: "loginctl_path", path: config.LoginctlPath, base: "loginctl"},
	} {
		if !filepath.IsAbs(tool.path) || filepath.Clean(tool.path) != tool.path || containsControl(tool.path) || filepath.Base(tool.path) != tool.base {
			return fmt.Errorf("%s must be an absolute canonical safe path to %s", tool.field, tool.base)
		}
	}
	for field, path := range map[string]string{
		"compute_agent_path":     config.ComputeAgentPath,
		"supervisor_config_path": config.SupervisorConfigPath,
		"local_status_path":      config.LocalStatusPath,
		"provider_marker_path":   config.ProviderMarkerPath,
		"install_root":           config.InstallRoot,
		"systemd_dir":            config.SystemdDir,
	} {
		if err := ValidateUserPath(home, path, false); err != nil {
			return fmt.Errorf("%s: %w", field, err)
		}
	}
	expectedInstallRoot := filepath.Join(filepath.Clean(home), ".workflow-compute", "github-runner-provider")
	if filepath.Clean(config.InstallRoot) != expectedInstallRoot {
		return fmt.Errorf("install_root must be the dedicated provider root %s", expectedInstallRoot)
	}
	externalPaths := []struct {
		field string
		path  string
	}{
		{field: "compute_agent_path", path: filepath.Clean(config.ComputeAgentPath)},
		{field: "supervisor_config_path", path: filepath.Clean(config.SupervisorConfigPath)},
		{field: "local_status_path", path: filepath.Clean(config.LocalStatusPath)},
		{field: "provider_marker_path", path: filepath.Clean(config.ProviderMarkerPath)},
		{field: "systemd_dir", path: filepath.Clean(config.SystemdDir)},
		{field: "podman_path", path: filepath.Clean(config.PodmanPath)},
		{field: "systemctl_path", path: filepath.Clean(config.SystemctlPath)},
		{field: "loginctl_path", path: filepath.Clean(config.LoginctlPath)},
	}
	for index, external := range externalPaths {
		for prior := 0; prior < index; prior++ {
			if external.path == externalPaths[prior].path {
				return fmt.Errorf("%s and %s must be distinct", externalPaths[prior].field, external.field)
			}
		}
		relative, err := filepath.Rel(expectedInstallRoot, external.path)
		if err != nil {
			return fmt.Errorf("%s relative to install_root: %w", external.field, err)
		}
		if relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%s must remain outside install_root", external.field)
		}
	}
	paths := LifecyclePathsFor(config)
	reserved := reservedProviderPaths(paths)
	for _, external := range externalPaths {
		for _, managed := range reserved {
			if pathsOverlap(external.path, managed) {
				return fmt.Errorf("%s must not overlap a managed provider path", external.field)
			}
		}
		if external.field != "systemd_dir" {
			for _, managed := range managedWiringPaths(paths) {
				if pathsOverlap(external.path, managed) {
					return fmt.Errorf("%s must not overlap a managed provider path", external.field)
				}
			}
		}
	}
	for index, external := range externalPaths {
		for prior := 0; prior < index; prior++ {
			if pathsOverlap(external.path, externalPaths[prior].path) {
				return fmt.Errorf("%s and %s authority paths overlap", externalPaths[prior].field, external.field)
			}
		}
	}
	providerURL, err := url.Parse(config.ProviderURL)
	if err != nil || providerURL.Scheme != "https" || providerURL.Host == "" || providerURL.User != nil || providerURL.RawQuery != "" || providerURL.Fragment != "" || (providerURL.Path != "" && providerURL.Path != "/") || providerURL.Port() != "18090" {
		return fmt.Errorf("provider_url must be an HTTPS URL without credentials, query, or fragment")
	}
	if providerURL.Hostname() != config.StableContainer {
		return fmt.Errorf("provider_url host must match stable_container")
	}
	parts := strings.Split(config.Repository, "/")
	if len(parts) != 2 || parts[0] != config.Organization || !safeIdentifierPattern.MatchString(parts[1]) {
		return fmt.Errorf("repository must be organization/name for the configured organization")
	}
	if !workflowPattern.MatchString(config.Workflow) || strings.Contains(config.Workflow, "..") || containsControl(config.Workflow) || (!strings.HasSuffix(config.Workflow, ".yml") && !strings.HasSuffix(config.Workflow, ".yaml")) {
		return fmt.Errorf("workflow contains an unsafe path")
	}
	if !gitRefPattern.MatchString(config.Ref) {
		return fmt.Errorf("ref must be a full lowercase commit SHA")
	}
	if len(config.Labels) == 0 || len(config.Labels) > MaxProviderProbeLabels {
		return fmt.Errorf("labels must contain between 1 and %d entries", MaxProviderProbeLabels)
	}
	seenLabels := make(map[string]struct{}, len(config.Labels))
	for _, label := range config.Labels {
		if len(label) > providerProbeValueBytes || !safeIdentifierPattern.MatchString(label) {
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

func reservedProviderPaths(paths LifecyclePaths) []string {
	return []string{
		paths.Root,
		paths.ConfigFile, paths.Launcher, paths.ActiveState, paths.Journal,
		paths.InstallLock, paths.InstallJournal,
		paths.LifecycleJournal, paths.LifecycleTransactions,
		paths.LifecycleAudit, paths.LifecycleAuditLock,
		paths.ProviderState, paths.PackagesRoot, paths.CandidatesRoot,
		paths.ProviderEnv, paths.ProbeEnv, paths.AgentEnv,
		paths.CAKey, paths.TLSRoot, paths.CAFile, paths.ServerCert, paths.ServerKey,
		paths.ContainersConf,
	}
}

func pathsOverlap(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if left == right {
		return true
	}
	for _, pair := range [][2]string{{left, right}, {right, left}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
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
