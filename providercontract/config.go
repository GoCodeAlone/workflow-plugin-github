package providercontract

type Config struct {
	Organizations []string `json:"organizations,omitempty"`
	Repositories  []string `json:"repositories,omitempty"`
	RunnerGroups  []string `json:"runner_groups,omitempty"`
	APIBaseURL    string   `json:"api_base_url,omitempty"`
	StateDir      string   `json:"state_dir"`
	Token         string   `json:"token"`
	ProviderToken string   `json:"provider_token"`
}
