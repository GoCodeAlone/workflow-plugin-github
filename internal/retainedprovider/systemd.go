package retainedprovider

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	systemdunit "github.com/coreos/go-systemd/v22/unit"
)

const (
	refreshServiceUnit = "workflow-plugin-github-runner-provider-refresh.service"
	refreshPathUnit    = "workflow-plugin-github-runner-provider-refresh.path"
	refreshTimerUnit   = "workflow-plugin-github-runner-provider-refresh.timer"
)

type SystemdUnits struct {
	ProviderService string
	RefreshService  string
	RefreshPath     string
	RefreshTimer    string
	AgentDropIn     string
}

func RenderSystemdUnits(config Config, paths LifecyclePaths) (SystemdUnits, error) {
	for name, path := range map[string]string{
		"launcher":        paths.Launcher,
		"config":          paths.ConfigFile,
		"provider marker": config.ProviderMarkerPath,
		"agent env":       paths.AgentEnv,
	} {
		if !filepath.IsAbs(path) || containsControl(path) {
			return SystemdUnits{}, fmt.Errorf("%s path must be absolute and safe", name)
		}
	}
	interval := strconv.Itoa(config.RefreshIntervalSeconds) + "s"
	return SystemdUnits{
		ProviderService: "[Unit]\n" +
			"Description=Workflow Compute GitHub runner provider\n" +
			"Wants=network-online.target\n" +
			"After=network-online.target\n\n" +
			"[Service]\n" +
			"Type=simple\n" +
			"ExecStart=" + systemdQuote(paths.Launcher) + " retained serve-active -config " + systemdQuote(paths.ConfigFile) + "\n" +
			"Restart=on-failure\n" +
			"RestartSec=5s\n\n" +
			"[Install]\n" +
			"WantedBy=default.target\n",
		RefreshService: "[Unit]\n" +
			"Description=Refresh Workflow Compute GitHub runner provider\n" +
			"After=network-online.target\n\n" +
			"[Service]\n" +
			"Type=oneshot\n" +
			"ExecStart=" + systemdQuote(paths.Launcher) + " retained refresh -config " + systemdQuote(paths.ConfigFile) + "\n" +
			"TimeoutStartSec=" + systemdTimeout(retainedRefreshServiceStartTimeout) + "\n" +
			"TimeoutStopSec=" + systemdTimeout(retainedRefreshServiceStopTimeout) + "\n",
		RefreshPath: "[Unit]\n" +
			"Description=Watch signed GitHub runner provider package marker\n\n" +
			"[Path]\n" +
			"PathChanged=" + systemdPathValue(config.ProviderMarkerPath) + "\n" +
			"Unit=" + refreshServiceUnit + "\n\n" +
			"[Install]\n" +
			"WantedBy=default.target\n",
		RefreshTimer: "[Unit]\n" +
			"Description=Reconcile GitHub runner provider package\n\n" +
			"[Timer]\n" +
			"OnBootSec=30s\n" +
			"OnUnitInactiveSec=" + interval + "\n" +
			"Persistent=true\n" +
			"Unit=" + refreshServiceUnit + "\n\n" +
			"[Install]\n" +
			"WantedBy=timers.target\n",
		AgentDropIn: "[Service]\nEnvironmentFile=" + systemdPathValue(paths.AgentEnv) + "\n",
	}, nil
}

func systemdTimeout(duration time.Duration) string {
	seconds := (duration + time.Second - 1) / time.Second
	return strconv.FormatInt(int64(seconds), 10) + "s"
}

func systemdQuote(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`)
	return `"` + replacer.Replace(value) + `"`
}

func systemdPathValue(value string) string {
	var escaped strings.Builder
	escaped.Grow(len(value))
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character == '%':
			escaped.WriteString("%%")
		case character == '/' || character == '.' || character == '_' || character == '-' ||
			character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9':
			escaped.WriteByte(character)
		default:
			_, _ = fmt.Fprintf(&escaped, `\x%02x`, character)
		}
	}
	return escaped.String()
}

type Credentials struct {
	GitHubToken   string
	ProviderToken string
}

const maxCredentialBytes = 32 << 10

type InstallMaterial struct {
	ProviderEnv    []byte
	ProbeEnv       []byte
	AgentEnv       []byte
	ContainersConf []byte
	CACert         []byte
	CAKey          []byte
	ServerCert     []byte
	ServerKey      []byte
}

func GenerateInstallMaterial(config Config, credentials Credentials, random io.Reader, now time.Time) (InstallMaterial, error) {
	if err := validateCredential(credentials.GitHubToken); err != nil {
		return InstallMaterial{}, errors.New("GitHub credential is required and must be canonical")
	}
	if err := validateCredential(credentials.ProviderToken); err != nil {
		return InstallMaterial{}, errors.New("provider credential is required and must be canonical")
	}
	if random == nil {
		random = rand.Reader
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	caCert, caKey, serverCert, serverKey, err := generateProviderTLS(config, random, now.UTC())
	if err != nil {
		return InstallMaterial{}, err
	}
	providerEnvironment, err := renderPodmanEnvironment([]environmentValue{
		{Name: "GITHUB_RUNNER_PROVIDER_TOKEN", Value: credentials.ProviderToken},
		{Name: "GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN", Value: credentials.GitHubToken},
		{Name: "GITHUB_RUNNER_PROVIDER_STATE_DIR", Value: providerStateMount},
		{Name: "GITHUB_RUNNER_PROVIDER_REPOSITORIES", Value: config.Repository},
		{Name: "GITHUB_RUNNER_PROVIDER_ORGANIZATIONS", Value: config.Organization},
		{Name: "GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS", Value: config.RunnerGroup},
		{Name: "GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE", Value: providerTLSCertPath},
		{Name: "GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE", Value: providerTLSKeyPath},
	})
	if err != nil {
		return InstallMaterial{}, err
	}
	probeEnvironment, err := renderPodmanEnvironment([]environmentValue{{Name: "GITHUB_RUNNER_PROVIDER_TOKEN", Value: credentials.ProviderToken}})
	if err != nil {
		return InstallMaterial{}, err
	}
	agentEnvironment, err := renderSystemdEnvironment([]environmentValue{
		{Name: "WORKFLOW_COMPUTE_DYNAMIC_PROVIDER_GITHUB_ACTIONS_RUNNER_ENV_KEYS", Value: "COMPUTE_GITHUB_RUNNER_PROVIDER_URL,COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN,COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64"},
		{Name: "COMPUTE_GITHUB_RUNNER_PROVIDER_URL", Value: config.ProviderURL},
		{Name: "COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", Value: credentials.ProviderToken},
		{Name: "COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", Value: base64.StdEncoding.EncodeToString(caCert)},
		{Name: "CONTAINERS_CONF", Value: LifecyclePathsFor(config).ContainersConf},
	})
	if err != nil {
		return InstallMaterial{}, err
	}
	return InstallMaterial{
		ProviderEnv:    providerEnvironment,
		ProbeEnv:       probeEnvironment,
		AgentEnv:       agentEnvironment,
		ContainersConf: []byte("[network]\ndefault_network = \"" + config.ContainerNetwork + "\"\n"),
		CACert:         caCert,
		CAKey:          caKey,
		ServerCert:     serverCert,
		ServerKey:      serverKey,
	}, nil
}

func WriteInstallMaterial(paths LifecyclePaths, material InstallMaterial) error {
	for _, file := range []struct {
		path string
		data []byte
	}{
		{path: paths.ProviderEnv, data: material.ProviderEnv},
		{path: paths.ProbeEnv, data: material.ProbeEnv},
		{path: paths.AgentEnv, data: material.AgentEnv},
		{path: paths.ContainersConf, data: material.ContainersConf},
		{path: paths.CAFile, data: material.CACert},
		{path: paths.CAKey, data: material.CAKey},
		{path: paths.ServerCert, data: material.ServerCert},
		{path: paths.ServerKey, data: material.ServerKey},
	} {
		if err := atomicWriteFile(file.path, file.data, 0o600); err != nil {
			return fmt.Errorf("write install material: %w", err)
		}
	}
	return nil
}

type environmentValue struct {
	Name  string
	Value string
}

func renderPodmanEnvironment(values []environmentValue) ([]byte, error) {
	var builder strings.Builder
	for _, value := range values {
		if !safeEnvironmentKey(value.Name) || value.Value == "" || strings.ContainsAny(value.Value, "\r\n\x00") {
			return nil, errors.New("podman environment contains an invalid value")
		}
		builder.WriteString(value.Name)
		builder.WriteByte('=')
		builder.WriteString(value.Value)
		builder.WriteByte('\n')
	}
	return []byte(builder.String()), nil
}

func renderSystemdEnvironment(values []environmentValue) ([]byte, error) {
	var builder strings.Builder
	for _, value := range values {
		if !safeEnvironmentKey(value.Name) || value.Value == "" || strings.ContainsAny(value.Value, "\r\n\x00") {
			return nil, errors.New("systemd environment contains an invalid value")
		}
		builder.WriteString(value.Name)
		builder.WriteString(`="`)
		builder.WriteString(strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value.Value))
		builder.WriteString("\"\n")
	}
	return []byte(builder.String()), nil
}

func validateCredential(value string) error {
	if value == "" || len(value) > maxCredentialBytes || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\x00") {
		return errors.New("invalid credential")
	}
	return nil
}

func generateProviderTLS(config Config, random io.Reader, now time.Time) ([]byte, []byte, []byte, []byte, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), random)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("generate provider CA key: %w", err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), random)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("generate provider server key: %w", err)
	}
	serial := big.NewInt(now.UnixNano())
	if serial.Sign() <= 0 {
		serial = big.NewInt(1)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Workflow Compute GitHub Runner Provider CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(random, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("create provider CA certificate: %w", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: new(big.Int).Add(serial, big.NewInt(1)),
		Subject:      pkix.Name{CommonName: config.StableContainer},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost", config.StableContainer, config.CandidateContainer},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	serverDER, err := x509.CreateCertificate(random, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("create provider server certificate: %w", err)
	}
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal provider CA key: %w", err)
	}
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("marshal provider server key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}), nil
}

const providerServerCertificateRenewalWindow = 30 * 24 * time.Hour

func renewProviderServerCertificate(config Config, paths LifecyclePaths, random io.Reader, now time.Time) (bool, error) {
	caCertificate, caKey, serverCertificate, serverKey, err := readProviderTLSAuthority(config, paths, now)
	if err != nil {
		return false, err
	}
	if serverCertificate.NotAfter.After(now.Add(providerServerCertificateRenewalWindow)) {
		return false, nil
	}
	if random == nil {
		random = rand.Reader
	}
	notAfter := now.AddDate(1, 0, 0)
	if caLimit := caCertificate.NotAfter.Add(-time.Hour); notAfter.After(caLimit) {
		notAfter = caLimit
	}
	if !notAfter.After(now.Add(providerServerCertificateRenewalWindow)) {
		return false, errors.New("provider CA is too close to expiry; credential reinstall is required")
	}
	serial := big.NewInt(now.UnixNano())
	if serial.Sign() <= 0 {
		serial = big.NewInt(1)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: config.StableContainer},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost", config.StableContainer, config.CandidateContainer},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(random, template, caCertificate, &serverKey.PublicKey, caKey)
	if err != nil {
		return false, fmt.Errorf("renew provider server certificate: %w", err)
	}
	if err := atomicWriteFile(paths.ServerCert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return false, fmt.Errorf("write renewed provider server certificate: %w", err)
	}
	return true, nil
}

func providerServerCertificateNeedsRenewal(config Config, paths LifecyclePaths, now time.Time) (bool, error) {
	_, _, certificate, _, err := readProviderTLSAuthority(config, paths, now)
	if err != nil {
		return false, err
	}
	return !certificate.NotAfter.After(now.Add(providerServerCertificateRenewalWindow)), nil
}

func readProviderTLSAuthority(config Config, paths LifecyclePaths, now time.Time) (*x509.Certificate, *ecdsa.PrivateKey, *x509.Certificate, *ecdsa.PrivateKey, error) {
	caCertificate, err := readProviderCertificate(paths.CAFile)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("read provider CA certificate: %w", err)
	}
	caKey, err := readProviderECKey(paths.CAKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("read provider CA key: %w", err)
	}
	serverCertificate, err := readProviderCertificate(paths.ServerCert)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("read provider server certificate: %w", err)
	}
	serverKey, err := readProviderECKey(paths.ServerKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("read provider server key: %w", err)
	}
	if !caCertificate.IsCA || caCertificate.NotBefore.After(now) || !caCertificate.NotAfter.After(now) || !sameECDSAPublicKey(caCertificate.PublicKey, &caKey.PublicKey) {
		return nil, nil, nil, nil, errors.New("provider CA authority is invalid or expired")
	}
	if err := serverCertificate.CheckSignatureFrom(caCertificate); err != nil || !sameECDSAPublicKey(serverCertificate.PublicKey, &serverKey.PublicKey) {
		return nil, nil, nil, nil, errors.New("provider server certificate authority or key binding is invalid")
	}
	if serverCertificate.NotBefore.After(now) {
		return nil, nil, nil, nil, errors.New("provider server certificate is not currently valid")
	}
	for _, hostname := range []string{config.StableContainer, config.CandidateContainer} {
		if err := serverCertificate.VerifyHostname(hostname); err != nil {
			return nil, nil, nil, nil, errors.New("provider server certificate hostname binding is invalid")
		}
	}
	return caCertificate, caKey, serverCertificate, serverKey, nil
}

func readProviderCertificate(path string) (*x509.Certificate, error) {
	data, err := readProviderSecretPEM(path)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("provider certificate PEM is invalid")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.New("provider certificate is invalid")
	}
	return certificate, nil
}

func readProviderECKey(path string) (*ecdsa.PrivateKey, error) {
	data, err := readProviderSecretPEM(path)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != "EC PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("provider private key PEM is invalid")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil || key.Curve != elliptic.P256() {
		return nil, errors.New("provider private key is invalid")
	}
	return key, nil
}

func readProviderSecretPEM(path string) ([]byte, error) {
	entry, err := os.Lstat(path)
	if err != nil || !entry.Mode().IsRegular() || entry.Mode().Perm() != 0o600 || entry.Size() <= 0 || entry.Size() > MaxStateFileBytes {
		return nil, errors.New("provider TLS input must be a bounded owner-only regular file")
	}
	if err := validateOwner(entry); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(entry, opened) || opened.Size() > MaxStateFileBytes {
		return nil, errors.New("provider TLS input changed during open")
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxStateFileBytes+1))
	if err != nil || len(data) > MaxStateFileBytes {
		return nil, errors.New("read provider TLS input")
	}
	return data, nil
}

func sameECDSAPublicKey(value any, expected *ecdsa.PublicKey) bool {
	actual, ok := value.(*ecdsa.PublicKey)
	return ok && actual.Equal(expected)
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) (returnErr error) {
	if len(data) == 0 || len(data) > MaxStateFileBytes {
		return errors.New("generated file must be non-empty and at most 1 MiB")
	}
	if mode != 0o600 && mode != 0o700 {
		return errors.New("generated file mode must be 0600 or 0700")
	}
	directory := filepath.Dir(path)
	if err := mkdirAllDurable(directory, 0o700); err != nil {
		return err
	}
	if err := rejectWritableDestination(path); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".retained-provider-install-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if err := os.Remove(temporaryPath); returnErr == nil && err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = err
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := rejectWritableDestination(path); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func rejectWritableDestination(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("generated file destination must be regular and not a symlink")
	}
	return validateOwner(info)
}

const (
	installMaintenanceID       = "workflow-plugin-github-retained-provider-install"
	installMaintenanceReason   = "workflow-plugin-github-retained-provider-install"
	refreshMaintenanceID       = "workflow-plugin-github-retained-provider-refresh"
	refreshMaintenanceReason   = "workflow-plugin-github-retained-provider-refresh"
	uninstallMaintenanceID     = "workflow-plugin-github-retained-provider-uninstall"
	uninstallMaintenanceReason = "workflow-plugin-github-retained-provider-uninstall"
	maintenanceMarkerKind      = "workflow-compute.supervisor-maintenance.v1"
	localStatusProtocolVersion = "compute.local_status.v1"
	localStatusAttempts        = 30
	installTransactionProtocol = "retained-provider.install-transaction.v1"
)

type Installer struct {
	Runner         CommandRunner
	ExecutablePath func() (string, error)
	UserID         func() (string, error)
	Random         io.Reader
	Now            func() time.Time
	Sleep          func(context.Context, time.Duration) error
	Refresh        func(context.Context, Config) (Status, error)
	ProbeActive    func(context.Context, Config) error
}

type maintenanceRecord struct {
	Kind      string    `json:"kind"`
	ID        string    `json:"id"`
	ProfileID string    `json:"profile_id"`
	Reason    string    `json:"reason"`
	StartedAt time.Time `json:"started_at"`
}

type maintenanceState struct {
	Active      bool               `json:"active"`
	Durable     bool               `json:"durable"`
	Maintenance *maintenanceRecord `json:"maintenance,omitempty"`
}

type maintenanceDisposition string

const (
	maintenanceExactActive maintenanceDisposition = "exact_active"
	maintenanceInactive    maintenanceDisposition = "inactive"
	maintenanceConflicting maintenanceDisposition = "conflicting"
)

type localAgentStatus struct {
	ProtocolVersion string          `json:"protocol_version"`
	WorkerID        string          `json:"worker_id"`
	State           string          `json:"state"`
	TaskID          string          `json:"task_id,omitempty"`
	LeaseID         string          `json:"lease_id,omitempty"`
	Message         string          `json:"message,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
	Diagnostic      json.RawMessage `json:"diagnostic,omitempty"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type managedFileSnapshot struct {
	Path    string      `json:"path"`
	Backup  string      `json:"backup"`
	Mode    os.FileMode `json:"mode"`
	Existed bool        `json:"existed"`
	SHA256  string      `json:"sha256,omitempty"`
}

type systemdActivation struct {
	ProviderService bool `json:"provider_service"`
	RefreshPath     bool `json:"refresh_path"`
	RefreshTimer    bool `json:"refresh_timer"`
}

type systemdUnitState struct {
	LoadState     string `json:"load_state"`
	FragmentPath  string `json:"fragment_path"`
	UnitFileState string `json:"unit_file_state"`
	ActiveState   string `json:"active_state"`
}

type installTransactionPhase string

const (
	installTransactionPrepared  installTransactionPhase = "prepared"
	installTransactionReady     installTransactionPhase = "ready"
	installTransactionCommitted installTransactionPhase = "committed"
)

type installTransactionJournal struct {
	ProtocolVersion string                      `json:"protocol_version"`
	Operation       string                      `json:"operation"`
	Phase           installTransactionPhase     `json:"phase"`
	MaintenanceID   string                      `json:"maintenance_id"`
	AgentStopped    bool                        `json:"agent_stopped"`
	Snapshots       []managedFileSnapshot       `json:"snapshots"`
	PreviousUnits   map[string]systemdUnitState `json:"previous_units"`
	Activation      systemdActivation           `json:"activation"`
	StartedAt       time.Time                   `json:"started_at"`
	UpdatedAt       time.Time                   `json:"updated_at"`
}

func (journal installTransactionJournal) Validate(paths LifecyclePaths) error {
	if journal.ProtocolVersion != installTransactionProtocol {
		return errors.New("unsupported retained provider install transaction protocol")
	}
	expectedMaintenanceID := installMaintenanceID
	if journal.Operation == "uninstall" {
		expectedMaintenanceID = uninstallMaintenanceID
	} else if journal.Operation != "install" {
		return errors.New("retained provider install transaction operation is invalid")
	}
	if journal.MaintenanceID != expectedMaintenanceID || !journal.AgentStopped {
		return errors.New("retained provider install transaction identity is invalid")
	}
	if journal.Phase != installTransactionPrepared && journal.Phase != installTransactionReady && journal.Phase != installTransactionCommitted {
		return errors.New("retained provider install transaction phase is invalid")
	}
	if journal.StartedAt.IsZero() || journal.UpdatedAt.Before(journal.StartedAt) {
		return errors.New("retained provider install transaction timestamps are invalid")
	}
	if len(journal.Snapshots) == 0 || len(journal.Snapshots) > len(managedInstallPaths(paths)) {
		return errors.New("retained provider install transaction snapshots are invalid")
	}
	allowedPaths := make(map[string]struct{}, len(managedInstallPaths(paths)))
	for _, path := range managedInstallPaths(paths) {
		allowedPaths[path] = struct{}{}
	}
	seenPaths := make(map[string]struct{}, len(journal.Snapshots))
	seenBackups := make(map[string]struct{}, len(journal.Snapshots))
	backupRoot := ""
	for _, snapshot := range journal.Snapshots {
		if _, allowed := allowedPaths[snapshot.Path]; !allowed {
			return errors.New("retained provider install transaction contains an unmanaged path")
		}
		if _, duplicate := seenPaths[snapshot.Path]; duplicate {
			return errors.New("retained provider install transaction contains duplicate paths")
		}
		if _, duplicate := seenBackups[snapshot.Backup]; duplicate {
			return errors.New("retained provider install transaction contains duplicate backups")
		}
		seenPaths[snapshot.Path] = struct{}{}
		seenBackups[snapshot.Backup] = struct{}{}
		root := filepath.Dir(snapshot.Backup)
		if backupRoot == "" {
			backupRoot = root
		} else if root != backupRoot {
			return errors.New("retained provider install transaction backup roots differ")
		}
		if !strings.HasPrefix(filepath.Base(root), ".install-backup-") {
			return errors.New("retained provider install transaction backup root is invalid")
		}
		if err := ValidateUserPath(paths.Root, snapshot.Backup, journal.Phase == installTransactionPrepared && snapshot.Existed); err != nil {
			return fmt.Errorf("validate retained provider install transaction backup: %w", err)
		}
		if snapshot.Existed {
			if snapshot.Mode != 0o600 && snapshot.Mode != 0o700 {
				return errors.New("retained provider install transaction snapshot mode is invalid")
			}
			if journal.Phase == installTransactionPrepared {
				info, err := os.Lstat(snapshot.Backup)
				if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != snapshot.Mode {
					return errors.New("retained provider install transaction backup is invalid")
				}
				if err := validateOwner(info); err != nil {
					return fmt.Errorf("retained provider install transaction backup ownership: %w", err)
				}
			}
		} else if snapshot.Mode != 0 {
			return errors.New("retained provider absent snapshot mode is invalid")
		}
	}
	for unit, state := range journal.PreviousUnits {
		if unit != providerServiceUnit && unit != refreshPathUnit && unit != refreshTimerUnit {
			return errors.New("retained provider install transaction contains an unmanaged unit")
		}
		if err := validateRestorableUnitState(state); err != nil {
			return fmt.Errorf("validate retained provider install transaction unit %s: %w", unit, err)
		}
	}
	return nil
}

func newInstallTransactionJournal(operation string, snapshots []managedFileSnapshot, previousUnits map[string]systemdUnitState, now time.Time) installTransactionJournal {
	maintenanceID := installMaintenanceID
	if operation == "uninstall" {
		maintenanceID = uninstallMaintenanceID
	}
	return installTransactionJournal{
		ProtocolVersion: installTransactionProtocol,
		Operation:       operation,
		Phase:           installTransactionPrepared,
		MaintenanceID:   maintenanceID,
		AgentStopped:    true,
		Snapshots:       snapshots,
		PreviousUnits:   previousUnits,
		StartedAt:       now,
		UpdatedAt:       now,
	}
}

func readInstallTransactionJournal(paths LifecyclePaths) (installTransactionJournal, bool, error) {
	if err := ValidateUserPath(filepath.Dir(paths.Root), paths.InstallJournal, false); err != nil {
		return installTransactionJournal{}, false, fmt.Errorf("install transaction path: %w", err)
	}
	var journal installTransactionJournal
	if err := ReadStrictJSONFile(paths.InstallJournal, &journal); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return installTransactionJournal{}, false, nil
		}
		return installTransactionJournal{}, false, err
	}
	if err := journal.Validate(paths); err != nil {
		return installTransactionJournal{}, false, err
	}
	return journal, true, nil
}

func writeInstallTransactionJournal(paths LifecyclePaths, journal installTransactionJournal) error {
	if err := journal.Validate(paths); err != nil {
		return err
	}
	return AtomicWriteJSON(paths.InstallJournal, journal)
}

func writeInstallTransactionPhase(paths LifecyclePaths, journal *installTransactionJournal, phase installTransactionPhase, now time.Time) error {
	next := *journal
	next.Phase = phase
	next.UpdatedAt = now
	if err := writeInstallTransactionJournal(paths, next); err != nil {
		return err
	}
	*journal = next
	return nil
}

func (installer Installer) recoverInstallTransaction(ctx context.Context, config Config, paths LifecyclePaths, refresher Refresher) error {
	journal, found, err := readInstallTransactionJournal(paths)
	if err != nil {
		return fmt.Errorf("read retained provider install transaction: %w", err)
	}
	providerJournal, providerFound, err := readTransactionJournalForConfig(paths.Journal, config)
	if err != nil {
		return fmt.Errorf("read retained provider refresh transaction: %w", err)
	}
	if !found {
		if providerFound && providerJournal.DeferredCommit && providerJournal.Phase == JournalCommitted {
			return errors.New("deferred provider refresh has no durable outer install transaction")
		}
		return nil
	}
	if providerFound && (!providerJournal.DeferredCommit || providerJournal.Phase != JournalCommitted) {
		return errors.New("outer install transaction references an incompatible provider refresh transaction")
	}
	switch journal.Phase {
	case installTransactionPrepared:
		var providerRollbackErr error
		if providerFound {
			providerRollbackErr = refresher.rollbackDeferredRefresh(ctx, config)
		}
		reason := installMaintenanceReason
		if journal.Operation == "uninstall" {
			reason = uninstallMaintenanceReason
		}
		rollbackErr := installer.rollbackInstall(ctx, config, journal.Snapshots, journal.PreviousUnits, journal.AgentStopped, true, journal.MaintenanceID, reason, journal.Activation)
		if err := errors.Join(providerRollbackErr, rollbackErr); err != nil {
			return fmt.Errorf("rollback interrupted retained provider %s: %w", journal.Operation, err)
		}
	case installTransactionReady:
		reason := installMaintenanceReason
		if journal.Operation == "uninstall" {
			reason = uninstallMaintenanceReason
		}
		if err := installer.endMaintenance(ctx, config, journal.MaintenanceID, reason); err != nil {
			return fmt.Errorf("release interrupted retained provider %s maintenance: %w", journal.Operation, err)
		}
		if err := writeInstallTransactionPhase(paths, &journal, installTransactionCommitted, installer.now()); err != nil {
			return fmt.Errorf("commit interrupted retained provider %s: %w", journal.Operation, err)
		}
		fallthrough
	case installTransactionCommitted:
		if providerFound {
			if err := refresher.finalizeDeferredRefresh(config); err != nil {
				return fmt.Errorf("finalize interrupted retained provider %s: %w", journal.Operation, err)
			}
		}
		if err := removeSnapshots(journal.Snapshots); err != nil {
			return fmt.Errorf("remove interrupted retained provider %s snapshots: %w", journal.Operation, err)
		}
	default:
		return errors.New("retained provider install transaction phase is invalid")
	}
	if err := removeDurableFile(paths.InstallJournal); err != nil {
		return fmt.Errorf("remove recovered retained provider install transaction: %w", err)
	}
	return nil
}

func (installer Installer) Install(ctx context.Context, home string, config Config, credentials Credentials) (status Status, returnErr error) {
	if installer.Runner == nil {
		return Status{}, errors.New("command runner is required")
	}
	if err := config.Validate(home); err != nil {
		return Status{}, err
	}
	paths := LifecyclePathsFor(config)
	lifecycleRefresher := Refresher{
		Runner: installer.Runner,
		ExecutablePath: func() (string, error) {
			return paths.Launcher, nil
		},
		Now:   installer.Now,
		Sleep: installer.Sleep,
	}
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		return Status{}, fmt.Errorf("acquire retained provider install lock: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()
	if err := mkdirAllDurable(paths.Root, 0o700); err != nil {
		return Status{}, fmt.Errorf("create retained provider root: %w", err)
	}
	if err := validateInstallRoot(paths.Root); err != nil {
		return Status{}, err
	}
	if err := installer.recoverLifecycleTransaction(ctx, home, paths, lifecycleRefresher); err != nil {
		return Status{}, err
	}
	if err := validateInstalledConfigBinding(home, config, paths); err != nil {
		return Status{}, err
	}
	if err := installer.recoverInstallTransaction(ctx, config, paths, lifecycleRefresher); err != nil {
		return Status{}, err
	}
	update, executable, material, units, err := installer.preflightInstall(ctx, config, paths, credentials)
	if err != nil {
		return Status{}, err
	}
	active, activeFound, err := readActiveStateForConfig(paths.ActiveState, config)
	if err != nil {
		return Status{}, err
	}
	effect := ProviderChanged
	if activeFound && active.Current.Update.SHA256 == update.SHA256 {
		runtimeRepair, err := lifecycleRefresher.activeProviderImageNeedsRepair(ctx, config, active.Current)
		if err != nil {
			return Status{}, err
		}
		if !runtimeRepair {
			effect = ProviderUnchanged
		}
	}
	transaction, err := newLifecycleJournal(config, LifecycleInstall, effect, nil, installer.now())
	if err != nil {
		return Status{}, err
	}
	beforeSignature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return Status{}, err
	}
	transaction.Recovery.AgentUnitBefore = beforeSignature
	if effect == ProviderUnchanged {
		transaction.Unchanged = &LifecycleUnchangedProvenance{Active: active.Current, Candidate: update}
	}
	if err := startLifecycleTransaction(home, paths, &transaction); err != nil {
		return Status{}, err
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleFencing, "", installer.now()); err != nil {
		return Status{}, err
	}
	fail := func(cause error) (Status, error) {
		rollbackContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycleRecoveryTimeout)
		defer cancel()
		auditErr := writeLifecycleDiagnostic(home, paths, &transaction, AuditError, "operation_failed", installer.now())
		return Status{}, errors.Join(cause, auditErr, installer.recoverLifecycleTransaction(rollbackContext, home, paths, lifecycleRefresher))
	}
	maintenance, err := installer.beginMaintenance(ctx, config, transaction.TransactionID, installMaintenanceReason)
	if err != nil {
		return fail(err)
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", maintenance.StartedAt); err != nil {
		return Status{}, fmt.Errorf("wait for retained agent maintenance fence: %w", err)
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return Status{}, err
	}
	if err := snapshotManagedFilesForLifecycle(home, paths, &transaction, installer.now()); err != nil {
		return fail(fmt.Errorf("snapshot retained provider wiring: %w", err))
	}
	previousUnits, err := installer.captureManagedUnitStates(ctx, config.SystemctlPath)
	if err != nil {
		return fail(fmt.Errorf("snapshot retained provider unit state: %w", err))
	}
	transaction.WiringIntent = managedWiringIntent(paths, units, true)
	intendedSignature, err := deriveLifecycleAgentUnitSignature(beforeSignature, paths, transaction.WiringIntent, &LifecycleFileAttestation{
		Path: paths.AgentEnv, SHA256: digestBytes(material.AgentEnv),
	})
	if err != nil {
		return fail(err)
	}
	transaction.AgentUnitIntended = &intendedSignature
	transaction.PreviousUnits = previousUnits
	transaction.UpdatedAt = installer.now()
	if err := writeLifecycleJournal(home, paths, transaction); err != nil {
		return fail(fmt.Errorf("write retained provider install recovery state: %w", err))
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleFenced, "", installer.now()); err != nil {
		return fail(err)
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "stop", config.AgentUnit); err != nil {
		return fail(fmt.Errorf("stop retained agent: %w", err))
	}
	watchUnits := previouslyLoadedWatchUnits(previousUnits)
	if len(watchUnits) > 0 {
		arguments := append([]string{"disable", "--now"}, watchUnits...)
		if err := installer.systemctl(ctx, config.SystemctlPath, arguments...); err != nil {
			return fail(fmt.Errorf("pause retained provider refresh: %w", err))
		}
	}
	if err := writeInstalledProvider(config, paths, executable, update.SHA256, material, units); err != nil {
		return fail(err)
	}
	if err := installer.ensureProviderNetwork(ctx, config); err != nil {
		return fail(err)
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "daemon-reload"); err != nil {
		return fail(fmt.Errorf("reload user systemd: %w", err))
	}
	loadedSignature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return fail(err)
	}
	if !equalLifecycleSystemdSignature(loadedSignature, intendedSignature) {
		return fail(errors.New("reloaded retained agent unit does not match intended signature"))
	}
	activation := transaction.Activation
	activation.ProviderService = true
	transaction.Activation = activation
	transaction.UpdatedAt = installer.now()
	if err := writeLifecycleJournal(home, paths, transaction); err != nil {
		return fail(fmt.Errorf("record provider service activation: %w", err))
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "enable", providerServiceUnit); err != nil {
		return fail(fmt.Errorf("enable provider service: %w", err))
	}
	refresh := installer.Refresh
	probeActive := installer.ProbeActive
	if refresh == nil || probeActive == nil {
		if refresh == nil {
			refresh = func(ctx context.Context, config Config) (Status, error) {
				return lifecycleRefresher.refreshUnderLifecycleTransaction(ctx, config, false, true, transaction.TransactionID, config.ProfileID, "", effect)
			}
		}
		if probeActive == nil {
			probeActive = lifecycleRefresher.RestartAndProbeActive
		}
	}
	status, err = refresh(ctx, config)
	if err != nil {
		return fail(fmt.Errorf("activate retained provider: %w", err))
	}
	if err := probeActive(ctx, config); err != nil {
		return fail(fmt.Errorf("revalidate retained provider: %w", err))
	}
	if transaction.Unchanged != nil {
		transaction.Unchanged.StableProbeAt = installer.now()
	}
	inner, innerFound, err := readTransactionJournalForConfig(paths.Journal, config)
	if err != nil {
		return fail(err)
	}
	if effect == ProviderChanged {
		if !innerFound || inner.Phase != JournalCommitted || inner.OuterTransactionID != transaction.TransactionID || inner.ProfileID != config.ProfileID {
			return fail(errors.New("changed install did not leave a matching deferred committed provider transaction"))
		}
		transaction.ProviderTransaction = &LifecycleProviderTransaction{TransactionID: inner.ID, ProfileID: config.ProfileID, Digest: inner.Candidate.Update.SHA256}
	} else if innerFound {
		return fail(errors.New("unchanged install unexpectedly created a provider transaction"))
	}
	transaction.UpdatedAt = installer.now()
	if err := writeLifecycleJournal(home, paths, transaction); err != nil {
		return fail(err)
	}
	_, err = installer.enableWatchUnitBefore(ctx, config.SystemctlPath, refreshPathUnit, func() error {
		activation := transaction.Activation
		activation.RefreshPath = true
		transaction.Activation = activation
		transaction.UpdatedAt = installer.now()
		return writeLifecycleJournal(home, paths, transaction)
	})
	if err != nil {
		return fail(fmt.Errorf("enable retained provider refresh: %w", err))
	}
	_, err = installer.enableWatchUnitBefore(ctx, config.SystemctlPath, refreshTimerUnit, func() error {
		activation := transaction.Activation
		activation.RefreshTimer = true
		transaction.Activation = activation
		transaction.UpdatedAt = installer.now()
		return writeLifecycleJournal(home, paths, transaction)
	})
	if err != nil {
		return fail(fmt.Errorf("enable retained provider refresh: %w", err))
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return fail(err)
	}
	agentRestartedAfter := installer.now()
	if err := installer.systemctl(ctx, config.SystemctlPath, "start", config.AgentUnit); err != nil {
		return fail(fmt.Errorf("restart retained agent: %w", err))
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", agentRestartedAfter); err != nil {
		return fail(fmt.Errorf("verify retained agent remains fenced: %w", err))
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return fail(err)
	}
	if err := validateLifecycleWiringVector(transaction, paths, lifecycleWiringIntended); err != nil {
		return fail(err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReady, LifecycleCommit, installer.now()); err != nil {
		return fail(fmt.Errorf("prepare retained provider install commit: %w", err))
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReleasing, LifecycleCommit, installer.now()); err != nil {
		return fail(err)
	}
	_ = drainLifecycleAudit(home, paths, &transaction)
	maintenanceReleasedAfter := installer.now()
	if err := installer.releaseLifecycleMaintenance(ctx, home, transaction); err != nil {
		return Status{}, fmt.Errorf("release retained agent maintenance fence: %w", err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleCommitted, LifecycleCommit, installer.now()); err != nil {
		return Status{}, fmt.Errorf("commit retained provider install: %w", err)
	}
	if err := finalizeLifecycleTransaction(home, paths, &transaction, lifecycleRefresher); err != nil {
		return Status{}, err
	}
	if err := installer.waitLocalStateAfter(ctx, config, "idle", maintenanceReleasedAfter); err != nil {
		return Status{}, fmt.Errorf("wait for retained agent idle state: %w", err)
	}
	return status, nil
}

func validateInstalledConfigBinding(home string, requested Config, paths LifecyclePaths) error {
	if _, err := os.Lstat(paths.ConfigFile); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect installed retained provider config: %w", err)
	}
	installed, err := ReadConfigFile(paths.ConfigFile, home)
	if err != nil {
		return fmt.Errorf("read installed retained provider config: %w", err)
	}
	if !reflect.DeepEqual(installed, requested) {
		return errors.New("lifecycle config must exactly match the installed retained provider config")
	}
	return nil
}

func (installer Installer) Uninstall(ctx context.Context, home string, config Config, purge bool) (status Status, returnErr error) {
	if installer.Runner == nil {
		return Status{}, errors.New("command runner is required")
	}
	if err := config.Validate(home); err != nil {
		return Status{}, err
	}
	paths := LifecyclePathsFor(config)
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		return Status{}, fmt.Errorf("acquire retained provider install lock: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()
	lifecycleRefresher := Refresher{Runner: installer.Runner, Now: installer.Now, Sleep: installer.Sleep}
	if err := mkdirAllDurable(paths.Root, 0o700); err != nil {
		return Status{}, fmt.Errorf("create retained provider root: %w", err)
	}
	if err := validateInstallRoot(paths.Root); err != nil {
		return Status{}, err
	}
	if err := installer.recoverLifecycleTransaction(ctx, home, paths, lifecycleRefresher); err != nil {
		return Status{}, err
	}
	if err := validateInstalledConfigBinding(home, config, paths); err != nil {
		return Status{}, err
	}
	if err := installer.recoverInstallTransaction(ctx, config, paths, lifecycleRefresher); err != nil {
		return Status{}, err
	}
	if err := installer.runAgentSystemPreflight(ctx, config); err != nil {
		return Status{}, err
	}
	transaction, err := newLifecycleJournal(config, LifecycleUninstall, ProviderNotApplicable, &LifecycleUninstallPayload{Purge: purge}, installer.now())
	if err != nil {
		return Status{}, err
	}
	beforeSignature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return Status{}, err
	}
	transaction.Recovery.AgentUnitBefore = beforeSignature
	if err := startLifecycleTransaction(home, paths, &transaction); err != nil {
		return Status{}, err
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleFencing, "", installer.now()); err != nil {
		return Status{}, err
	}
	fail := func(cause error) (Status, error) {
		rollbackContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycleRecoveryTimeout)
		defer cancel()
		auditErr := writeLifecycleDiagnostic(home, paths, &transaction, AuditError, "operation_failed", installer.now())
		return Status{}, errors.Join(cause, auditErr, installer.recoverLifecycleTransaction(rollbackContext, home, paths, lifecycleRefresher))
	}
	maintenance, err := installer.beginMaintenance(ctx, config, transaction.TransactionID, uninstallMaintenanceReason)
	if err != nil {
		return fail(err)
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", maintenance.StartedAt); err != nil {
		return Status{}, fmt.Errorf("wait for retained agent maintenance fence: %w", err)
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return Status{}, err
	}
	if err := snapshotManagedFilesForLifecycle(home, paths, &transaction, installer.now()); err != nil {
		return fail(fmt.Errorf("snapshot retained provider wiring: %w", err))
	}
	previousUnits, err := installer.captureManagedUnitStates(ctx, config.SystemctlPath)
	if err != nil {
		return fail(fmt.Errorf("snapshot retained provider unit state: %w", err))
	}
	transaction.WiringIntent = managedWiringIntent(paths, SystemdUnits{}, false)
	intendedSignature, err := deriveLifecycleAgentUnitSignature(beforeSignature, paths, transaction.WiringIntent, nil)
	if err != nil {
		return fail(err)
	}
	transaction.AgentUnitIntended = &intendedSignature
	transaction.PreviousUnits = previousUnits
	transaction.UpdatedAt = installer.now()
	if err := writeLifecycleJournal(home, paths, transaction); err != nil {
		return fail(fmt.Errorf("write retained provider uninstall recovery state: %w", err))
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleFenced, "", installer.now()); err != nil {
		return fail(err)
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "stop", config.AgentUnit); err != nil {
		return fail(fmt.Errorf("stop retained agent: %w", err))
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "disable", "--now", refreshPathUnit, refreshTimerUnit, providerServiceUnit); err != nil {
		return fail(fmt.Errorf("disable retained provider wiring: %w", err))
	}
	for _, path := range managedWiringPaths(paths) {
		if err := removeDurableFile(path); err != nil {
			return fail(fmt.Errorf("remove retained provider wiring: %w", err))
		}
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "daemon-reload"); err != nil {
		return fail(fmt.Errorf("reload user systemd: %w", err))
	}
	loadedSignature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return fail(err)
	}
	if !equalLifecycleSystemdSignature(loadedSignature, intendedSignature) {
		return fail(errors.New("reloaded retained agent unit does not match intended signature"))
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return fail(err)
	}
	agentRestartedAfter := installer.now()
	if err := installer.systemctl(ctx, config.SystemctlPath, "start", config.AgentUnit); err != nil {
		return fail(fmt.Errorf("restart retained agent: %w", err))
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", agentRestartedAfter); err != nil {
		return fail(fmt.Errorf("verify retained agent remains fenced: %w", err))
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return fail(err)
	}
	if err := validateLifecycleWiringVector(transaction, paths, lifecycleWiringIntended); err != nil {
		return fail(err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReady, LifecycleCommit, installer.now()); err != nil {
		return fail(fmt.Errorf("prepare retained provider uninstall commit: %w", err))
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReleasing, LifecycleCommit, installer.now()); err != nil {
		return fail(err)
	}
	_ = drainLifecycleAudit(home, paths, &transaction)
	maintenanceReleasedAfter := installer.now()
	if err := installer.releaseLifecycleMaintenance(ctx, home, transaction); err != nil {
		return Status{}, fmt.Errorf("release retained agent maintenance fence: %w", err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleCommitted, LifecycleCommit, installer.now()); err != nil {
		return Status{}, fmt.Errorf("commit retained provider uninstall: %w", err)
	}
	if err := installer.waitLocalStateAfter(ctx, config, "idle", maintenanceReleasedAfter); err != nil {
		return Status{}, fmt.Errorf("wait for retained agent idle state: %w", err)
	}
	if err := finalizeLifecycleTransaction(home, paths, &transaction, lifecycleRefresher); err != nil {
		return Status{}, err
	}
	return Status{ProtocolVersion: StatusProtocolVersion, ObservedAt: installer.now()}, nil
}

func (installer Installer) Recover(ctx context.Context, home string, config Config, confirmation string) (status Status, returnErr error) {
	if installer.Runner == nil {
		return Status{}, errors.New("command runner is required")
	}
	if err := config.Validate(home); err != nil {
		return Status{}, err
	}
	if !safeIdentifierPattern.MatchString(confirmation) {
		return Status{}, errors.New("exact legacy provider transaction confirmation is required")
	}
	paths := LifecyclePathsFor(config)
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		return Status{}, fmt.Errorf("acquire retained provider install lock: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()
	if err := validateInstallRoot(paths.Root); err != nil {
		return Status{}, err
	}
	if _, found, err := readLifecycleJournal(home, paths); err != nil {
		return Status{}, err
	} else if found {
		return Status{}, errors.New("explicit legacy recovery refuses an existing outer lifecycle transaction")
	}
	inner, found, err := readTransactionJournal(paths.Journal)
	if err != nil {
		return Status{}, err
	}
	if !found {
		return Status{}, errors.New("no legacy provider transaction requires recovery")
	}
	if inner.ID != confirmation {
		return Status{}, errors.New("legacy provider transaction confirmation does not match")
	}
	refresher := Refresher{Runner: installer.Runner, Now: installer.Now, Sleep: installer.Sleep}
	if err := installer.adoptLegacyProviderTransaction(ctx, home, paths, refresher, &config, confirmation); err != nil {
		return Status{}, err
	}
	return installer.Status(ctx, home, config)
}

func (installer Installer) Status(ctx context.Context, home string, config Config) (Status, error) {
	if installer.Runner == nil {
		return Status{}, errors.New("command runner is required")
	}
	if err := config.Validate(home); err != nil {
		return Status{}, err
	}
	paths := LifecyclePathsFor(config)
	active, found, err := readActiveStateForConfig(paths.ActiveState, config)
	if err != nil {
		return Status{}, err
	}
	if !found {
		return Status{ProtocolVersion: StatusProtocolVersion, ObservedAt: installer.now()}, nil
	}
	unitInfo, err := os.Lstat(paths.ProviderUnit)
	if errors.Is(err, os.ErrNotExist) {
		return Status{ProtocolVersion: StatusProtocolVersion, ObservedAt: installer.now()}, nil
	}
	if err != nil || !unitInfo.Mode().IsRegular() {
		return Status{}, errors.New("retained provider service unit must be a regular file")
	}
	if err := validateOwner(unitInfo); err != nil {
		return Status{}, fmt.Errorf("retained provider service unit: %w", err)
	}
	if _, err := hashHostExecutable(config.SystemctlPath); err != nil {
		return Status{}, fmt.Errorf("validate systemctl authority: %w", err)
	}
	output, err := installer.run(ctx, Command{Path: config.SystemctlPath, Args: []string{
		"--user", "show", providerServiceUnit, "--property", "ActiveState", "--value",
	}})
	if err != nil {
		return Status{}, fmt.Errorf("inspect retained provider service: %w", err)
	}
	return statusForActive(active, strings.TrimSpace(string(output)) == "active", installer.now()), nil
}

func (installer Installer) preflightInstall(ctx context.Context, config Config, paths LifecyclePaths, credentials Credentials) (VerifiedUpdate, string, InstallMaterial, SystemdUnits, error) {
	if err := installer.runSystemPreflight(ctx, config); err != nil {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, err
	}
	update, err := VerifyCurrentUpdate(ctx, config, installer.Runner)
	if err != nil {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, err
	}
	executablePath := installer.ExecutablePath
	if executablePath == nil {
		executablePath = os.Executable
	}
	executable, err := executablePath()
	if err != nil {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, fmt.Errorf("resolve installer executable: %w", err)
	}
	digest, err := hashRegularFile(executable, true)
	if err != nil {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, fmt.Errorf("hash installer executable: %w", err)
	}
	if digest != update.SHA256 {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, errors.New("installer digest does not match verified provider update")
	}
	material, err := GenerateInstallMaterial(config, credentials, installer.Random, installer.now())
	if err != nil {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, err
	}
	units, err := RenderSystemdUnits(config, paths)
	if err != nil {
		return VerifiedUpdate{}, "", InstallMaterial{}, SystemdUnits{}, err
	}
	return update, executable, material, units, nil
}

func (installer Installer) runSystemPreflight(ctx context.Context, config Config) error {
	if err := validateConfiguredHostExecutables(config); err != nil {
		return err
	}
	uid, err := installer.currentUserID()
	if err != nil {
		return fmt.Errorf("resolve retained provider user identity: %w", err)
	}
	if uid == "0" {
		return errors.New("retained provider install requires a non-root user")
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "show-environment"); err != nil {
		return fmt.Errorf("user systemd preflight: %w", err)
	}
	linger, err := installer.run(ctx, Command{Path: config.LoginctlPath, Args: []string{
		"show-user", uid, "--property", "Linger", "--value",
	}})
	if err != nil {
		return fmt.Errorf("user systemd lingering preflight: %w", err)
	}
	if strings.TrimSpace(string(linger)) != "yes" {
		return errors.New("user systemd lingering must be enabled")
	}
	if _, err := installer.run(ctx, Command{Path: config.PodmanPath, Args: []string{"version", "--format", "{{.Client.Version}}"}}); err != nil {
		return fmt.Errorf("rootless Podman preflight: %w", err)
	}
	rootless, err := installer.run(ctx, Command{Path: config.PodmanPath, Args: []string{
		"info", "--format", "{{.Host.Security.Rootless}}",
	}})
	if err != nil {
		return fmt.Errorf("inspect rootless Podman preflight: %w", err)
	}
	if strings.TrimSpace(string(rootless)) != "true" {
		return errors.New("podman must report a rootless runtime")
	}
	return installer.runSupervisorConfigPreflight(ctx, config)
}

func (installer Installer) currentUserID() (string, error) {
	if installer.UserID != nil {
		return installer.UserID()
	}
	current, err := user.Current()
	if err != nil {
		return "", err
	}
	if _, err := strconv.ParseUint(current.Uid, 10, 32); err != nil {
		return "", errors.New("current user has no numeric UID")
	}
	return current.Uid, nil
}

func (installer Installer) runAgentSystemPreflight(ctx context.Context, config Config) error {
	if err := validateConfiguredHostExecutables(config); err != nil {
		return err
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "show-environment"); err != nil {
		return fmt.Errorf("user systemd preflight: %w", err)
	}
	return installer.runSupervisorConfigPreflight(ctx, config)
}

func validateConfiguredHostExecutables(config Config) error {
	for _, executable := range []struct {
		label string
		path  string
	}{
		{label: "podman", path: config.PodmanPath},
		{label: "systemctl", path: config.SystemctlPath},
		{label: "loginctl", path: config.LoginctlPath},
	} {
		if _, err := hashHostExecutable(executable.path); err != nil {
			return fmt.Errorf("validate %s authority: %w", executable.label, err)
		}
	}
	return nil
}

func (installer Installer) runSupervisorConfigPreflight(ctx context.Context, config Config) error {
	if _, err := installer.run(ctx, Command{Path: config.ComputeAgentPath, Args: []string{
		"supervisor-config", "validate", "-path", config.SupervisorConfigPath, "-format", "auto",
	}}); err != nil {
		return fmt.Errorf("supervisor config preflight: %w", err)
	}
	return nil
}

func (installer Installer) ensureProviderNetwork(ctx context.Context, config Config) error {
	if _, err := installer.run(ctx, Command{Path: config.PodmanPath, Args: []string{
		"network", "create", "--driver", "bridge", "--ignore", config.ContainerNetwork,
	}}); err != nil {
		return fmt.Errorf("create provider network: %w", err)
	}
	refresher := Refresher{Runner: installer.Runner}
	if err := refresher.validateProviderNetwork(ctx, config); err != nil {
		return err
	}
	return nil
}

func (installer Installer) beginMaintenance(ctx context.Context, config Config, id, reason string) (maintenanceRecord, error) {
	state, err := installer.maintenanceCommand(ctx, config, "begin", id, reason)
	if err != nil {
		return maintenanceRecord{}, err
	}
	if err := validateMaintenanceState(state, true, config.ProfileID, id, reason); err != nil {
		return maintenanceRecord{}, err
	}
	return *state.Maintenance, nil
}

func (installer Installer) endMaintenance(ctx context.Context, config Config, id, reason string) error {
	state, err := installer.maintenanceCommand(ctx, config, "end", id, "")
	if err != nil {
		return err
	}
	return validateMaintenanceState(state, false, config.ProfileID, id, reason)
}

func (installer Installer) releaseLifecycleMaintenance(ctx context.Context, home string, journal LifecycleJournal) error {
	if err := installer.reattestLifecycleAuthority(ctx, home, journal); err != nil {
		return err
	}
	id, reason, err := lifecycleMaintenanceIdentity(journal)
	if err != nil {
		return err
	}
	return installer.endMaintenance(ctx, journal.Recovery.Config, id, reason)
}

func (installer Installer) maintenanceStatus(ctx context.Context, config Config) (maintenanceState, error) {
	state, err := installer.maintenanceCommand(ctx, config, "status", "", "")
	if err != nil {
		return maintenanceState{}, err
	}
	if !state.Durable {
		return maintenanceState{}, errors.New("supervisor maintenance status is not durable")
	}
	if !state.Active {
		if state.Maintenance != nil {
			return maintenanceState{}, errors.New("inactive supervisor maintenance status contains a marker")
		}
		return state, nil
	}
	if state.Maintenance == nil {
		return maintenanceState{}, errors.New("active supervisor maintenance status is missing its marker")
	}
	maintenance := state.Maintenance
	if maintenance.Kind != maintenanceMarkerKind || !safeIdentifierPattern.MatchString(maintenance.ID) ||
		!safeIdentifierPattern.MatchString(maintenance.ProfileID) || !safeIdentifierPattern.MatchString(maintenance.Reason) || maintenance.StartedAt.IsZero() {
		return maintenanceState{}, errors.New("supervisor maintenance status contains an invalid marker")
	}
	return state, nil
}

func classifyMaintenanceState(state maintenanceState, profileID, id, reason string) maintenanceDisposition {
	if !state.Active {
		return maintenanceInactive
	}
	maintenance := state.Maintenance
	if maintenance != nil && maintenance.Kind == maintenanceMarkerKind && maintenance.ID == id && maintenance.ProfileID == profileID && maintenance.Reason == reason {
		return maintenanceExactActive
	}
	return maintenanceConflicting
}

func (installer Installer) maintenanceCommand(ctx context.Context, config Config, operation, id, reason string) (maintenanceState, error) {
	arguments := []string{
		"supervisor-maintenance", operation,
		"-config", config.SupervisorConfigPath,
		"-format", "auto",
		"-profile", config.ProfileID,
	}
	if id != "" {
		arguments = append(arguments, "-id", id)
	}
	if reason != "" {
		arguments = append(arguments, "-reason", reason)
	}
	output, err := installer.run(ctx, Command{Path: config.ComputeAgentPath, Args: arguments})
	if err != nil {
		return maintenanceState{}, err
	}
	var state maintenanceState
	if err := decodeStrictJSON(bytes.NewReader(output), &state); err != nil {
		return maintenanceState{}, fmt.Errorf("decode supervisor maintenance state: %w", err)
	}
	return state, nil
}

func validateMaintenanceState(state maintenanceState, active bool, profileID, id, reason string) error {
	if state.Active != active || !state.Durable || state.Maintenance == nil {
		return errors.New("supervisor maintenance command did not return the required durable state")
	}
	maintenance := state.Maintenance
	if maintenance.Kind != maintenanceMarkerKind || maintenance.ID != id || maintenance.ProfileID != profileID || maintenance.Reason != reason || maintenance.StartedAt.IsZero() {
		return errors.New("supervisor maintenance command returned a mismatched maintenance fence")
	}
	return nil
}

func (installer Installer) waitLocalStateAfter(ctx context.Context, config Config, expected string, observedAfter time.Time) error {
	if observedAfter.IsZero() {
		return errors.New("local agent observation boundary is required")
	}
	for attempt := 0; attempt < localStatusAttempts; attempt++ {
		output, err := installer.run(ctx, Command{Path: config.ComputeAgentPath, Args: []string{
			"local-status", "sanitize", "-path", config.LocalStatusPath,
		}})
		if err != nil {
			return err
		}
		var status localAgentStatus
		if err := decodeStrictJSON(bytes.NewReader(output), &status); err != nil {
			return fmt.Errorf("decode local agent status: %w", err)
		}
		if status.ProtocolVersion != localStatusProtocolVersion || status.WorkerID != config.WorkerID || status.UpdatedAt.IsZero() {
			return errors.New("local agent status identity or protocol mismatch")
		}
		if status.UpdatedAt.After(observedAfter) && status.State == expected && status.TaskID == "" && status.LeaseID == "" {
			return nil
		}
		if attempt+1 < localStatusAttempts {
			if err := installer.sleep(ctx, time.Second); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("local agent did not reach %s without an active task or lease", expected)
}

func (installer Installer) waitLocalDrainedAfter(ctx context.Context, config Config, observedAfter time.Time) error {
	if observedAfter.IsZero() {
		return errors.New("local agent drain boundary is required")
	}
	for attempt := 0; attempt < localStatusAttempts; attempt++ {
		output, err := installer.run(ctx, Command{Path: config.ComputeAgentPath, Args: []string{
			"local-status", "sanitize", "-path", config.LocalStatusPath,
		}})
		if err != nil {
			return err
		}
		var status localAgentStatus
		if err := decodeStrictJSON(bytes.NewReader(output), &status); err != nil {
			return fmt.Errorf("decode local agent status: %w", err)
		}
		if status.ProtocolVersion != localStatusProtocolVersion || status.WorkerID != config.WorkerID || status.UpdatedAt.IsZero() {
			return errors.New("local agent status identity or protocol mismatch")
		}
		if status.UpdatedAt.After(observedAfter) && status.TaskID == "" && status.LeaseID == "" {
			return nil
		}
		if attempt+1 < localStatusAttempts {
			if err := installer.sleep(ctx, time.Second); err != nil {
				return err
			}
		}
	}
	return errors.New("local agent remained assigned to a task or lease")
}

func (installer Installer) systemctl(ctx context.Context, path string, args ...string) error {
	arguments := append([]string{"--user"}, args...)
	_, err := installer.run(ctx, Command{Path: path, Args: arguments})
	return err
}

func (installer Installer) enableWatchUnitBefore(ctx context.Context, systemctlPath, unit string, beforeMutation func() error) (bool, error) {
	if beforeMutation != nil {
		if err := beforeMutation(); err != nil {
			return false, err
		}
	}
	if err := installer.systemctl(ctx, systemctlPath, "enable", "--now", unit); err == nil {
		return true, nil
	} else {
		activated, inspectErr := installer.inspectUnitActivation(ctx, systemctlPath, unit)
		if inspectErr != nil {
			return true, errors.Join(err, fmt.Errorf("inspect failed unit activation: %w", inspectErr))
		}
		return activated, err
	}
}

func (installer Installer) inspectUnitActivation(ctx context.Context, systemctlPath, unit string) (bool, error) {
	state, err := installer.inspectUnitState(ctx, systemctlPath, unit)
	if err != nil {
		return false, err
	}
	return state.ActiveState != "inactive" || state.UnitFileState != "disabled", nil
}

func (installer Installer) inspectAgentUnitSignature(ctx context.Context, home string, config Config) (LifecycleSystemdSignature, error) {
	output, err := installer.run(ctx, Command{Path: config.SystemctlPath, Args: []string{
		"--user", "show", config.AgentUnit,
		"--property", "LoadState", "--property", "FragmentPath", "--property", "DropInPaths",
	}})
	if err != nil {
		return LifecycleSystemdSignature{}, fmt.Errorf("inspect retained agent effective unit: %w", err)
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSuffix(string(output), "\n"), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || (key != "LoadState" && key != "FragmentPath" && key != "DropInPaths") {
			return LifecycleSystemdSignature{}, errors.New("effective agent systemd state is malformed")
		}
		if _, duplicate := properties[key]; duplicate {
			return LifecycleSystemdSignature{}, errors.New("effective agent systemd state contains a duplicate property")
		}
		properties[key] = value
	}
	if len(properties) != 3 || properties["LoadState"] != "loaded" {
		return LifecycleSystemdSignature{}, errors.New("effective agent systemd state is incomplete or unloaded")
	}
	fragmentPath, err := decodeSystemdPath(properties["FragmentPath"])
	if err != nil {
		return LifecycleSystemdSignature{}, fmt.Errorf("decode effective agent fragment: %w", err)
	}
	dropInPaths, err := decodeSystemdPathList(properties["DropInPaths"], false)
	if err != nil {
		return LifecycleSystemdSignature{}, fmt.Errorf("decode effective agent drop-ins: %w", err)
	}
	fragment, fragmentContents, err := readAndAttestLifecycleSystemdPath(home, fragmentPath)
	if err != nil {
		return LifecycleSystemdSignature{}, err
	}
	signature := LifecycleSystemdSignature{Fragment: fragment}
	unitContents := [][]byte{fragmentContents}
	for _, path := range dropInPaths {
		attestation, contents, err := readAndAttestLifecycleSystemdPath(home, path)
		if err != nil {
			return LifecycleSystemdSignature{}, err
		}
		signature.DropIns = append(signature.DropIns, attestation)
		unitContents = append(unitContents, contents)
	}
	environmentPaths, err := effectiveSystemdEnvironmentFiles(unitContents)
	if err != nil {
		return LifecycleSystemdSignature{}, fmt.Errorf("decode effective agent environment files: %w", err)
	}
	signature.ExecStart, err = effectiveSystemdExecStart(unitContents)
	if err != nil {
		return LifecycleSystemdSignature{}, fmt.Errorf("decode effective agent ExecStart: %w", err)
	}
	for _, path := range environmentPaths {
		attestation, err := attestLifecycleSystemdPath(home, path)
		if err != nil {
			return LifecycleSystemdSignature{}, err
		}
		signature.EnvironmentFiles = append(signature.EnvironmentFiles, attestation)
	}
	sort.Slice(signature.EnvironmentFiles, func(left, right int) bool {
		return signature.EnvironmentFiles[left].Path < signature.EnvironmentFiles[right].Path
	})
	if err := signature.Validate(home); err != nil {
		return LifecycleSystemdSignature{}, err
	}
	return signature, nil
}

func effectiveSystemdExecStart(unitContents [][]byte) (string, error) {
	var commands []string
	for _, contents := range unitContents {
		options, err := systemdunit.DeserializeOptions(bytes.NewReader(contents))
		if err != nil {
			return "", fmt.Errorf("parse systemd unit: %w", err)
		}
		for _, option := range options {
			if option.Section != "Service" || option.Name != "ExecStart" {
				continue
			}
			if strings.TrimSpace(option.Value) == "" {
				commands = nil
				continue
			}
			if len(option.Value) > 16*1024 || containsControl(option.Value) {
				return "", errors.New("systemd ExecStart contains an invalid value")
			}
			commands = append(commands, option.Value)
			if len(commands) > 64 {
				return "", errors.New("systemd unit contains too many ExecStart commands")
			}
		}
	}
	if len(commands) == 0 {
		return "", errors.New("systemd unit has no ExecStart command")
	}
	encoded, err := json.Marshal(commands)
	if err != nil {
		return "", fmt.Errorf("encode static ExecStart: %w", err)
	}
	return string(encoded), nil
}

func effectiveSystemdEnvironmentFiles(unitContents [][]byte) ([]string, error) {
	var paths []string
	for _, contents := range unitContents {
		options, err := systemdunit.DeserializeOptions(bytes.NewReader(contents))
		if err != nil {
			return nil, fmt.Errorf("parse systemd unit: %w", err)
		}
		for _, option := range options {
			if option.Section != "Service" || option.Name != "EnvironmentFile" {
				continue
			}
			if strings.TrimSpace(option.Value) == "" {
				paths = nil
				continue
			}
			values, err := splitSystemdPathWords(option.Value)
			if err != nil {
				return nil, err
			}
			paths = append(paths, values...)
			if len(paths) > 64 {
				return nil, errors.New("systemd unit references too many environment files")
			}
		}
	}
	return paths, nil
}

func splitSystemdPathWords(value string) ([]string, error) {
	var words []string
	var word strings.Builder
	var quote byte
	started := false
	flush := func() {
		if started {
			words = append(words, word.String())
			word.Reset()
			started = false
		}
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if quote == 0 && (character == ' ' || character == '\t') {
			flush()
			continue
		}
		if character == '\'' || character == '"' {
			if quote == 0 {
				quote = character
				started = true
				continue
			}
			if quote == character {
				quote = 0
				continue
			}
		}
		if character == '\\' {
			decoded, consumed, err := decodeSystemdEscape(value[index:])
			if err != nil {
				return nil, err
			}
			word.WriteByte(decoded)
			started = true
			index += consumed - 1
			continue
		}
		if character < 0x20 || character == 0x7f {
			return nil, errors.New("systemd environment file path contains control bytes")
		}
		word.WriteByte(character)
		started = true
	}
	if quote != 0 {
		return nil, errors.New("systemd environment file path has an unterminated quote")
	}
	flush()
	if len(words) == 0 {
		return nil, errors.New("systemd EnvironmentFile directive is empty")
	}
	for index, word := range words {
		if strings.HasPrefix(word, "-") || strings.ContainsAny(word, "*?[") {
			return nil, errors.New("optional or globbed systemd environment files are unsupported")
		}
		expanded, err := decodeLiteralSystemdPercents(word)
		if err != nil {
			return nil, err
		}
		if !filepath.IsAbs(expanded) || containsControl(expanded) {
			return nil, errors.New("systemd environment file path must be absolute")
		}
		words[index] = expanded
	}
	return words, nil
}

func decodeSystemdEscape(value string) (byte, int, error) {
	if len(value) < 2 {
		return 0, 0, errors.New("systemd environment file path has a trailing escape")
	}
	switch value[1] {
	case '\\', '\'', '"', ' ':
		return value[1], 2, nil
	case 'x':
		if len(value) < 4 {
			return 0, 0, errors.New("systemd environment file path has a short hexadecimal escape")
		}
		high, okHigh := fromHex(value[2])
		low, okLow := fromHex(value[3])
		if !okHigh || !okLow || high<<4|low == 0 {
			return 0, 0, errors.New("systemd environment file path has an invalid hexadecimal escape")
		}
		return high<<4 | low, 4, nil
	default:
		return 0, 0, errors.New("systemd environment file path has an unsupported escape")
	}
}

func decodeLiteralSystemdPercents(value string) (string, error) {
	var decoded strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] != '%' {
			decoded.WriteByte(value[index])
			continue
		}
		if index+1 >= len(value) || value[index+1] != '%' {
			return "", errors.New("systemd environment file path contains an unsupported specifier")
		}
		decoded.WriteByte('%')
		index++
	}
	return decoded.String(), nil
}

func decodeSystemdPathList(value string, environment bool) ([]string, error) {
	if value == "" {
		return nil, nil
	}
	fields := strings.Fields(value)
	paths := make([]string, 0, len(fields))
	for _, field := range fields {
		if environment && strings.HasPrefix(field, "(ignore_errors=") && strings.HasSuffix(field, ")") {
			continue
		}
		path, err := decodeSystemdPath(field)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func decodeSystemdPath(value string) (string, error) {
	if value == "" || value[0] != '/' || containsControl(value) {
		return "", errors.New("systemd path is empty, relative, or contains control bytes")
	}
	var decoded strings.Builder
	for index := 0; index < len(value); index++ {
		if value[index] != '\\' {
			decoded.WriteByte(value[index])
			continue
		}
		if index+1 < len(value) && value[index+1] == '\\' {
			decoded.WriteByte('\\')
			index++
			continue
		}
		if index+3 >= len(value) || value[index+1] != 'x' {
			return "", errors.New("systemd path contains an unsupported escape")
		}
		high, okHigh := fromHex(value[index+2])
		low, okLow := fromHex(value[index+3])
		if !okHigh || !okLow {
			return "", errors.New("systemd path contains an invalid hexadecimal escape")
		}
		decoded.WriteByte(high<<4 | low)
		index += 3
	}
	path := decoded.String()
	if !filepath.IsAbs(path) || containsControl(path) {
		return "", errors.New("decoded systemd path is invalid")
	}
	return path, nil
}

func fromHex(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func attestLifecycleSystemdPath(home, path string) (LifecycleFileAttestation, error) {
	attestation, _, err := readAndAttestLifecycleSystemdPath(home, path)
	return attestation, err
}

func readAndAttestLifecycleSystemdPath(home, path string) (LifecycleFileAttestation, []byte, error) {
	if err := ValidateUserPath(home, path, true); err != nil {
		return LifecycleFileAttestation{}, nil, fmt.Errorf("validate effective agent systemd path: %w", err)
	}
	entry, err := os.Lstat(path)
	if err != nil || !entry.Mode().IsRegular() || entry.Size() > MaxStateFileBytes {
		return LifecycleFileAttestation{}, nil, errors.New("effective agent systemd input must be a regular file of at most 1 MiB")
	}
	if err := validateOwner(entry); err != nil {
		return LifecycleFileAttestation{}, nil, fmt.Errorf("effective agent systemd input ownership: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return LifecycleFileAttestation{}, nil, fmt.Errorf("open effective agent systemd input: %w", err)
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(entry, opened) || opened.Size() > MaxStateFileBytes {
		return LifecycleFileAttestation{}, nil, errors.New("effective agent systemd input changed during open")
	}
	contents, err := io.ReadAll(io.LimitReader(file, MaxStateFileBytes+1))
	if err != nil {
		return LifecycleFileAttestation{}, nil, fmt.Errorf("read effective agent systemd input: %w", err)
	}
	if len(contents) > MaxStateFileBytes {
		return LifecycleFileAttestation{}, nil, errors.New("effective agent systemd input exceeds 1 MiB")
	}
	digest := digestBytes(contents)
	attestation := LifecycleFileAttestation{Path: path, SHA256: digest}
	if err := reattestLifecycleFile("input", attestation); err != nil {
		return LifecycleFileAttestation{}, nil, err
	}
	return attestation, contents, nil
}

func (installer Installer) inspectUnitState(ctx context.Context, systemctlPath, unit string) (systemdUnitState, error) {
	output, err := installer.run(ctx, Command{Path: systemctlPath, Args: []string{
		"--user", "show", unit,
		"--property", "LoadState", "--property", "FragmentPath",
		"--property", "ActiveState", "--property", "UnitFileState",
	}})
	if err != nil {
		return systemdUnitState{}, err
	}
	properties := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found || (key != "LoadState" && key != "FragmentPath" && key != "ActiveState" && key != "UnitFileState") {
			return systemdUnitState{}, errors.New("systemd unit activation state is malformed")
		}
		if _, duplicate := properties[key]; duplicate {
			return systemdUnitState{}, errors.New("systemd unit activation state contains duplicate properties")
		}
		properties[key] = value
	}
	if len(properties) != 4 || properties["LoadState"] == "" || properties["ActiveState"] == "" {
		return systemdUnitState{}, errors.New("systemd unit activation state is incomplete")
	}
	return systemdUnitState{
		LoadState: properties["LoadState"], FragmentPath: properties["FragmentPath"],
		ActiveState: properties["ActiveState"], UnitFileState: properties["UnitFileState"],
	}, nil
}

func (installer Installer) captureManagedUnitStates(ctx context.Context, systemctlPath string) (map[string]systemdUnitState, error) {
	states := map[string]systemdUnitState{}
	for _, unit := range []string{providerServiceUnit, refreshPathUnit, refreshTimerUnit} {
		state, err := installer.inspectUnitState(ctx, systemctlPath, unit)
		if err != nil {
			return nil, fmt.Errorf("inspect %s: %w", unit, err)
		}
		if state.LoadState == "not-found" {
			continue
		}
		if err := validateRestorableUnitState(state); err != nil {
			return nil, fmt.Errorf("inspect %s: %w", unit, err)
		}
		states[unit] = state
	}
	return states, nil
}

func (installer Installer) restoreUnitState(ctx context.Context, systemctlPath, unit string, state systemdUnitState) error {
	if err := validateRestorableUnitState(state); err != nil {
		return err
	}
	var restoreErr error
	switch state.UnitFileState {
	case "enabled":
		restoreErr = errors.Join(restoreErr, installer.systemctl(ctx, systemctlPath, "enable", unit))
	case "enabled-runtime":
		restoreErr = errors.Join(restoreErr, installer.systemctl(ctx, systemctlPath, "enable", "--runtime", unit))
	case "disabled":
		restoreErr = errors.Join(restoreErr, installer.systemctl(ctx, systemctlPath, "disable", unit))
	case "static", "indirect", "generated", "transient":
	}
	switch state.ActiveState {
	case "active":
		restoreErr = errors.Join(restoreErr, installer.systemctl(ctx, systemctlPath, "start", unit))
	case "inactive":
		restoreErr = errors.Join(restoreErr, installer.systemctl(ctx, systemctlPath, "stop", unit))
	}
	return restoreErr
}

func validateRestorableUnitState(state systemdUnitState) error {
	if state.LoadState != "loaded" {
		return fmt.Errorf("unsupported prior LoadState %q", state.LoadState)
	}
	if !filepath.IsAbs(state.FragmentPath) || containsControl(state.FragmentPath) {
		return fmt.Errorf("unsupported prior FragmentPath %q", state.FragmentPath)
	}
	switch state.UnitFileState {
	case "enabled", "enabled-runtime", "disabled", "static", "indirect", "generated", "transient":
	default:
		return fmt.Errorf("unsupported prior UnitFileState %q", state.UnitFileState)
	}
	switch state.ActiveState {
	case "active", "inactive":
	default:
		return fmt.Errorf("unsupported prior ActiveState %q", state.ActiveState)
	}
	return nil
}

func (installer Installer) rollbackInstall(ctx context.Context, config Config, snapshots []managedFileSnapshot, previousUnits map[string]systemdUnitState, agentStopped, maintenanceActive bool, maintenanceID, maintenanceReason string, activation systemdActivation) error {
	if err := installer.rollbackInstallBeforeStart(ctx, config, snapshots, previousUnits, agentStopped, maintenanceActive, maintenanceID, maintenanceReason, activation, nil); err != nil {
		return err
	}
	return removeSnapshots(snapshots)
}

func (installer Installer) rollbackInstallBeforeStart(ctx context.Context, config Config, snapshots []managedFileSnapshot, previousUnits map[string]systemdUnitState, agentStopped, maintenanceActive bool, maintenanceID, maintenanceReason string, activation systemdActivation, beforeStart func(context.Context) error) error {
	rollbackContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), installRollbackTimeout)
	defer cancel()
	if !agentStopped {
		if maintenanceActive {
			return installer.endMaintenance(rollbackContext, config, maintenanceID, maintenanceReason)
		}
		return nil
	}
	var rollbackErr error
	activatedUnits := make([]string, 0, 3)
	if activation.RefreshPath {
		activatedUnits = append(activatedUnits, refreshPathUnit)
	}
	if activation.RefreshTimer {
		activatedUnits = append(activatedUnits, refreshTimerUnit)
	}
	if activation.ProviderService {
		activatedUnits = append(activatedUnits, providerServiceUnit)
	}
	if len(activatedUnits) > 0 {
		arguments := append([]string{"disable", "--now"}, activatedUnits...)
		rollbackErr = errors.Join(rollbackErr, installer.systemctl(rollbackContext, config.SystemctlPath, arguments...))
	}
	if err := restoreManagedFileContents(snapshots); err != nil {
		rollbackErr = errors.Join(rollbackErr, err)
	} else {
		rollbackErr = errors.Join(rollbackErr, installer.systemctl(rollbackContext, config.SystemctlPath, "daemon-reload"))
		for _, unit := range []string{providerServiceUnit, refreshPathUnit, refreshTimerUnit} {
			if state, found := previousUnits[unit]; found {
				rollbackErr = errors.Join(rollbackErr, installer.restoreUnitState(rollbackContext, config.SystemctlPath, unit, state))
			}
		}
	}
	if beforeStart == nil {
		rollbackErr = errors.Join(rollbackErr, installer.systemctl(rollbackContext, config.SystemctlPath, "start", config.AgentUnit))
	} else if rollbackErr == nil {
		if err := beforeStart(rollbackContext); err != nil {
			rollbackErr = err
		} else {
			rollbackErr = installer.systemctl(rollbackContext, config.SystemctlPath, "start", config.AgentUnit)
		}
	}
	if rollbackErr == nil && maintenanceActive {
		rollbackErr = installer.endMaintenance(rollbackContext, config, maintenanceID, maintenanceReason)
	}
	return rollbackErr
}

func writeInstalledProvider(config Config, paths LifecyclePaths, executable, digest string, material InstallMaterial, units SystemdUnits) error {
	if err := AtomicWriteJSON(paths.ConfigFile, config); err != nil {
		return fmt.Errorf("write retained provider config: %w", err)
	}
	if err := replaceRegularFile(executable, paths.Launcher, 0o700, maxProviderPackageBytes); err != nil {
		return fmt.Errorf("install retained provider launcher: %w", err)
	}
	if installedDigest, err := hashRegularFile(paths.Launcher, true); err != nil || installedDigest != digest {
		return errors.New("installed retained provider launcher digest mismatch")
	}
	if err := WriteInstallMaterial(paths, material); err != nil {
		return err
	}
	if err := mkdirAllDurable(paths.ProviderState, 0o700); err != nil {
		return fmt.Errorf("create provider state directory: %w", err)
	}
	for _, file := range []struct {
		path string
		data string
	}{
		{paths.ProviderUnit, units.ProviderService},
		{paths.RefreshUnit, units.RefreshService},
		{paths.PathUnit, units.RefreshPath},
		{paths.TimerUnit, units.RefreshTimer},
		{paths.AgentDropIn, units.AgentDropIn},
	} {
		if err := atomicWriteFile(file.path, []byte(file.data), 0o600); err != nil {
			return fmt.Errorf("write user systemd unit: %w", err)
		}
	}
	return nil
}

func managedInstallPaths(paths LifecyclePaths) []string {
	return append([]string{
		paths.ConfigFile, paths.Launcher,
		paths.ActiveState, paths.Journal,
		paths.ProviderEnv, paths.ProbeEnv, paths.AgentEnv, paths.CAKey,
		paths.ContainersConf,
		paths.CAFile, paths.ServerCert, paths.ServerKey,
	}, managedWiringPaths(paths)...)
}

func previouslyLoadedWatchUnits(states map[string]systemdUnitState) []string {
	units := make([]string, 0, 2)
	if _, found := states[refreshPathUnit]; found {
		units = append(units, refreshPathUnit)
	}
	if _, found := states[refreshTimerUnit]; found {
		units = append(units, refreshTimerUnit)
	}
	return units
}

func managedWiringPaths(paths LifecyclePaths) []string {
	return []string{paths.ProviderUnit, paths.RefreshUnit, paths.PathUnit, paths.TimerUnit, paths.AgentDropIn}
}

func managedWiringIntent(paths LifecyclePaths, units SystemdUnits, present bool) []LifecycleManagedFileIntent {
	contents := []string{units.ProviderService, units.RefreshService, units.RefreshPath, units.RefreshTimer, units.AgentDropIn}
	managed := managedWiringPaths(paths)
	intents := make([]LifecycleManagedFileIntent, 0, len(managed))
	for index, path := range managed {
		intent := LifecycleManagedFileIntent{Path: path, Present: present}
		if present {
			intent.Mode = 0o600
			intent.Contents = []byte(contents[index])
			intent.SHA256 = digestBytes(intent.Contents)
		}
		intents = append(intents, intent)
	}
	return intents
}

func snapshotManagedFiles(paths LifecyclePaths) ([]managedFileSnapshot, error) {
	if err := mkdirAllDurable(paths.Root, 0o700); err != nil {
		return nil, err
	}
	backupRoot, err := os.MkdirTemp(paths.Root, ".install-backup-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(backupRoot, 0o700); err != nil {
		_ = os.RemoveAll(backupRoot)
		return nil, err
	}
	snapshots := make([]managedFileSnapshot, 0, len(managedInstallPaths(paths)))
	for index, path := range managedInstallPaths(paths) {
		snapshot := managedFileSnapshot{Path: path, Backup: filepath.Join(backupRoot, strconv.Itoa(index))}
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			snapshots = append(snapshots, snapshot)
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.Join(errors.New("managed install path must be a regular file"), os.RemoveAll(backupRoot))
		}
		if err := validateOwner(info); err != nil {
			return nil, errors.Join(err, os.RemoveAll(backupRoot))
		}
		snapshot.Existed = true
		snapshot.Mode = info.Mode().Perm()
		if err := replaceRegularFile(path, snapshot.Backup, snapshot.Mode, maxProviderPackageBytes); err != nil {
			return nil, errors.Join(err, os.RemoveAll(backupRoot))
		}
		snapshot.SHA256, err = hashRegularFile(snapshot.Backup, snapshot.Mode&0o100 != 0)
		if err != nil {
			return nil, errors.Join(err, os.RemoveAll(backupRoot))
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func snapshotManagedFilesAt(paths LifecyclePaths, transactionRoot string) ([]managedFileSnapshot, error) {
	if filepath.Dir(transactionRoot) != paths.LifecycleTransactions || !safeIdentifierPattern.MatchString(filepath.Base(transactionRoot)) {
		return nil, errors.New("lifecycle snapshot transaction root is invalid")
	}
	info, err := os.Lstat(transactionRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return nil, errors.New("lifecycle snapshot transaction root must be an owner-only real directory")
	}
	if err := validateOwner(info); err != nil {
		return nil, fmt.Errorf("lifecycle snapshot transaction root ownership: %w", err)
	}
	snapshotRoot := filepath.Join(transactionRoot, "snapshots")
	if err := os.Mkdir(snapshotRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create lifecycle snapshot root: %w", err)
	}
	cleanup := func(cause error) ([]managedFileSnapshot, error) {
		return nil, errors.Join(cause, os.RemoveAll(snapshotRoot), syncDirectory(transactionRoot))
	}
	if err := syncDirectory(transactionRoot); err != nil {
		return cleanup(err)
	}
	snapshots := make([]managedFileSnapshot, 0, len(managedInstallPaths(paths)))
	for index, path := range managedInstallPaths(paths) {
		snapshot := managedFileSnapshot{Path: path, Backup: filepath.Join(snapshotRoot, strconv.Itoa(index))}
		entry, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			snapshots = append(snapshots, snapshot)
			continue
		}
		if err != nil || !entry.Mode().IsRegular() {
			return cleanup(errors.New("managed lifecycle snapshot path must be a regular file"))
		}
		if err := validateOwner(entry); err != nil {
			return cleanup(err)
		}
		snapshot.Existed = true
		snapshot.Mode = entry.Mode().Perm()
		if snapshot.Mode != 0o600 && snapshot.Mode != 0o700 {
			return cleanup(errors.New("managed lifecycle snapshot path must be owner-only"))
		}
		if err := replaceRegularFile(path, snapshot.Backup, snapshot.Mode, maxProviderPackageBytes); err != nil {
			return cleanup(err)
		}
		snapshot.SHA256, err = hashRegularFile(snapshot.Backup, snapshot.Mode&0o100 != 0)
		if err != nil {
			return cleanup(err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := syncDirectory(snapshotRoot); err != nil {
		return cleanup(err)
	}
	return snapshots, nil
}

func snapshotManagedFilesForLifecycle(home string, paths LifecyclePaths, journal *LifecycleJournal, now time.Time) error {
	if journal == nil || journal.Phase != LifecycleFencing || len(journal.Snapshots) != 0 {
		return errors.New("lifecycle snapshot journal must be in an empty fencing phase")
	}
	transactionRoot := paths.LifecycleTransactionRoot(journal.TransactionID)
	if filepath.Dir(transactionRoot) != paths.LifecycleTransactions || !safeIdentifierPattern.MatchString(filepath.Base(transactionRoot)) {
		return errors.New("lifecycle snapshot transaction root is invalid")
	}
	info, err := os.Lstat(transactionRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("lifecycle snapshot transaction root must be an owner-only real directory")
	}
	if err := validateOwner(info); err != nil {
		return fmt.Errorf("lifecycle snapshot transaction root ownership: %w", err)
	}
	snapshotRoot := filepath.Join(transactionRoot, "snapshots")
	if err := os.Mkdir(snapshotRoot, 0o700); err != nil {
		return fmt.Errorf("create lifecycle snapshot root: %w", err)
	}
	if err := syncDirectory(transactionRoot); err != nil {
		return err
	}
	for index, path := range managedInstallPaths(paths) {
		snapshot := managedFileSnapshot{Path: path, Backup: filepath.Join(snapshotRoot, strconv.Itoa(index))}
		entry, err := os.Lstat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil || !entry.Mode().IsRegular():
			return errors.New("managed lifecycle snapshot path must be a regular file")
		default:
			if err := validateOwner(entry); err != nil {
				return err
			}
			snapshot.Existed = true
			snapshot.Mode = entry.Mode().Perm()
			if snapshot.Mode != 0o600 && snapshot.Mode != 0o700 {
				return errors.New("managed lifecycle snapshot path must be owner-only")
			}
			if err := replaceRegularFile(path, snapshot.Backup, snapshot.Mode, maxProviderPackageBytes); err != nil {
				return err
			}
			snapshot.SHA256, err = hashRegularFile(snapshot.Backup, snapshot.Mode&0o100 != 0)
			if err != nil {
				return err
			}
		}
		journal.Snapshots = append(journal.Snapshots, snapshot)
		journal.UpdatedAt = now.UTC()
		if err := writeLifecycleJournal(home, paths, *journal); err != nil {
			return fmt.Errorf("record lifecycle snapshot: %w", err)
		}
	}
	return syncDirectory(snapshotRoot)
}

func restoreManagedFiles(snapshots []managedFileSnapshot) error {
	if err := restoreManagedFileContents(snapshots); err != nil {
		return err
	}
	return removeSnapshots(snapshots)
}

func restoreManagedFileContents(snapshots []managedFileSnapshot) error {
	var restoreErr error
	for _, snapshot := range snapshots {
		if !snapshot.Existed {
			restoreErr = errors.Join(restoreErr, removeDurableFile(snapshot.Path))
			continue
		}
		restoreErr = errors.Join(restoreErr, replaceRegularFile(snapshot.Backup, snapshot.Path, snapshot.Mode, maxProviderPackageBytes))
	}
	if restoreErr != nil {
		return restoreErr
	}
	return nil
}

func removeSnapshots(snapshots []managedFileSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}
	return os.RemoveAll(filepath.Dir(snapshots[0].Backup))
}

func replaceRegularFile(source, destination string, mode os.FileMode, maxBytes int64) (returnErr error) {
	if maxBytes <= 0 || mode&^os.FileMode(0o777) != 0 || mode&0o077 != 0 {
		return errors.New("replacement file mode or size limit is invalid")
	}
	sourceInfo, err := os.Lstat(source)
	if err != nil || !sourceInfo.Mode().IsRegular() || sourceInfo.Size() <= 0 || sourceInfo.Size() > maxBytes {
		return errors.New("replacement source must be a bounded regular file")
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(sourceInfo, opened) {
		return errors.New("replacement source changed during open")
	}
	directory := filepath.Dir(destination)
	if err := mkdirAllDurable(directory, 0o700); err != nil {
		return err
	}
	if err := rejectWritableDestination(destination); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".retained-provider-copy-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if err := os.Remove(temporaryPath); returnErr == nil && err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = err
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err := io.CopyN(temporary, input, sourceInfo.Size()); err != nil {
		return err
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); count != 0 || err != io.EOF {
		return errors.New("replacement source size changed during copy")
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func (installer Installer) now() time.Time {
	if installer.Now != nil {
		return installer.Now().UTC()
	}
	return time.Now().UTC()
}

func (installer Installer) run(ctx context.Context, command Command) ([]byte, error) {
	return runBoundedCommand(ctx, installer.Runner, command)
}

func (installer Installer) sleep(ctx context.Context, duration time.Duration) error {
	if installer.Sleep != nil {
		return installer.Sleep(ctx, duration)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
