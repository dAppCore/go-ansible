package ansible

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// --- Mock SSH Client ---

// MockSSHClient simulates an SSHClient for testing module logic
// without requiring real SSH connections.
type MockSSHClient struct {
	mu sync.Mutex

	// Command registry: patterns → pre-configured responses
	commands []commandExpectation

	// File system simulation: path → content
	files map[string][]byte

	// Stat results: path → stat info
	stats map[string]map[string]any

	// Become state tracking
	become     bool
	becomeUser string
	becomePass string

	// Execution log: every command that was executed
	executed []executedCommand

	// Upload log: every upload that was performed
	uploads []uploadRecord
}

// commandExpectation holds a pre-configured response for a command pattern.
type commandExpectation struct {
	pattern *regexp.Regexp
	stdout  string
	stderr  string
	rc      int
	err     error
}

// executedCommand records a command that was executed.
type executedCommand struct {
	Method string // "Run" or "RunScript"
	Cmd    string
}

// uploadRecord records an upload that was performed.
type uploadRecord struct {
	Content []byte
	Remote  string
	Mode    os.FileMode
}

// NewMockSSHClient creates a new mock SSH client with empty state.
func NewMockSSHClient() *MockSSHClient {
	return &MockSSHClient{
		files: make(map[string][]byte),
		stats: make(map[string]map[string]any),
	}
}

// expectCommand registers a command pattern with a pre-configured response.
// The pattern is a regular expression matched against the full command string.
func (m *MockSSHClient) expectCommand(pattern, stdout, stderr string, rc int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, commandExpectation{
		pattern: regexp.MustCompile(pattern),
		stdout:  stdout,
		stderr:  stderr,
		rc:      rc,
	})
}

// expectCommandError registers a command pattern that returns an error.
func (m *MockSSHClient) expectCommandError(pattern string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = append(m.commands, commandExpectation{
		pattern: regexp.MustCompile(pattern),
		err:     err,
	})
}

// addFile adds a file to the simulated filesystem.
func (m *MockSSHClient) addFile(path string, content []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = content
}

// addStat adds stat info for a path.
func (m *MockSSHClient) addStat(path string, info map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats[path] = info
}

// Run simulates executing a command. It matches against registered
// expectations in order (last match wins) and records the execution.
func (m *MockSSHClient) Run(_ context.Context, cmd string) (string, string, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executed = append(m.executed, executedCommand{Method: "Run", Cmd: cmd})

	// Search expectations in reverse order (last registered wins)
	for i := len(m.commands) - 1; i >= 0; i-- {
		exp := m.commands[i]
		if exp.pattern.MatchString(cmd) {
			return exp.stdout, exp.stderr, exp.rc, exp.err
		}
	}

	// Default: success with empty output
	return "", "", 0, nil
}

// RunScript simulates executing a script via heredoc.
func (m *MockSSHClient) RunScript(_ context.Context, script string) (string, string, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executed = append(m.executed, executedCommand{Method: "RunScript", Cmd: script})

	// Match against the script content
	for i := len(m.commands) - 1; i >= 0; i-- {
		exp := m.commands[i]
		if exp.pattern.MatchString(script) {
			return exp.stdout, exp.stderr, exp.rc, exp.err
		}
	}

	return "", "", 0, nil
}

// Upload simulates uploading content to the remote filesystem.
func (m *MockSSHClient) Upload(_ context.Context, local io.Reader, remote string, mode os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	content, err := io.ReadAll(local)
	if err != nil {
		return fmt.Errorf("mock upload read: %w", err)
	}

	m.uploads = append(m.uploads, uploadRecord{
		Content: content,
		Remote:  remote,
		Mode:    mode,
	})
	m.files[remote] = content
	return nil
}

// Download simulates downloading content from the remote filesystem.
func (m *MockSSHClient) Download(_ context.Context, remote string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	content, ok := m.files[remote]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", remote)
	}
	return content, nil
}

// FileExists checks if a path exists in the simulated filesystem.
func (m *MockSSHClient) FileExists(_ context.Context, path string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.files[path]
	return ok, nil
}

// Stat returns stat info from the pre-configured map, or constructs
// a basic result from the file existence in the simulated filesystem.
func (m *MockSSHClient) Stat(_ context.Context, path string) (map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check explicit stat results first
	if info, ok := m.stats[path]; ok {
		return info, nil
	}

	// Fall back to file existence
	if _, ok := m.files[path]; ok {
		return map[string]any{"exists": true, "isdir": false}, nil
	}
	return map[string]any{"exists": false}, nil
}

// SetBecome records become state changes.
func (m *MockSSHClient) SetBecome(become bool, user, password string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.become = become
	if user != "" {
		m.becomeUser = user
	}
	if password != "" {
		m.becomePass = password
	}
}

// Close is a no-op for the mock.
func (m *MockSSHClient) Close() error {
	return nil
}

// --- Assertion helpers ---

// executedCommands returns a copy of the execution log.
func (m *MockSSHClient) executedCommands() []executedCommand {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]executedCommand, len(m.executed))
	copy(cp, m.executed)
	return cp
}

// lastCommand returns the most recent command executed, or empty if none.
func (m *MockSSHClient) lastCommand() executedCommand {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.executed) == 0 {
		return executedCommand{}
	}
	return m.executed[len(m.executed)-1]
}

// commandCount returns the number of commands executed.
func (m *MockSSHClient) commandCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.executed)
}

// hasExecuted checks if any command matching the pattern was executed.
func (m *MockSSHClient) hasExecuted(pattern string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	re := regexp.MustCompile(pattern)
	for _, cmd := range m.executed {
		if re.MatchString(cmd.Cmd) {
			return true
		}
	}
	return false
}

// hasExecutedMethod checks if a command with the given method and matching
// pattern was executed.
func (m *MockSSHClient) hasExecutedMethod(method, pattern string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	re := regexp.MustCompile(pattern)
	for _, cmd := range m.executed {
		if cmd.Method == method && re.MatchString(cmd.Cmd) {
			return true
		}
	}
	return false
}

// findExecuted returns the first command matching the pattern, or nil.
func (m *MockSSHClient) findExecuted(pattern string) *executedCommand {
	m.mu.Lock()
	defer m.mu.Unlock()
	re := regexp.MustCompile(pattern)
	for i := range m.executed {
		if re.MatchString(m.executed[i].Cmd) {
			cmd := m.executed[i]
			return &cmd
		}
	}
	return nil
}

// uploadCount returns the number of uploads performed.
func (m *MockSSHClient) uploadCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploads)
}

// lastUpload returns the most recent upload, or nil if none.
func (m *MockSSHClient) lastUpload() *uploadRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.uploads) == 0 {
		return nil
	}
	u := m.uploads[len(m.uploads)-1]
	return &u
}

// reset clears all execution history (but keeps expectations and files).
func (m *MockSSHClient) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executed = nil
	m.uploads = nil
}

// --- Test helper: create executor with mock client ---

// newTestExecutorWithMock creates an Executor pre-wired with a MockSSHClient
// for the given host. The executor has a minimal inventory so that
// executeModule can be called directly.
func newTestExecutorWithMock(host string) (*Executor, *MockSSHClient) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()

	// Wire mock into executor's client map
	// We cannot store a *MockSSHClient directly because the executor
	// expects *SSHClient. Instead, we provide a helper that calls
	// modules the same way the executor does but with the mock.
	// Since modules call methods on *SSHClient directly and the mock
	// has identical method signatures, we use a shim approach.

	// Set up minimal inventory so host resolution works
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				host: {AnsibleHost: "127.0.0.1"},
			},
		},
	})

	return e, mock
}

// executeModuleWithMock calls a module handler directly using the mock client.
// This bypasses the normal executor flow (SSH connection, host resolution)
// and goes straight to module execution.
func executeModuleWithMock(e *Executor, mock *MockSSHClient, host string, task *Task) (*TaskResult, error) {
	module := NormalizeModule(task.Module)
	args := e.templateArgs(task.Args, host, task)

	// Dispatch directly to module handlers using the mock
	switch module {
	case "ansible.builtin.shell":
		return moduleShellWithClient(e, mock, args)
	case "ansible.builtin.command":
		return moduleCommandWithClient(e, mock, args)
	case "ansible.builtin.raw":
		return moduleRawWithClient(e, mock, args)
	case "ansible.builtin.script":
		return moduleScriptWithClient(e, mock, args)
	case "ansible.builtin.copy":
		return moduleCopyWithClient(e, mock, args, host, task)
	case "ansible.builtin.template":
		return moduleTemplateWithClient(e, mock, args, host, task)
	case "ansible.builtin.file":
		return moduleFileWithClient(e, mock, args)
	case "ansible.builtin.lineinfile":
		return moduleLineinfileWithClient(e, mock, args)
	case "ansible.builtin.blockinfile":
		return moduleBlockinfileWithClient(e, mock, args)
	case "ansible.builtin.stat":
		return moduleStatWithClient(e, mock, args)
	// Service management
	case "ansible.builtin.service":
		return moduleServiceWithClient(e, mock, args)
	case "ansible.builtin.systemd":
		return moduleSystemdWithClient(e, mock, args)

	// Package management
	case "ansible.builtin.apt":
		return moduleAptWithClient(e, mock, args)
	case "ansible.builtin.apt_key":
		return moduleAptKeyWithClient(e, mock, args)
	case "ansible.builtin.apt_repository":
		return moduleAptRepositoryWithClient(e, mock, args)
	case "ansible.builtin.package":
		return modulePackageWithClient(e, mock, args)
	case "ansible.builtin.pip":
		return modulePipWithClient(e, mock, args)

	// User/group management
	case "ansible.builtin.user":
		return moduleUserWithClient(e, mock, args)
	case "ansible.builtin.group":
		return moduleGroupWithClient(e, mock, args)

	// Cron
	case "ansible.builtin.cron":
		return moduleCronWithClient(e, mock, args)

	// SSH keys
	case "ansible.posix.authorized_key", "ansible.builtin.authorized_key":
		return moduleAuthorizedKeyWithClient(e, mock, args)

	// Git
	case "ansible.builtin.git":
		return moduleGitWithClient(e, mock, args)

	// Archive
	case "ansible.builtin.unarchive":
		return moduleUnarchiveWithClient(e, mock, args)

	// HTTP
	case "ansible.builtin.uri":
		return moduleURIWithClient(e, mock, args)

	// Firewall
	case "community.general.ufw", "ansible.builtin.ufw":
		return moduleUFWWithClient(e, mock, args)

	// Docker
	case "community.docker.docker_compose_v2", "ansible.builtin.docker_compose":
		return moduleDockerComposeWithClient(e, mock, args)

	default:
		return nil, fmt.Errorf("mock dispatch: unsupported module %s", module)
	}
}

// --- Module shims that accept the mock interface ---
// These mirror the module methods but accept our mock instead of *SSHClient.

type sshRunner interface {
	Run(ctx context.Context, cmd string) (string, string, int, error)
	RunScript(ctx context.Context, script string) (string, string, int, error)
}

func moduleShellWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		cmd = getStringArg(args, "cmd", "")
	}
	if cmd == "" {
		return nil, fmt.Errorf("shell: no command specified")
	}

	if chdir := getStringArg(args, "chdir", ""); chdir != "" {
		cmd = fmt.Sprintf("cd %q && %s", chdir, cmd)
	}

	stdout, stderr, rc, err := client.RunScript(context.Background(), cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error(), Stdout: stdout, Stderr: stderr, RC: rc}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
		Failed:  rc != 0,
	}, nil
}

func moduleCommandWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		cmd = getStringArg(args, "cmd", "")
	}
	if cmd == "" {
		return nil, fmt.Errorf("command: no command specified")
	}

	if chdir := getStringArg(args, "chdir", ""); chdir != "" {
		cmd = fmt.Sprintf("cd %q && %s", chdir, cmd)
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
		Failed:  rc != 0,
	}, nil
}

func moduleRawWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		return nil, fmt.Errorf("raw: no command specified")
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
	}, nil
}

func moduleScriptWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	script := getStringArg(args, "_raw_params", "")
	if script == "" {
		return nil, fmt.Errorf("script: no script specified")
	}

	content, err := os.ReadFile(script)
	if err != nil {
		return nil, fmt.Errorf("read script: %w", err)
	}

	stdout, stderr, rc, err := client.RunScript(context.Background(), string(content))
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
		Failed:  rc != 0,
	}, nil
}

// --- Extended interface for file operations ---
// File modules need Upload, Stat, FileExists in addition to Run/RunScript.

type sshFileRunner interface {
	sshRunner
	Upload(ctx context.Context, local io.Reader, remote string, mode os.FileMode) error
	Stat(ctx context.Context, path string) (map[string]any, error)
	FileExists(ctx context.Context, path string) (bool, error)
}

// --- File module shims ---

func moduleCopyWithClient(e *Executor, client sshFileRunner, args map[string]any, host string, task *Task) (*TaskResult, error) {
	dest := getStringArg(args, "dest", "")
	if dest == "" {
		return nil, fmt.Errorf("copy: dest required")
	}

	var content []byte
	var err error

	if src := getStringArg(args, "src", ""); src != "" {
		content, err = os.ReadFile(src)
		if err != nil {
			return nil, fmt.Errorf("read src: %w", err)
		}
	} else if c := getStringArg(args, "content", ""); c != "" {
		content = []byte(c)
	} else {
		return nil, fmt.Errorf("copy: src or content required")
	}

	mode := os.FileMode(0644)
	if m := getStringArg(args, "mode", ""); m != "" {
		if parsed, parseErr := strconv.ParseInt(m, 8, 32); parseErr == nil {
			mode = os.FileMode(parsed)
		}
	}

	err = client.Upload(context.Background(), strings.NewReader(string(content)), dest, mode)
	if err != nil {
		return nil, err
	}

	// Handle owner/group (best-effort, errors ignored)
	if owner := getStringArg(args, "owner", ""); owner != "" {
		_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chown %s %q", owner, dest))
	}
	if group := getStringArg(args, "group", ""); group != "" {
		_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chgrp %s %q", group, dest))
	}

	return &TaskResult{Changed: true, Msg: fmt.Sprintf("copied to %s", dest)}, nil
}

func moduleTemplateWithClient(e *Executor, client sshFileRunner, args map[string]any, host string, task *Task) (*TaskResult, error) {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	if src == "" || dest == "" {
		return nil, fmt.Errorf("template: src and dest required")
	}

	// Process template
	content, err := e.TemplateFile(src, host, task)
	if err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}

	mode := os.FileMode(0644)
	if m := getStringArg(args, "mode", ""); m != "" {
		if parsed, parseErr := strconv.ParseInt(m, 8, 32); parseErr == nil {
			mode = os.FileMode(parsed)
		}
	}

	err = client.Upload(context.Background(), strings.NewReader(content), dest, mode)
	if err != nil {
		return nil, err
	}

	return &TaskResult{Changed: true, Msg: fmt.Sprintf("templated to %s", dest)}, nil
}

func moduleFileWithClient(_ *Executor, client sshFileRunner, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return nil, fmt.Errorf("file: path required")
	}

	state := getStringArg(args, "state", "file")

	switch state {
	case "directory":
		mode := getStringArg(args, "mode", "0755")
		cmd := fmt.Sprintf("mkdir -p %q && chmod %s %q", path, mode, path)
		stdout, stderr, rc, err := client.Run(context.Background(), cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}

	case "absent":
		cmd := fmt.Sprintf("rm -rf %q", path)
		_, stderr, rc, err := client.Run(context.Background(), cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
		}

	case "touch":
		cmd := fmt.Sprintf("touch %q", path)
		_, stderr, rc, err := client.Run(context.Background(), cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
		}

	case "link":
		src := getStringArg(args, "src", "")
		if src == "" {
			return nil, fmt.Errorf("file: src required for link state")
		}
		cmd := fmt.Sprintf("ln -sf %q %q", src, path)
		_, stderr, rc, err := client.Run(context.Background(), cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
		}

	case "file":
		// Ensure file exists and set permissions
		if mode := getStringArg(args, "mode", ""); mode != "" {
			_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chmod %s %q", mode, path))
		}
	}

	// Handle owner/group (best-effort, errors ignored)
	if owner := getStringArg(args, "owner", ""); owner != "" {
		_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chown %s %q", owner, path))
	}
	if group := getStringArg(args, "group", ""); group != "" {
		_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chgrp %s %q", group, path))
	}
	if recurse := getBoolArg(args, "recurse", false); recurse {
		if owner := getStringArg(args, "owner", ""); owner != "" {
			_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chown -R %s %q", owner, path))
		}
	}

	return &TaskResult{Changed: true}, nil
}

func moduleLineinfileWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return nil, fmt.Errorf("lineinfile: path required")
	}

	line := getStringArg(args, "line", "")
	regexpArg := getStringArg(args, "regexp", "")
	state := getStringArg(args, "state", "present")

	if state == "absent" {
		if regexpArg != "" {
			cmd := fmt.Sprintf("sed -i '/%s/d' %q", regexpArg, path)
			_, stderr, rc, _ := client.Run(context.Background(), cmd)
			if rc != 0 {
				return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
			}
		}
	} else {
		// state == present
		if regexpArg != "" {
			// Replace line matching regexp
			escapedLine := strings.ReplaceAll(line, "/", "\\/")
			cmd := fmt.Sprintf("sed -i 's/%s/%s/' %q", regexpArg, escapedLine, path)
			_, _, rc, _ := client.Run(context.Background(), cmd)
			if rc != 0 {
				// Line not found, append
				cmd = fmt.Sprintf("echo %q >> %q", line, path)
				_, _, _, _ = client.Run(context.Background(), cmd)
			}
		} else if line != "" {
			// Ensure line is present
			cmd := fmt.Sprintf("grep -qxF %q %q || echo %q >> %q", line, path, line, path)
			_, _, _, _ = client.Run(context.Background(), cmd)
		}
	}

	return &TaskResult{Changed: true}, nil
}

func moduleBlockinfileWithClient(_ *Executor, client sshFileRunner, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return nil, fmt.Errorf("blockinfile: path required")
	}

	block := getStringArg(args, "block", "")
	marker := getStringArg(args, "marker", "# {mark} ANSIBLE MANAGED BLOCK")
	state := getStringArg(args, "state", "present")
	create := getBoolArg(args, "create", false)

	beginMarker := strings.Replace(marker, "{mark}", "BEGIN", 1)
	endMarker := strings.Replace(marker, "{mark}", "END", 1)

	if state == "absent" {
		// Remove block
		cmd := fmt.Sprintf("sed -i '/%s/,/%s/d' %q",
			strings.ReplaceAll(beginMarker, "/", "\\/"),
			strings.ReplaceAll(endMarker, "/", "\\/"),
			path)
		_, _, _, _ = client.Run(context.Background(), cmd)
		return &TaskResult{Changed: true}, nil
	}

	// Create file if needed (best-effort)
	if create {
		_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("touch %q", path))
	}

	// Remove existing block and add new one
	escapedBlock := strings.ReplaceAll(block, "'", "'\\''")
	cmd := fmt.Sprintf(`
sed -i '/%s/,/%s/d' %q 2>/dev/null || true
cat >> %q << 'BLOCK_EOF'
%s
%s
%s
BLOCK_EOF
`, strings.ReplaceAll(beginMarker, "/", "\\/"),
		strings.ReplaceAll(endMarker, "/", "\\/"),
		path, path, beginMarker, escapedBlock, endMarker)

	stdout, stderr, rc, err := client.RunScript(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func moduleStatWithClient(_ *Executor, client sshFileRunner, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		return nil, fmt.Errorf("stat: path required")
	}

	stat, err := client.Stat(context.Background(), path)
	if err != nil {
		return nil, err
	}

	return &TaskResult{
		Changed: false,
		Data:    map[string]any{"stat": stat},
	}, nil
}

// --- Service module shims ---

func moduleServiceWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "")
	enabled := args["enabled"]

	if name == "" {
		return nil, fmt.Errorf("service: name required")
	}

	var cmds []string

	if state != "" {
		switch state {
		case "started":
			cmds = append(cmds, fmt.Sprintf("systemctl start %s", name))
		case "stopped":
			cmds = append(cmds, fmt.Sprintf("systemctl stop %s", name))
		case "restarted":
			cmds = append(cmds, fmt.Sprintf("systemctl restart %s", name))
		case "reloaded":
			cmds = append(cmds, fmt.Sprintf("systemctl reload %s", name))
		}
	}

	if enabled != nil {
		if getBoolArg(args, "enabled", false) {
			cmds = append(cmds, fmt.Sprintf("systemctl enable %s", name))
		} else {
			cmds = append(cmds, fmt.Sprintf("systemctl disable %s", name))
		}
	}

	for _, cmd := range cmds {
		stdout, stderr, rc, err := client.Run(context.Background(), cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
	}

	return &TaskResult{Changed: len(cmds) > 0}, nil
}

func moduleSystemdWithClient(e *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	if getBoolArg(args, "daemon_reload", false) {
		_, _, _, _ = client.Run(context.Background(), "systemctl daemon-reload")
	}

	return moduleServiceWithClient(e, client, args)
}

// --- Package module shims ---

func moduleAptWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	updateCache := getBoolArg(args, "update_cache", false)

	var cmd string

	if updateCache {
		_, _, _, _ = client.Run(context.Background(), "apt-get update -qq")
	}

	switch state {
	case "present", "installed":
		if name != "" {
			cmd = fmt.Sprintf("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq %s", name)
		}
	case "absent", "removed":
		cmd = fmt.Sprintf("DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq %s", name)
	case "latest":
		cmd = fmt.Sprintf("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --only-upgrade %s", name)
	}

	if cmd == "" {
		return &TaskResult{Changed: false}, nil
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func moduleAptKeyWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	url := getStringArg(args, "url", "")
	keyring := getStringArg(args, "keyring", "")
	state := getStringArg(args, "state", "present")

	if state == "absent" {
		if keyring != "" {
			_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("rm -f %q", keyring))
		}
		return &TaskResult{Changed: true}, nil
	}

	if url == "" {
		return nil, fmt.Errorf("apt_key: url required")
	}

	var cmd string
	if keyring != "" {
		cmd = fmt.Sprintf("curl -fsSL %q | gpg --dearmor -o %q", url, keyring)
	} else {
		cmd = fmt.Sprintf("curl -fsSL %q | apt-key add -", url)
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func moduleAptRepositoryWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	repo := getStringArg(args, "repo", "")
	filename := getStringArg(args, "filename", "")
	state := getStringArg(args, "state", "present")

	if repo == "" {
		return nil, fmt.Errorf("apt_repository: repo required")
	}

	if filename == "" {
		filename = strings.ReplaceAll(repo, " ", "-")
		filename = strings.ReplaceAll(filename, "/", "-")
		filename = strings.ReplaceAll(filename, ":", "")
	}

	path := fmt.Sprintf("/etc/apt/sources.list.d/%s.list", filename)

	if state == "absent" {
		_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("rm -f %q", path))
		return &TaskResult{Changed: true}, nil
	}

	cmd := fmt.Sprintf("echo %q > %q", repo, path)
	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	if getBoolArg(args, "update_cache", true) {
		_, _, _, _ = client.Run(context.Background(), "apt-get update -qq")
	}

	return &TaskResult{Changed: true}, nil
}

func modulePackageWithClient(e *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	stdout, _, _, _ := client.Run(context.Background(), "which apt-get yum dnf 2>/dev/null | head -1")
	stdout = strings.TrimSpace(stdout)

	if strings.Contains(stdout, "apt") {
		return moduleAptWithClient(e, client, args)
	}

	return moduleAptWithClient(e, client, args)
}

func modulePipWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	executable := getStringArg(args, "executable", "pip3")

	var cmd string
	switch state {
	case "present", "installed":
		cmd = fmt.Sprintf("%s install %s", executable, name)
	case "absent", "removed":
		cmd = fmt.Sprintf("%s uninstall -y %s", executable, name)
	case "latest":
		cmd = fmt.Sprintf("%s install --upgrade %s", executable, name)
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- User/Group module shims ---

func moduleUserWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")

	if name == "" {
		return nil, fmt.Errorf("user: name required")
	}

	if state == "absent" {
		cmd := fmt.Sprintf("userdel -r %s 2>/dev/null || true", name)
		_, _, _, _ = client.Run(context.Background(), cmd)
		return &TaskResult{Changed: true}, nil
	}

	// Build useradd/usermod command
	var opts []string

	if uid := getStringArg(args, "uid", ""); uid != "" {
		opts = append(opts, "-u", uid)
	}
	if group := getStringArg(args, "group", ""); group != "" {
		opts = append(opts, "-g", group)
	}
	if groups := getStringArg(args, "groups", ""); groups != "" {
		opts = append(opts, "-G", groups)
	}
	if home := getStringArg(args, "home", ""); home != "" {
		opts = append(opts, "-d", home)
	}
	if shell := getStringArg(args, "shell", ""); shell != "" {
		opts = append(opts, "-s", shell)
	}
	if getBoolArg(args, "system", false) {
		opts = append(opts, "-r")
	}
	if getBoolArg(args, "create_home", true) {
		opts = append(opts, "-m")
	}

	// Try usermod first, then useradd
	optsStr := strings.Join(opts, " ")
	var cmd string
	if optsStr == "" {
		cmd = fmt.Sprintf("id %s >/dev/null 2>&1 || useradd %s", name, name)
	} else {
		cmd = fmt.Sprintf("id %s >/dev/null 2>&1 && usermod %s %s || useradd %s %s",
			name, optsStr, name, optsStr, name)
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func moduleGroupWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")

	if name == "" {
		return nil, fmt.Errorf("group: name required")
	}

	if state == "absent" {
		cmd := fmt.Sprintf("groupdel %s 2>/dev/null || true", name)
		_, _, _, _ = client.Run(context.Background(), cmd)
		return &TaskResult{Changed: true}, nil
	}

	var opts []string
	if gid := getStringArg(args, "gid", ""); gid != "" {
		opts = append(opts, "-g", gid)
	}
	if getBoolArg(args, "system", false) {
		opts = append(opts, "-r")
	}

	cmd := fmt.Sprintf("getent group %s >/dev/null 2>&1 || groupadd %s %s",
		name, strings.Join(opts, " "), name)

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- Cron module shim ---

func moduleCronWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	job := getStringArg(args, "job", "")
	state := getStringArg(args, "state", "present")
	user := getStringArg(args, "user", "root")

	minute := getStringArg(args, "minute", "*")
	hour := getStringArg(args, "hour", "*")
	day := getStringArg(args, "day", "*")
	month := getStringArg(args, "month", "*")
	weekday := getStringArg(args, "weekday", "*")

	if state == "absent" {
		if name != "" {
			// Remove by name (comment marker)
			cmd := fmt.Sprintf("crontab -u %s -l 2>/dev/null | grep -v '# %s' | grep -v '%s' | crontab -u %s -",
				user, name, job, user)
			_, _, _, _ = client.Run(context.Background(), cmd)
		}
		return &TaskResult{Changed: true}, nil
	}

	// Build cron entry
	schedule := fmt.Sprintf("%s %s %s %s %s", minute, hour, day, month, weekday)
	entry := fmt.Sprintf("%s %s # %s", schedule, job, name)

	// Add to crontab
	cmd := fmt.Sprintf("(crontab -u %s -l 2>/dev/null | grep -v '# %s' ; echo %q) | crontab -u %s -",
		user, name, entry, user)
	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- Authorized key module shim ---

func moduleAuthorizedKeyWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	user := getStringArg(args, "user", "")
	key := getStringArg(args, "key", "")
	state := getStringArg(args, "state", "present")

	if user == "" || key == "" {
		return nil, fmt.Errorf("authorized_key: user and key required")
	}

	// Get user's home directory
	stdout, _, _, err := client.Run(context.Background(), fmt.Sprintf("getent passwd %s | cut -d: -f6", user))
	if err != nil {
		return nil, fmt.Errorf("get home dir: %w", err)
	}
	home := strings.TrimSpace(stdout)
	if home == "" {
		home = "/root"
		if user != "root" {
			home = "/home/" + user
		}
	}

	authKeysPath := filepath.Join(home, ".ssh", "authorized_keys")

	if state == "absent" {
		// Remove key
		escapedKey := strings.ReplaceAll(key, "/", "\\/")
		cmd := fmt.Sprintf("sed -i '/%s/d' %q 2>/dev/null || true", escapedKey[:40], authKeysPath)
		_, _, _, _ = client.Run(context.Background(), cmd)
		return &TaskResult{Changed: true}, nil
	}

	// Ensure .ssh directory exists (best-effort)
	_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("mkdir -p %q && chmod 700 %q && chown %s:%s %q",
		filepath.Dir(authKeysPath), filepath.Dir(authKeysPath), user, user, filepath.Dir(authKeysPath)))

	// Add key if not present
	cmd := fmt.Sprintf("grep -qF %q %q 2>/dev/null || echo %q >> %q",
		key[:40], authKeysPath, key, authKeysPath)
	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Fix permissions (best-effort)
	_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("chmod 600 %q && chown %s:%s %q",
		authKeysPath, user, user, authKeysPath))

	return &TaskResult{Changed: true}, nil
}

// --- Git module shim ---

func moduleGitWithClient(_ *Executor, client sshFileRunner, args map[string]any) (*TaskResult, error) {
	repo := getStringArg(args, "repo", "")
	dest := getStringArg(args, "dest", "")
	version := getStringArg(args, "version", "HEAD")

	if repo == "" || dest == "" {
		return nil, fmt.Errorf("git: repo and dest required")
	}

	// Check if dest exists
	exists, _ := client.FileExists(context.Background(), dest+"/.git")

	var cmd string
	if exists {
		cmd = fmt.Sprintf("cd %q && git fetch --all && git checkout --force %q", dest, version)
	} else {
		cmd = fmt.Sprintf("git clone %q %q && cd %q && git checkout %q",
			repo, dest, dest, version)
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- Unarchive module shim ---

func moduleUnarchiveWithClient(_ *Executor, client sshFileRunner, args map[string]any) (*TaskResult, error) {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	remote := getBoolArg(args, "remote_src", false)

	if src == "" || dest == "" {
		return nil, fmt.Errorf("unarchive: src and dest required")
	}

	// Create dest directory (best-effort)
	_, _, _, _ = client.Run(context.Background(), fmt.Sprintf("mkdir -p %q", dest))

	var cmd string
	if !remote {
		// Upload local file first
		content, err := os.ReadFile(src)
		if err != nil {
			return nil, fmt.Errorf("read src: %w", err)
		}
		tmpPath := "/tmp/ansible_unarchive_" + filepath.Base(src)
		err = client.Upload(context.Background(), strings.NewReader(string(content)), tmpPath, 0644)
		if err != nil {
			return nil, err
		}
		src = tmpPath
		defer func() { _, _, _, _ = client.Run(context.Background(), fmt.Sprintf("rm -f %q", tmpPath)) }()
	}

	// Detect archive type and extract
	if strings.HasSuffix(src, ".tar.gz") || strings.HasSuffix(src, ".tgz") {
		cmd = fmt.Sprintf("tar -xzf %q -C %q", src, dest)
	} else if strings.HasSuffix(src, ".tar.xz") {
		cmd = fmt.Sprintf("tar -xJf %q -C %q", src, dest)
	} else if strings.HasSuffix(src, ".tar.bz2") {
		cmd = fmt.Sprintf("tar -xjf %q -C %q", src, dest)
	} else if strings.HasSuffix(src, ".tar") {
		cmd = fmt.Sprintf("tar -xf %q -C %q", src, dest)
	} else if strings.HasSuffix(src, ".zip") {
		cmd = fmt.Sprintf("unzip -o %q -d %q", src, dest)
	} else {
		cmd = fmt.Sprintf("tar -xf %q -C %q", src, dest) // Guess tar
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- URI module shim ---

func moduleURIWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	url := getStringArg(args, "url", "")
	method := getStringArg(args, "method", "GET")

	if url == "" {
		return nil, fmt.Errorf("uri: url required")
	}

	var curlOpts []string
	curlOpts = append(curlOpts, "-s", "-S")
	curlOpts = append(curlOpts, "-X", method)

	// Headers
	if headers, ok := args["headers"].(map[string]any); ok {
		for k, v := range headers {
			curlOpts = append(curlOpts, "-H", fmt.Sprintf("%s: %v", k, v))
		}
	}

	// Body
	if body := getStringArg(args, "body", ""); body != "" {
		curlOpts = append(curlOpts, "-d", body)
	}

	// Status code
	curlOpts = append(curlOpts, "-w", "\\n%{http_code}")

	cmd := fmt.Sprintf("curl %s %q", strings.Join(curlOpts, " "), url)
	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	// Parse status code from last line
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	statusCode := 0
	if len(lines) > 0 {
		statusCode, _ = strconv.Atoi(lines[len(lines)-1])
	}

	// Check expected status
	expectedStatus := 200
	if s, ok := args["status_code"].(int); ok {
		expectedStatus = s
	}

	failed := rc != 0 || statusCode != expectedStatus

	return &TaskResult{
		Changed: false,
		Failed:  failed,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      statusCode,
		Data:    map[string]any{"status": statusCode},
	}, nil
}

// --- UFW module shim ---

func moduleUFWWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	rule := getStringArg(args, "rule", "")
	port := getStringArg(args, "port", "")
	proto := getStringArg(args, "proto", "tcp")
	state := getStringArg(args, "state", "")

	var cmd string

	// Handle state (enable/disable)
	if state != "" {
		switch state {
		case "enabled":
			cmd = "ufw --force enable"
		case "disabled":
			cmd = "ufw disable"
		case "reloaded":
			cmd = "ufw reload"
		case "reset":
			cmd = "ufw --force reset"
		}
		if cmd != "" {
			stdout, stderr, rc, err := client.Run(context.Background(), cmd)
			if err != nil || rc != 0 {
				return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
			}
			return &TaskResult{Changed: true}, nil
		}
	}

	// Handle rule
	if rule != "" && port != "" {
		switch rule {
		case "allow":
			cmd = fmt.Sprintf("ufw allow %s/%s", port, proto)
		case "deny":
			cmd = fmt.Sprintf("ufw deny %s/%s", port, proto)
		case "reject":
			cmd = fmt.Sprintf("ufw reject %s/%s", port, proto)
		case "limit":
			cmd = fmt.Sprintf("ufw limit %s/%s", port, proto)
		}

		stdout, stderr, rc, err := client.Run(context.Background(), cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
	}

	return &TaskResult{Changed: true}, nil
}

// --- Docker Compose module shim ---

func moduleDockerComposeWithClient(_ *Executor, client sshRunner, args map[string]any) (*TaskResult, error) {
	projectSrc := getStringArg(args, "project_src", "")
	state := getStringArg(args, "state", "present")

	if projectSrc == "" {
		return nil, fmt.Errorf("docker_compose: project_src required")
	}

	var cmd string
	switch state {
	case "present":
		cmd = fmt.Sprintf("cd %q && docker compose up -d", projectSrc)
	case "absent":
		cmd = fmt.Sprintf("cd %q && docker compose down", projectSrc)
	case "restarted":
		cmd = fmt.Sprintf("cd %q && docker compose restart", projectSrc)
	default:
		cmd = fmt.Sprintf("cd %q && docker compose up -d", projectSrc)
	}

	stdout, stderr, rc, err := client.Run(context.Background(), cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Heuristic for changed
	changed := !strings.Contains(stdout, "Up to date") && !strings.Contains(stderr, "Up to date")

	return &TaskResult{Changed: changed, Stdout: stdout}, nil
}

// --- String helpers for assertions ---

// containsSubstring checks if any executed command contains the given substring.
func (m *MockSSHClient) containsSubstring(sub string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cmd := range m.executed {
		if strings.Contains(cmd.Cmd, sub) {
			return true
		}
	}
	return false
}
