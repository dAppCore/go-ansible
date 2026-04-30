package ansible

import (
	"context"
	"io"
	"io/fs"
	"regexp"
	"sync"

	core "dappco.re/go"
)

// --- Mock SSH Client ---

// MockSSHClient simulates an SSHClient for testing module logic
// without requiring real SSH connections.
//
// Example:
//
//	mock := NewMockSSHClient()
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

	// Lifecycle tracking
	closed bool

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
	Mode    fs.FileMode
}

// NewMockSSHClient creates a new mock SSH client with empty state.
//
// Example:
//
//	mock := NewMockSSHClient()
//	mock.expectCommand("echo ok", "ok", "", 0)
func NewMockSSHClient() *MockSSHClient {
	return &MockSSHClient{
		files: make(map[string][]byte),
		stats: make(map[string]map[string]any),
	}
}

func mockError(op, msg string) error {
	return core.E(op, msg, nil)
}

func mockWrap(op, msg string, err error) error {
	return core.E(op, msg, err)
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
//
// Example:
//
//	result := mock.Run(context.Background(), "echo ok")
func (m *MockSSHClient) Run(_ context.Context, cmd string) core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executed = append(m.executed, executedCommand{Method: "Run", Cmd: cmd})

	// Search expectations in reverse order (last registered wins)
	for i := len(m.commands) - 1; i >= 0; i-- {
		exp := m.commands[i]
		if exp.pattern.MatchString(cmd) {
			if exp.err != nil {
				return commandRunFail(exp.stdout, exp.stderr, exp.rc, exp.err)
			}
			return commandRunOK(exp.stdout, exp.stderr, exp.rc)
		}
	}

	// Default: success with empty output
	return commandRunOK("", "", 0)
}

// RunScript simulates executing a script via heredoc.
//
// Example:
//
//	result := mock.RunScript(context.Background(), "echo ok")
func (m *MockSSHClient) RunScript(_ context.Context, script string) core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executed = append(m.executed, executedCommand{Method: "RunScript", Cmd: script})

	// Match against the script content
	for i := len(m.commands) - 1; i >= 0; i-- {
		exp := m.commands[i]
		if exp.pattern.MatchString(script) {
			if exp.err != nil {
				return commandRunFail(exp.stdout, exp.stderr, exp.rc, exp.err)
			}
			return commandRunOK(exp.stdout, exp.stderr, exp.rc)
		}
	}

	return commandRunOK("", "", 0)
}

// Upload simulates uploading content to the remote filesystem.
//
// Example:
//
//	result := mock.Upload(context.Background(), newReader("hello"), "/tmp/hello.txt", 0644)
func (m *MockSSHClient) Upload(_ context.Context, local io.Reader, remote string, mode fs.FileMode) core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	content, err := io.ReadAll(local)
	if err != nil {
		return core.Fail(mockWrap("MockSSHClient.Upload", "mock upload read", err))
	}

	m.uploads = append(m.uploads, uploadRecord{
		Content: content,
		Remote:  remote,
		Mode:    mode,
	})
	m.files[remote] = content
	return core.Ok(nil)
}

// Download simulates downloading content from the remote filesystem.
//
// Example:
//
//	result := mock.Download(context.Background(), "/tmp/hello.txt")
func (m *MockSSHClient) Download(_ context.Context, remote string) core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	content, ok := m.files[remote]
	if !ok {
		return core.Fail(mockError("MockSSHClient.Download", sprintf("file not found: %s", remote)))
	}
	return core.Ok(content)
}

// FileExists checks if a path exists in the simulated filesystem.
//
// Example:
//
//	result := mock.FileExists(context.Background(), "/tmp/hello.txt")
func (m *MockSSHClient) FileExists(_ context.Context, path string) core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.files[path]
	return core.Ok(ok)
}

// Stat returns stat info from the pre-configured map, or constructs
// a basic result from the file existence in the simulated filesystem.
//
// Example:
//
//	result := mock.Stat(context.Background(), "/tmp/hello.txt")
func (m *MockSSHClient) Stat(_ context.Context, path string) core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check explicit stat results first
	if info, ok := m.stats[path]; ok {
		return core.Ok(info)
	}

	// Fall back to file existence
	if _, ok := m.files[path]; ok {
		return core.Ok(map[string]any{"exists": true, "isdir": false})
	}
	return core.Ok(map[string]any{"exists": false})
}

// SetBecome records become state changes.
//
// Example:
//
//	mock.SetBecome(true, "root", "")
func (m *MockSSHClient) SetBecome(become bool, user, password string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.become = become
	if !become {
		m.becomeUser = ""
		m.becomePass = ""
		return
	}
	if user != "" {
		m.becomeUser = user
	}
	if password != "" {
		m.becomePass = password
	}
}

// Close is a no-op for the mock.
//
// Example:
//
//	result := mock.Close()
func (m *MockSSHClient) Close() core.Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return core.Ok(nil)
}

// BecomeState returns the current privilege escalation settings.
func (m *MockSSHClient) BecomeState() (bool, string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.become, m.becomeUser, m.becomePass
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
// for the given host. The executor has a minimal inventory so that tasks can
// be executed through the normal host/client lookup path.
func newTestExecutorWithMock(host string) (*Executor, *MockSSHClient) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()

	// Set up minimal inventory so host resolution works
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				host: {AnsibleHost: "127.0.0.1"},
			},
		},
	})
	e.clients[host] = mock

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
	case "ansible.builtin.yum":
		return moduleYumWithClient(e, mock, args)
	case "ansible.builtin.dnf":
		return moduleDnfWithClient(e, mock, args)
	case "ansible.builtin.rpm":
		return moduleRPMWithClient(mock, args, "rpm")
	case "ansible.builtin.package":
		return modulePackageWithClient(e, mock, args)
	case "ansible.builtin.pip":
		return modulePipWithClient(e, mock, args)

	// User/group management
	case "ansible.builtin.user":
		return moduleUserWithClient(e, mock, args)
	case "ansible.builtin.group":
		return moduleGroupWithClient(e, mock, args)
	case "ansible.builtin.group_by", "group_by":
		return taskResultFromResult(e.moduleGroupBy(host, args))

	// Cron
	case "ansible.builtin.cron":
		return moduleCronWithClient(e, mock, args)

	// SSH keys
	case "ansible.posix.authorized_key", "ansible.builtin.authorized_key":
		return moduleAuthorizedKeyWithClient(e, mock, args)

	// Git
	case "ansible.builtin.git":
		return moduleGitWithClient(e, mock, args)

	case "ansible.builtin.wait_for_connection", "wait_for_connection":
		return taskResultFromResult(e.moduleWaitForConnection(context.Background(), mock, args))

	// Archive
	case "ansible.builtin.unarchive":
		return moduleUnarchiveWithClient(e, mock, args)
	case "ansible.builtin.archive":
		return moduleArchiveWithClient(e, mock, args)

	case "ansible.builtin.ping", "ping":
		return taskResultFromResult(e.modulePing(context.Background(), mock, args))

	case "ansible.builtin.debug":
		return taskResultFromResult(e.moduleDebug(host, task, args))

	case "ansible.builtin.set_fact":
		return taskResultFromResult(e.moduleSetFact(host, args))

	case "ansible.builtin.setup":
		return taskResultFromResult(e.moduleSetup(context.Background(), host, mock, args))

	// HTTP
	case "ansible.builtin.uri":
		return moduleURIWithClient(e, mock, args)

	// Firewall
	case "community.general.ufw", "ansible.builtin.ufw":
		return moduleUFWWithClient(e, mock, args)

	// Docker
	case "community.docker.docker_compose_v2", "community.docker.docker_compose", "ansible.builtin.docker_compose":
		return moduleDockerComposeWithClient(e, mock, args)

	default:
		return nil, mockError("executeModuleWithMock", sprintf("unsupported module %s", module))
	}
}

// --- Module shims that accept the mock interface ---
// These mirror the module methods but accept our mock instead of *SSHClient.

func taskResultFromResult(result core.Result) (*TaskResult, error) {
	if !result.OK {
		if err, ok := result.Value.(error); ok {
			return nil, err
		}
		return nil, mockError("taskResultFromResult", result.Error())
	}
	taskResult, _ := result.Value.(*TaskResult)
	return taskResult, nil
}

func requireTaskResult(t *core.T, result core.Result) *TaskResult {
	taskResult, err := taskResultFromResult(result)
	core.RequireNoError(t, err)
	return taskResult
}

func executorForModuleHelper(e *Executor) *Executor {
	if e != nil {
		return e
	}
	return NewExecutor("/tmp")
}

func moduleShellWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleShell(context.Background(), client, args))
}

func moduleCommandWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleCommand(context.Background(), client, args))
}

func moduleRawWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleRaw(context.Background(), client, args))
}

func moduleScriptWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleScript(context.Background(), client, args))
}

func moduleCopyWithClient(e *Executor, client sshExecutorClient, args map[string]any, host string, task *Task) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleCopy(context.Background(), client, args, host, task))
}

func moduleTemplateWithClient(e *Executor, client sshExecutorClient, args map[string]any, host string, task *Task) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleTemplate(context.Background(), client, args, host, task))
}

func moduleFileWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleFile(context.Background(), client, args))
}

func moduleLineinfileWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleLineinfile(context.Background(), client, args))
}

func moduleBlockinfileWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleBlockinfile(context.Background(), client, args))
}

func moduleStatWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleStat(context.Background(), client, args))
}

func moduleServiceWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleService(context.Background(), client, args))
}

func moduleSystemdWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleSystemd(context.Background(), client, args))
}

func moduleAptWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleApt(context.Background(), client, args))
}

func moduleAptKeyWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleAptKey(context.Background(), client, args))
}

func moduleAptRepositoryWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleAptRepository(context.Background(), client, args))
}

func modulePackageWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).modulePackage(context.Background(), client, args))
}

func moduleYumWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleYum(context.Background(), client, args))
}

func moduleDnfWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleDnf(context.Background(), client, args))
}

func moduleRPMWithClient(client sshExecutorClient, args map[string]any, manager string) (*TaskResult, error) {
	return taskResultFromResult(NewExecutor("/tmp").moduleRPM(context.Background(), client, args, manager))
}

func modulePipWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).modulePip(context.Background(), client, args))
}

func moduleUserWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleUser(context.Background(), client, args))
}

func moduleGroupWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleGroup(context.Background(), client, args))
}

func moduleCronWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleCron(context.Background(), client, args))
}

func moduleAuthorizedKeyWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleAuthorizedKey(context.Background(), client, args))
}

func moduleGitWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleGit(context.Background(), client, args))
}

func moduleUnarchiveWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleUnarchive(context.Background(), client, args))
}

func moduleArchiveWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleArchive(context.Background(), client, args))
}

func moduleURIWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleURI(context.Background(), client, args))
}

func moduleUFWWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleUFW(context.Background(), client, args))
}

func moduleDockerComposeWithClient(e *Executor, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return taskResultFromResult(executorForModuleHelper(e).moduleDockerCompose(context.Background(), client, args))
}

// --- String helpers for assertions ---

// containsSubstring checks if any executed command contains the given substring.
func (m *MockSSHClient) containsSubstring(sub string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cmd := range m.executed {
		if contains(cmd.Cmd, sub) {
			return true
		}
	}
	return false
}
