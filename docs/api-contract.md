# API Contract

`CODEX.md` is not present in this repository. This extraction follows the repo conventions documented in `CLAUDE.md`.

Function and method coverage percentages below come from `go test -coverprofile=/tmp/ansible.cover ./...` run on 2026-03-23. Type rows list the tests that exercise or validate each type because Go coverage does not report percentages for type declarations. Exported variables are intentionally excluded because the task requested exported types, functions, and methods only.

## Package `ansible`: Types

| Name | Signature | Description | Test coverage |
| --- | --- | --- | --- |
| `Playbook` | `type Playbook struct` | Top-level playbook container for an inline list of plays. | No direct tests; parser tests work with `[]Play` rather than `Playbook`. |
| `Play` | `type Play struct` | Declares play metadata, host targeting, vars, lifecycle task lists, roles, handlers, tags, and execution settings. | Exercised by `parser_test.go` playbook cases and `executor_extra_test.go: TestParsePlaybookIter_Good`. |
| `RoleRef` | `type RoleRef struct` | Represents a role reference, including string and map forms plus role-scoped vars, tags, and `when`. | Directly validated by `types_test.go: TestRoleRef_UnmarshalYAML_*`; also parsed in `parser_test.go: TestParsePlaybook_Good_RoleRefs`. |
| `Task` | `type Task struct` | Represents a task, including module selection, args, conditions, loops, block/rescue/always sections, includes, notify, and privilege controls. | Directly validated by `types_test.go: TestTask_UnmarshalYAML_*`; also exercised throughout `parser_test.go` and module dispatch tests. |
| `LoopControl` | `type LoopControl struct` | Configures loop variable names, labels, pauses, and extended loop metadata. | No dedicated tests; only reachable through `Task.loop_control` parsing. |
| `TaskResult` | `type TaskResult struct` | Standard task execution result with change/failure state, output streams, loop subresults, module data, and duration. | Directly validated by `types_test.go: TestTaskResult_*`; also used across executor and module tests. |
| `Inventory` | `type Inventory struct` | Root Ansible inventory object keyed under `all`. | Directly validated by `types_test.go: TestInventory_UnmarshalYAML_Good_Complex`; also exercised by parser and host-resolution tests. |
| `InventoryGroup` | `type InventoryGroup struct` | Inventory group containing hosts, child groups, and inherited vars. | Exercised by `parser_test.go` inventory and host-matching cases plus `executor_extra_test.go: TestAllHostsIter_Good`. |
| `Host` | `type Host struct` | Host-level connection settings plus inline custom vars. | Exercised by inventory parsing and host-var inheritance tests in `types_test.go` and `parser_test.go`. |
| `Facts` | `type Facts struct` | Captures gathered host facts such as identity, OS, kernel, memory, CPUs, and primary IPv4. | Directly validated by `types_test.go: TestFacts_Struct`; also exercised by fact resolution tests in `modules_infra_test.go` and `executor_extra_test.go`. |
| `Parser` | `type Parser struct` | Stateful YAML parser for playbooks, inventories, task files, and roles. | Directly exercised by `parser_test.go: TestNewParser_Good` and all parser method tests. |
| `Executor` | `type Executor struct` | Playbook execution engine holding parser/inventory state, callbacks, vars/results/facts, SSH clients, and run options. | Directly exercised by `executor_test.go`, `executor_extra_test.go`, and module tests; the public `Run` entrypoint itself is untested. |
| `SSHClient` | `type SSHClient struct` | Lazy SSH client that tracks connection, auth, privilege-escalation state, and timeout. | Constructor and become state are covered in `ssh_test.go` and `modules_infra_test.go`; network and file-transfer methods are untested. |
| `SSHConfig` | `type SSHConfig struct` | Public configuration for SSH connection, auth, become, and timeout defaults. | Directly exercised by `ssh_test.go: TestNewSSHClient`, `TestSSHConfig_Defaults`, and become-state tests in `modules_infra_test.go`. |

## Package `ansible`: Parser and Inventory API

| Name | Signature | Description | Test coverage |
| --- | --- | --- | --- |
| `(*RoleRef).UnmarshalYAML` | `func (r *RoleRef) UnmarshalYAML(unmarshal func(any) error) error` | Accepts either a scalar role name or a structured role reference and normalises `name` into `Role`. | `91.7%`; covered by `types_test.go: TestRoleRef_UnmarshalYAML_*`. |
| `(*Task).UnmarshalYAML` | `func (t *Task) UnmarshalYAML(node *yaml.Node) error` | Decodes known task fields, extracts the module key dynamically, and converts `with_items` into `Loop`. | `87.5%`; covered by `types_test.go: TestTask_UnmarshalYAML_*`. |
| `NewParser` | `func NewParser(basePath string) *Parser` | Constructs a parser rooted at `basePath` with an empty variable map. | `100.0%`; covered by `parser_test.go: TestNewParser_Good`. |
| `(*Parser).ParsePlaybook` | `func (p *Parser) ParsePlaybook(path string) ([]Play, error)` | Reads a playbook YAML file into plays and post-processes each play's tasks. | `90.0%`; covered by `parser_test.go: TestParsePlaybook_*`. |
| `(*Parser).ParsePlaybookIter` | `func (p *Parser) ParsePlaybookIter(path string) (iter.Seq[Play], error)` | Wraps `ParsePlaybook` with an iterator over plays. | `85.7%`; covered by `executor_extra_test.go: TestParsePlaybookIter_*`. |
| `(*Parser).ParseInventory` | `func (p *Parser) ParseInventory(path string) (*Inventory, error)` | Reads an inventory YAML file into the public inventory model. | `100.0%`; covered by `parser_test.go: TestParseInventory_*`. |
| `(*Parser).ParseTasks` | `func (p *Parser) ParseTasks(path string) ([]Task, error)` | Reads a task file and extracts module metadata for each task. | `90.0%`; covered by `parser_test.go: TestParseTasks_*`. |
| `(*Parser).ParseTasksIter` | `func (p *Parser) ParseTasksIter(path string) (iter.Seq[Task], error)` | Wraps `ParseTasks` with an iterator over tasks. | `85.7%`; covered by `executor_extra_test.go: TestParseTasksIter_*`. |
| `(*Parser).ParseRole` | `func (p *Parser) ParseRole(name string, tasksFrom string) ([]Task, error)` | Resolves a role across several search paths, loads role defaults and vars, then parses the requested task file. | `0.0%`; no automated tests found. |
| `NormalizeModule` | `func NormalizeModule(name string) string` | Canonicalises short module names to `ansible.builtin.*` while leaving dotted names unchanged. | `100.0%`; covered by `parser_test.go: TestNormalizeModule_*`. |
| `GetHosts` | `func GetHosts(inv *Inventory, pattern string) []string` | Resolves `all`, `localhost`, group names, or explicit host names from inventory. | `100.0%`; covered by `parser_test.go: TestGetHosts_*`. |
| `GetHostsIter` | `func GetHostsIter(inv *Inventory, pattern string) iter.Seq[string]` | Iterator wrapper over `GetHosts`. | `80.0%`; covered by `executor_extra_test.go: TestGetHostsIter_Good`. |
| `AllHostsIter` | `func AllHostsIter(group *InventoryGroup) iter.Seq[string]` | Iterates every host in a group tree with deterministic key ordering. | `84.6%`; covered by `executor_extra_test.go: TestAllHostsIter_*`. |
| `GetHostVars` | `func GetHostVars(inv *Inventory, hostname string) map[string]any` | Collects effective variables for a host by merging group ancestry with host-specific connection vars and inline vars. | `100.0%`; covered by `parser_test.go: TestGetHostVars_*`. |

## Package `ansible`: Executor API

| Name | Signature | Description | Test coverage |
| --- | --- | --- | --- |
| `NewExecutor` | `func NewExecutor(basePath string) *Executor` | Constructs an executor with parser state, variable stores, handler tracking, and SSH client cache. | `100.0%`; covered by `executor_test.go: TestNewExecutor_Good`. |
| `(*Executor).SetInventory` | `func (e *Executor) SetInventory(path string) error` | Loads inventory from disk via the embedded parser and stores it on the executor. | `100.0%`; covered by `executor_extra_test.go: TestSetInventory_*`. |
| `(*Executor).SetInventoryDirect` | `func (e *Executor) SetInventoryDirect(inv *Inventory)` | Replaces the executor inventory with a caller-supplied value. | `100.0%`; covered by `executor_test.go: TestSetInventoryDirect_Good`. |
| `(*Executor).SetVar` | `func (e *Executor) SetVar(key string, value any)` | Stores a variable in the executor-scoped variable map under lock. | `100.0%`; covered by `executor_test.go: TestSetVar_Good`. |
| `(*Executor).Run` | `func (e *Executor) Run(ctx context.Context, playbookPath string) error` | Parses a playbook and executes each play in order. | `0.0%`; no automated tests found for the public run path. |
| `(*Executor).Close` | `func (e *Executor) Close()` | Closes all cached SSH clients and resets the client cache. | `80.0%`; covered by `executor_test.go: TestClose_Good_EmptyClients`. |
| `(*Executor).TemplateFile` | `func (e *Executor) TemplateFile(src, host string, task *Task) (string, error)` | Reads a template, performs a basic Jinja2-to-Go-template conversion, and falls back to string substitution if parsing or execution fails. | `75.0%`; exercised indirectly by `modules_file_test.go: TestModuleTemplate_*`. |

## Package `ansible`: SSH API

| Name | Signature | Description | Test coverage |
| --- | --- | --- | --- |
| `NewSSHClient` | `func NewSSHClient(cfg SSHConfig) (*SSHClient, error)` | Applies SSH defaults and constructs a client from the supplied config. | `100.0%`; covered by `ssh_test.go: TestNewSSHClient`, `TestSSHConfig_Defaults`, plus become-state tests in `modules_infra_test.go`. |
| `(*SSHClient).Connect` | `func (c *SSHClient) Connect(ctx context.Context) error` | Lazily establishes an SSH connection using key, password, and `known_hosts` handling. | `0.0%`; no automated tests found. |
| `(*SSHClient).Close` | `func (c *SSHClient) Close() error` | Closes the active SSH connection if one exists. | `0.0%`; no automated tests found. |
| `(*SSHClient).Run` | `func (c *SSHClient) Run(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error)` | Executes a remote command, optionally wrapped with `sudo`, and returns streams plus exit code. | `0.0%`; no automated tests found for the concrete SSH implementation. |
| `(*SSHClient).RunScript` | `func (c *SSHClient) RunScript(ctx context.Context, script string) (stdout, stderr string, exitCode int, err error)` | Executes a remote shell script through a heredoc wrapper. | `0.0%`; no automated tests found. |
| `(*SSHClient).Upload` | `func (c *SSHClient) Upload(ctx context.Context, local io.Reader, remote string, mode os.FileMode) error` | Uploads content to a remote file, creating parent directories and handling `sudo` writes when needed. | `0.0%`; no automated tests found. |
| `(*SSHClient).Download` | `func (c *SSHClient) Download(ctx context.Context, remote string) ([]byte, error)` | Downloads a remote file by reading it with `cat`. | `0.0%`; no automated tests found. |
| `(*SSHClient).FileExists` | `func (c *SSHClient) FileExists(ctx context.Context, path string) (bool, error)` | Checks remote path existence with `test -e`. | `0.0%`; no automated tests found. |
| `(*SSHClient).Stat` | `func (c *SSHClient) Stat(ctx context.Context, path string) (map[string]any, error)` | Returns a minimal remote stat map describing existence and directory state. | `0.0%`; no automated tests found. |
| `(*SSHClient).SetBecome` | `func (c *SSHClient) SetBecome(become bool, user, password string)` | Updates the privilege-escalation settings stored on the client. | `100.0%`; covered by `modules_infra_test.go: TestBecome_Infra_Good_*`. |

## Package `anscmd`: CLI API

| Name | Signature | Description | Test coverage |
| --- | --- | --- | --- |
| `Register` | `func Register(c *core.Core)` | Registers the `ansible` and `ansible/test` CLI commands and their flags on a `core.Core` instance. | `0.0%`; `cmd/ansible` has no test files. |
