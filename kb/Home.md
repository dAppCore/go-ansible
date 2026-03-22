# go-ansible

Module: `dappco.re/go/core/ansible`

Pure Go Ansible executor that parses and runs Ansible playbooks without requiring the Python ansible binary. Supports SSH-based remote execution, inventory parsing, Jinja2-like templating, module execution, roles, handlers, loops, blocks, and conditionals.

## Architecture

| File | Purpose |
|------|---------|
| `types.go` | Data types: `Playbook`, `Play`, `Task`, `TaskResult`, `Inventory`, `Host`, `Facts`, `KnownModules` |
| `parser.go` | YAML parsing for playbooks, inventory, roles, and task files |
| `executor.go` | Playbook execution engine with SSH client management, templating, conditionals |
| `ssh.go` | `SSHClient` for remote command execution, file upload/download |
| `modules.go` | Ansible module implementations (shell, copy, template, file, service, etc.) |

CLI registration in `cmd/ansible/`.

## Key Types

### Core Types

- **`Executor`** — Runs playbooks: `Run()`, `SetInventory()`, `SetVar()`. Supports callbacks: `OnPlayStart`, `OnTaskStart`, `OnTaskEnd`, `OnPlayEnd`. Options: `Limit`, `Tags`, `SkipTags`, `CheckMode`, `Diff`, `Verbose`
- **`Parser`** — Parses YAML: `ParsePlaybook()`, `ParseInventory()`, `ParseRole()`, `ParseTasks()`
- **`SSHClient`** — SSH operations: `Connect()`, `Run()`, `RunScript()`, `Upload()`, `Download()`, `FileExists()`, `Stat()`, `SetBecome()`
- **`SSHConfig`** — Connection config: `Host`, `Port`, `User`, `Password`, `KeyFile`, `Become`, `BecomeUser`, `BecomePass`, `Timeout`

### Playbook Types

- **`Play`** — Single play: `Name`, `Hosts`, `Become`, `Vars`, `PreTasks`, `Tasks`, `PostTasks`, `Roles`, `Handlers`
- **`Task`** — Single task: `Name`, `Module`, `Args`, `Register`, `When`, `Loop`, `LoopControl`, `Block`, `Rescue`, `Always`, `Notify`, `IncludeTasks`, `ImportTasks`
- **`TaskResult`** — Execution result: `Changed`, `Failed`, `Skipped`, `Msg`, `Stdout`, `Stderr`, `RC`, `Results` (for loops)
- **`RoleRef`** — Role reference with vars and conditions

### Inventory Types

- **`Inventory`** — Top-level with `All` group
- **`InventoryGroup`** — `Hosts`, `Children`, `Vars`
- **`Host`** — Connection details: `AnsibleHost`, `AnsiblePort`, `AnsibleUser`, `AnsibleSSHPrivateKeyFile`
- **`Facts`** — Gathered facts: `Hostname`, `FQDN`, `OS`, `Distribution`, `Architecture`, `Kernel`, `Memory`, `CPUs`

## Usage

```go
import "dappco.re/go/core/ansible"

executor := ansible.NewExecutor("/path/to/playbooks")
executor.SetInventory("inventory/hosts.yml")
executor.SetVar("deploy_version", "1.2.3")

executor.OnTaskStart = func(host string, task *ansible.Task) {
    fmt.Printf("[%s] %s\n", host, task.Name)
}

err := executor.Run(ctx, "deploy.yml")
defer executor.Close()
```

## Dependencies

- `dappco.re/go/core/log` — Structured logging and errors
- `golang.org/x/crypto/ssh` — SSH client
- `golang.org/x/crypto/ssh/knownhosts` — Host key verification
- `gopkg.in/yaml.v3` — YAML parsing
