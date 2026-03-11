---
title: Architecture
description: Internal architecture of go-ansible -- key types, data flow, module dispatch, templating, and SSH transport.
---

# Architecture

This document explains how `go-ansible` works internally. The package is a single flat Go package (`package ansible`) with four distinct layers: **types**, **parser**, **executor**, and **SSH transport**.

## High-Level Data Flow

```
Playbook YAML ──► Parser ──► []Play ──► Executor ──► Module Handlers ──► SSH Client ──► Remote Host
                               │                         │
Inventory YAML ──► Parser ──► Inventory        Callbacks (OnTaskStart, OnTaskEnd, ...)
```

1. The **Parser** reads YAML files and produces typed Go structs (`Play`, `Task`, `Inventory`).
2. The **Executor** iterates over plays and tasks, resolving hosts from inventory, evaluating conditions, expanding templates, and dispatching each task to the appropriate module handler.
3. **Module handlers** translate Ansible module semantics (e.g. `apt`, `copy`, `systemd`) into shell commands.
4. The **SSH client** executes those commands on remote hosts and returns stdout, stderr, and exit codes.

## Key Types

All types live in `types.go`.

### Playbook and Play

A `Playbook` is simply a wrapper around a slice of `Play` values. Each `Play` targets a set of hosts and contains ordered task lists:

```go
type Play struct {
    Name        string            // Human-readable play name
    Hosts       string            // Host pattern ("all", "webservers", "web1")
    Become      bool              // Enable privilege escalation
    BecomeUser  string            // User to escalate to (default: root)
    GatherFacts *bool             // Whether to collect host facts (default: true)
    Vars        map[string]any    // Play-scoped variables
    PreTasks    []Task            // Run before roles
    Tasks       []Task            // Main task list
    PostTasks   []Task            // Run after tasks
    Roles       []RoleRef         // Roles to apply
    Handlers    []Task            // Triggered by notify
    Tags        []string          // Play-level tags
    Environment map[string]string // Environment variables
    Serial      any               // Batch size (int or string)
}
```

### Task

A `Task` is the fundamental unit of work. The `Module` and `Args` fields are not part of the YAML schema directly -- they are extracted at parse time by custom `UnmarshalYAML` logic that scans for known module keys:

```go
type Task struct {
    Name         string            // Human-readable task name
    Module       string            // Derived: module name (e.g. "apt", "shell")
    Args         map[string]any    // Derived: module arguments
    Register     string            // Variable name to store result
    When         any               // Condition (string or []string)
    Loop         any               // Items to iterate ([]any or var reference)
    LoopControl  *LoopControl      // Loop variable naming
    Notify       any               // Handler(s) to trigger on change
    IgnoreErrors bool              // Continue on failure
    Become       *bool             // Task-level privilege escalation
    Block        []Task            // Block/rescue/always error handling
    Rescue       []Task
    Always       []Task
    // ... and more (tags, retries, delegate_to, etc.)
}
```

Free-form module syntax (e.g. `shell: echo hello`) is stored as `Args["_raw_params"]`.

### TaskResult

Every module handler returns a `TaskResult`:

```go
type TaskResult struct {
    Changed  bool           // Whether the task made a change
    Failed   bool           // Whether the task failed
    Skipped  bool           // Whether the task was skipped
    Msg      string         // Human-readable message
    Stdout   string         // Command stdout
    Stderr   string         // Command stderr
    RC       int            // Exit code
    Results  []TaskResult   // Per-item results (for loops)
    Data     map[string]any // Module-specific structured data
    Duration time.Duration  // Execution time
}
```

Results can be captured via `register:` and referenced in later `when:` conditions or `{{ var.stdout }}` templates.

### Inventory

```go
type Inventory struct {
    All *InventoryGroup
}

type InventoryGroup struct {
    Hosts    map[string]*Host
    Children map[string]*InventoryGroup
    Vars     map[string]any
}

type Host struct {
    AnsibleHost              string
    AnsiblePort              int
    AnsibleUser              string
    AnsiblePassword          string
    AnsibleSSHPrivateKeyFile string
    AnsibleConnection        string
    AnsibleBecomePassword    string
    Vars                     map[string]any  // Custom vars via inline YAML
}
```

Host resolution supports the `all` pattern, group names, and individual host names. The `GetHosts()` function performs pattern matching, and `GetHostVars()` collects variables by walking the group hierarchy (group vars are inherited, host vars take precedence).

### Facts

The `Facts` struct holds basic system information gathered from remote hosts via shell commands:

```go
type Facts struct {
    Hostname     string
    FQDN         string
    Distribution string  // e.g. "ubuntu"
    Version      string  // e.g. "24.04"
    Architecture string  // e.g. "x86_64"
    Kernel       string  // e.g. "6.8.0"
    // ...
}
```

Facts are gathered automatically at the start of each play (unless `gather_facts: false`) and are available for templating via `{{ ansible_hostname }}`, `{{ ansible_distribution }}`, etc.

## Parser

The parser (`parser.go`) handles four types of YAML file:

| Method | Input | Output |
|--------|-------|--------|
| `ParsePlaybook(path)` | Playbook YAML | `[]Play` |
| `ParseInventory(path)` | Inventory YAML | `*Inventory` |
| `ParseTasks(path)` | Task list YAML | `[]Task` |
| `ParseRole(name, tasksFrom)` | Role directory | `[]Task` |

Iterator variants (`ParsePlaybookIter`, `ParseTasksIter`, `GetHostsIter`, `AllHostsIter`) return `iter.Seq` values for lazy, range-based consumption.

### Module Extraction

Ansible tasks embed the module name as a YAML key rather than a fixed field. The parser handles this via a custom `UnmarshalYAML` on `Task`:

1. Decode the YAML node into both a raw `map[string]any` and the typed struct.
2. Iterate over the map keys, skipping known structural keys (`name`, `register`, `when`, etc.).
3. Match remaining keys against `KnownModules` (a list of 68 entries covering both FQCN and short forms).
4. Any key containing a dot that is not in the known list is also accepted (to support collection modules).
5. Store the module name in `Task.Module` and its value in `Task.Args`.

The `with_items` legacy syntax is automatically normalised to `Loop`.

### Role Resolution

`ParseRole` searches multiple directory patterns for the role's `tasks/main.yml` (or a custom `tasks_from` file):

- `{basePath}/roles/{name}/tasks/`
- `{basePath}/../roles/{name}/tasks/`
- `{basePath}/playbooks/roles/{name}/tasks/`

It also loads `defaults/main.yml` and `vars/main.yml` from the role directory, merging them into the parser's variable context.

## Executor

The `Executor` (`executor.go`) is the orchestration engine. It holds all runtime state:

```go
type Executor struct {
    parser    *Parser
    inventory *Inventory
    vars      map[string]any                    // Global variables
    facts     map[string]*Facts                 // Per-host facts
    results   map[string]map[string]*TaskResult // host -> register_name -> result
    handlers  map[string][]Task                 // Handler registry
    notified  map[string]bool                   // Which handlers have been triggered
    clients   map[string]*SSHClient             // Cached SSH connections

    // Callbacks
    OnPlayStart func(play *Play)
    OnTaskStart func(host string, task *Task)
    OnTaskEnd   func(host string, task *Task, result *TaskResult)
    OnPlayEnd   func(play *Play)

    // Options
    Limit     string
    Tags      []string
    SkipTags  []string
    CheckMode bool
    Verbose   int
}
```

### Play Execution Order

For each play, the executor follows this sequence:

1. Resolve target hosts from inventory (applying `Limit` if set).
2. Merge play-level `vars` into the global variable context.
3. **Gather facts** on each host (unless `gather_facts: false`).
4. Execute **pre_tasks** in order.
5. Execute **roles** in order.
6. Execute **tasks** in order.
7. Execute **post_tasks** in order.
8. Run any **notified handlers**.

### Condition Evaluation

The `evaluateWhen` method processes `when:` clauses. It supports:

- Boolean literals: `true`, `false`, `True`, `False`
- Negation: `not <condition>`
- Registered variable checks: `result is defined`, `result is success`, `result is failed`, `result is changed`, `result is skipped`
- Variable truthiness: checks `vars` map for the condition as a key, evaluating booleans, non-empty strings, and non-zero integers
- Default filter handling: `var | default(value)` always evaluates to true (permissive)
- Multiple conditions (AND semantics): all must pass

### Templating

Jinja2-style `{{ expression }}` placeholders are resolved by `templateString`. The resolution order is:

1. Jinja2 filters (`| default(...)`, `| bool`, `| trim`)
2. `lookup()` expressions (`env`, `file`)
3. Registered variable dot-access (`result.stdout`, `result.rc`)
4. Global variables (`vars` map)
5. Task-local variables
6. Host variables from inventory
7. Host facts (`ansible_hostname`, `ansible_distribution`, etc.)

Unresolved expressions are returned verbatim (e.g. `{{ undefined_var }}` remains as-is).

Template file processing (`TemplateFile`) performs a basic Jinja2-to-Go-template conversion for the `template` module, with a fallback to simple `{{ }}` substitution.

### Loops

The executor supports `loop:` (and the legacy `with_items:`) with configurable loop variable names via `loop_control`. Loop results are aggregated into a single `TaskResult` with a `Results` slice.

### Block / Rescue / Always

Error handling blocks follow Ansible semantics:

1. Execute all tasks in `block`.
2. If any `block` task fails and `rescue` is defined, execute the rescue tasks.
3. Always execute `always` tasks regardless of success or failure.

### Handler Notification

When a task produces `Changed: true` and has a `notify` field, the named handler(s) are marked. After all tasks in the play complete, notified handlers are executed in the order they are defined.

## Module Dispatch

The `executeModule` method in `modules.go` normalises the module name (adding the `ansible.builtin.` prefix if absent) and dispatches to the appropriate handler via a `switch` statement.

Each module handler:

1. Extracts arguments using helper functions (`getStringArg`, `getBoolArg`).
2. Constructs one or more shell commands.
3. Executes them via the SSH client.
4. Parses the output into a `TaskResult`.

Some modules (e.g. `debug`, `set_fact`, `fail`, `assert`) are purely local and do not require SSH.

### Argument Helpers

```go
func getStringArg(args map[string]any, key, def string) string
func getBoolArg(args map[string]any, key string, def bool) bool
```

These handle type coercion (e.g. `"yes"`, `"true"`, `"1"` all evaluate to `true` for `getBoolArg`).

## SSH Transport

The `SSHClient` (`ssh.go`) manages connections to remote hosts.

### Authentication

Authentication methods are tried in order:

1. **Explicit key file** -- from `ansible_ssh_private_key_file` or the `KeyFile` config field.
2. **Default keys** -- `~/.ssh/id_ed25519`, then `~/.ssh/id_rsa`.
3. **Password** -- both `ssh.Password` and `ssh.KeyboardInteractive` are registered.

### Host Key Verification

The client uses `~/.ssh/known_hosts` for host key verification via `golang.org/x/crypto/ssh/knownhosts`. If the file does not exist, it is created automatically.

### Privilege Escalation (become)

When `become` is enabled (at play or task level), commands are wrapped with `sudo`:

- **With password**: `sudo -S -u {user} bash -c '{command}'` (password piped via stdin)
- **Without password**: `sudo -n -u {user} bash -c '{command}'` (passwordless sudo)

### File Transfer

File uploads use `cat >` piped via an SSH session's stdin rather than SCP. This approach is simpler and works well with `become`, as the `sudo` wrapper can be applied to the `cat` command. Downloads use `cat` with quoted paths.

### Connection Lifecycle

SSH connections are created lazily (on first use per host) and cached in the executor's `clients` map. The `Executor.Close()` method terminates all open connections.

## Concurrency

The executor uses a `sync.RWMutex` to protect shared state (variables, results, SSH client cache). Tasks within a play execute sequentially per host. The `clients` map is locked during connection creation to prevent duplicate connections to the same host.
