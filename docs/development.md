---
title: Development
description: How to build, test, and contribute to go-ansible.
---

# Development

## Prerequisites

- **Go 1.26+** (the module requires Go 1.26 features)
- SSH access to a test host (for integration testing, not required for unit tests)

## Building

The package is a library -- there is no standalone binary. The CLI integration lives in `cmd/ansible/` and is compiled as part of the `core` CLI binary.

```bash
# Verify the module compiles
go build ./...

# If working within the Go workspace
go work sync
```

## Running Tests

```bash
# Run all tests
go test ./...

# Run tests with race detection
go test -race ./...

# Run a specific test
go test -run TestParsePlaybook_Good_SimplePlay

# Run tests with verbose output
go test -v ./...
```

The test suite uses a mock SSH client infrastructure (`mock_ssh_test.go`) to test module handlers without requiring real SSH connections. Tests are organised into separate files by category:

| File | Coverage |
|------|----------|
| `types_test.go` | YAML unmarshalling for `Task`, `RoleRef`, `Inventory`, `Facts` |
| `parser_test.go` | Playbook, inventory, and task file parsing |
| `executor_test.go` | Executor lifecycle, condition evaluation, templating, loops, tag filtering |
| `ssh_test.go` | SSH client construction and defaults |
| `mock_ssh_test.go` | Mock SSH infrastructure for module tests |
| `modules_cmd_test.go` | Command modules: `shell`, `command`, `raw`, `script` |
| `modules_file_test.go` | File modules: `copy`, `template`, `file`, `lineinfile`, `stat`, `slurp`, `fetch`, `get_url` |
| `modules_svc_test.go` | Service modules: `service`, `systemd`, `user`, `group` |
| `modules_infra_test.go` | Infrastructure modules: `apt`, `pip`, `git`, `unarchive`, `ufw`, `docker_compose` |
| `modules_adv_test.go` | Advanced modules: `debug`, `fail`, `assert`, `set_fact`, `pause`, `wait_for`, `uri`, `blockinfile`, `cron`, `hostname`, `sysctl`, `reboot` |

### Test Naming Convention

Tests follow the `_Good` / `_Bad` / `_Ugly` suffix pattern:

- **`_Good`** -- Happy path: valid inputs produce expected outputs
- **`_Bad`** -- Expected error conditions: invalid inputs are handled gracefully
- **`_Ugly`** -- Edge cases: panics, nil inputs, boundary conditions

Example:

```go
func TestParsePlaybook_Good_SimplePlay(t *testing.T) { ... }
func TestParsePlaybook_Bad_MissingFile(t *testing.T) { ... }
func TestParsePlaybook_Bad_InvalidYAML(t *testing.T) { ... }
```

## Code Organisation

The package is intentionally flat -- a single `ansible` package with no sub-packages. This keeps the API surface small and avoids circular dependencies.

When adding new functionality:

- **New module**: Add a `module{Name}` method to `Executor` in `modules.go`, add the case to the `executeModule` switch statement, and add the module name to `KnownModules` in `types.go` (both FQCN and short forms). Write tests in the appropriate `modules_*_test.go` file.
- **New parser feature**: Extend the relevant `Parse*` method in `parser.go`. If it involves new YAML keys on `Task`, update the `knownKeys` map in `UnmarshalYAML` to prevent them from being mistakenly identified as module names.
- **New type**: Add to `types.go` with appropriate YAML and JSON struct tags.

## Coding Standards

- **UK English** in comments and documentation (colour, organisation, centre).
- All functions must have typed parameters and return types.
- Use `log.E(scope, message, err)` from `go-log` for contextual errors in SSH and parser code.
- Use `fmt.Errorf` with `%w` for wrapping errors in the executor.
- Test assertions use `testify/assert` (soft) and `testify/require` (hard, stops test on failure).

## Adding a New Module

Here is a walkthrough for adding a hypothetical `ansible.builtin.hostname` module (which already exists -- this is illustrative):

### 1. Register the module name

In `types.go`, add both forms to `KnownModules`:

```go
var KnownModules = []string{
    // ...existing entries...
    "ansible.builtin.hostname",
    // ...
    "hostname",
}
```

### 2. Add the dispatch case

In `modules.go`, inside `executeModule`:

```go
case "ansible.builtin.hostname":
    return e.moduleHostname(ctx, client, args)
```

### 3. Implement the handler

```go
func (e *Executor) moduleHostname(ctx context.Context, client *SSHClient, args map[string]any) (*TaskResult, error) {
    name := getStringArg(args, "name", "")
    if name == "" {
        return nil, errors.New("hostname: name is required")
    }

    cmd := fmt.Sprintf("hostnamectl set-hostname %q", name)
    stdout, stderr, rc, err := client.Run(ctx, cmd)
    if err != nil {
        return &TaskResult{Failed: true, Msg: err.Error(), Stderr: stderr, RC: rc}, nil
    }
    if rc != 0 {
        return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, Stderr: stderr, RC: rc}, nil
    }

    return &TaskResult{Changed: true, Msg: fmt.Sprintf("hostname set to %s", name)}, nil
}
```

### 4. Write tests

In the appropriate `modules_*_test.go` file, using the mock SSH infrastructure:

```go
func TestModuleHostname_Good(t *testing.T) {
    // Use mock SSH client to verify the command is constructed correctly
    // ...
}

func TestModuleHostname_Bad_MissingName(t *testing.T) {
    // Verify that omitting the name argument returns an error
    // ...
}
```

## Project Structure Reference

```
go-ansible/
  go.mod              Module definition (dappco.re/go/core/ansible)
  go.sum              Dependency checksums
  CLAUDE.md           AI assistant context file
  types.go            Core data types and KnownModules registry
  parser.go           YAML parsing (playbooks, inventories, roles)
  executor.go         Execution engine (orchestration, templating, conditions)
  modules.go          49 module handler implementations
  ssh.go              SSH client (auth, commands, file transfer, become)
  *_test.go           Test files (see table above)
  cmd/
    ansible/
      cmd.go          CLI command registration via core/cli
      ansible.go      CLI implementation (flags, runner, test subcommand)
```

## Licence

EUPL-1.2
