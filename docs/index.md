---
title: go-ansible
description: A pure Go Ansible playbook engine -- parses YAML playbooks, inventories, and roles, then executes tasks on remote hosts via SSH without requiring Python.
---

# go-ansible

`forge.lthn.ai/core/go-ansible` is a pure Go implementation of an Ansible playbook engine. It parses standard Ansible YAML playbooks, inventories, and roles, then executes tasks against remote hosts over SSH -- with no dependency on Python or the upstream `ansible-playbook` binary.

## Module Path

```
forge.lthn.ai/core/go-ansible
```

Requires **Go 1.26+**.

## Quick Start

### As a Library

```go
package main

import (
    "context"
    "fmt"

    ansible "forge.lthn.ai/core/go-ansible"
)

func main() {
    // Create an executor rooted at the playbook directory
    executor := ansible.NewExecutor("/path/to/project")
    defer executor.Close()

    // Load inventory
    if err := executor.SetInventory("/path/to/inventory.yml"); err != nil {
        panic(err)
    }

    // Optionally set extra variables
    executor.SetVar("deploy_version", "1.2.3")

    // Optionally limit to specific hosts
    executor.Limit = "web1"

    // Set up callbacks for progress reporting
    executor.OnTaskStart = func(host string, task *ansible.Task) {
        fmt.Printf("TASK [%s] on %s\n", task.Name, host)
    }
    executor.OnTaskEnd = func(host string, task *ansible.Task, result *ansible.TaskResult) {
        if result.Failed {
            fmt.Printf("  FAILED: %s\n", result.Msg)
        } else if result.Changed {
            fmt.Printf("  changed\n")
        } else {
            fmt.Printf("  ok\n")
        }
    }

    // Run the playbook
    ctx := context.Background()
    if err := executor.Run(ctx, "/path/to/playbook.yml"); err != nil {
        panic(err)
    }
}
```

### As a CLI Command

The package ships with a CLI integration under `cmd/ansible/` that registers a `core ansible` subcommand:

```bash
# Run a playbook
core ansible playbooks/deploy.yml -i inventory/production.yml

# Limit to a single host
core ansible site.yml -l web1

# Pass extra variables
core ansible deploy.yml -e "version=1.2.3" -e "env=prod"

# Dry run (check mode)
core ansible deploy.yml --check

# Increase verbosity
core ansible deploy.yml -vvv

# Test SSH connectivity to a host
core ansible test server.example.com -u root -i ~/.ssh/id_ed25519
```

**CLI flags:**

| Flag | Short | Description |
|------|-------|-------------|
| `--inventory` | `-i` | Inventory file or directory |
| `--limit` | `-l` | Restrict execution to matching hosts |
| `--tags` | `-t` | Only run tasks tagged with these values (comma-separated) |
| `--skip-tags` | | Skip tasks tagged with these values |
| `--extra-vars` | `-e` | Set additional variables (`key=value`, repeatable) |
| `--verbose` | `-v` | Increase verbosity (stack for more: `-vvv`) |
| `--check` | | Dry-run mode -- no changes are made |

## Package Layout

```
go-ansible/
  types.go          Core data types: Playbook, Play, Task, Inventory, Host, Facts
  parser.go         YAML parser for playbooks, inventories, tasks, roles
  executor.go       Execution engine: module dispatch, templating, conditions, loops
  modules.go        41 module implementations (shell, apt, docker-compose, etc.)
  ssh.go            SSH client with key/password auth, become/sudo, file transfer
  types_test.go     Tests for data types and YAML unmarshalling
  parser_test.go    Tests for the YAML parser
  executor_test.go  Tests for the executor engine
  ssh_test.go       Tests for SSH client construction
  mock_ssh_test.go  Mock SSH infrastructure for module tests
  modules_*_test.go Module-specific tests (cmd, file, svc, infra, adv)
  cmd/
    ansible/
      cmd.go        CLI command registration
      ansible.go    CLI implementation (flags, callbacks, output formatting)
```

## Supported Modules

41 module handlers are implemented, covering the most commonly used Ansible modules:

| Category | Modules |
|----------|---------|
| **Command execution** | `shell`, `command`, `raw`, `script` |
| **File operations** | `copy`, `template`, `file`, `lineinfile`, `blockinfile`, `stat`, `slurp`, `fetch`, `get_url` |
| **Package management** | `apt`, `apt_key`, `apt_repository`, `package`, `pip` |
| **Service management** | `service`, `systemd` |
| **User and group** | `user`, `group` |
| **HTTP** | `uri` |
| **Source control** | `git` |
| **Archive** | `unarchive` |
| **System** | `hostname`, `sysctl`, `cron`, `reboot`, `setup` |
| **Flow control** | `debug`, `fail`, `assert`, `set_fact`, `pause`, `wait_for`, `meta`, `include_vars` |
| **Community** | `community.general.ufw`, `ansible.posix.authorized_key`, `community.docker.docker_compose` |

Both fully-qualified collection names (e.g. `ansible.builtin.shell`) and short-form names (e.g. `shell`) are accepted.

## Dependencies

| Module | Purpose |
|--------|---------|
| `forge.lthn.ai/core/cli` | CLI framework (command registration, flags, styled output) |
| `forge.lthn.ai/core/go-log` | Structured logging and contextual error helper (`log.E()`) |
| `golang.org/x/crypto` | SSH protocol implementation (`crypto/ssh`, `crypto/ssh/knownhosts`) |
| `gopkg.in/yaml.v3` | YAML parsing for playbooks, inventories, and role files |
| `github.com/stretchr/testify` | Test assertions (test-only) |

## Licence

EUPL-1.2
