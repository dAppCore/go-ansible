# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`core/go-ansible` is a pure Go Ansible playbook engine. It parses YAML playbooks, inventories, and roles, then executes tasks on remote hosts via SSH. 42 module handler implementations (plus 3 community modules), Jinja2-compatible templating, privilege escalation (become), and event-driven callbacks. This is a library — there is no standalone binary. The CLI integration lives in `cmd/ansible/` and is compiled as part of the `core` CLI binary.

## Build & Test

```bash
go build ./...                                    # verify compilation
go test ./...                                     # run all tests
go test -race ./...                               # with race detection
go test -run TestParsePlaybook_Good_SimplePlay     # single test
go test -v ./...                                  # verbose output
go work sync                                      # if in a Go workspace
```

No SSH access is needed for tests — the suite uses a mock SSH client (`mock_ssh_test.go`).

## Architecture

Single flat `ansible` package with four layers:

```
Playbook YAML ──► Parser ──► []Play ──► Executor ──► Module Handlers ──► SSH Client ──► Remote Host
                               │                         │
Inventory YAML ──► Parser ──► Inventory        Callbacks (OnPlayStart, OnTaskEnd, ...)
```

- **`types.go`** — Core structs (`Playbook`, `Play`, `Task`, `TaskResult`, `Inventory`, `Host`, `Facts`) and `KnownModules` registry (80 entries: both FQCN `ansible.builtin.*` and short forms).
- **`parser.go`** — YAML parsing for playbooks, inventories, tasks, and roles. Custom `Task.UnmarshalYAML` scans map keys against `KnownModules` to extract the module name and args (since Ansible embeds the module name as a YAML key, not a fixed field). Free-form syntax (`shell: echo hello`) is stored as `Args["_raw_params"]`. Iterator variants (`ParsePlaybookIter`, `ParseTasksIter`, etc.) return `iter.Seq` values.
- **`executor.go`** — Orchestration engine: host resolution from inventory, play execution order (gather facts → pre_tasks → roles → tasks → post_tasks → notified handlers), `when:` condition evaluation, `{{ }}` Jinja2-style templating with filter support, loop execution, block/rescue/always, handler notification.
- **`modules.go`** — 41 module handler implementations dispatched via a `switch` on the normalised module name. Each handler extracts args via `getStringArg`/`getBoolArg`, constructs shell commands, runs them via SSH, and returns a `TaskResult`.
- **`ssh.go`** — SSH client with lazy connection, auth chain (key file → default keys → password), `known_hosts` verification, become/sudo wrapping, file transfer via `cat >` piped through stdin.
- **`cmd/ansible/`** — CLI command registration via `core/cli`. Provides `ansible <playbook>` and `ansible test <host>` subcommands with flags for inventory, limit, tags, extra-vars, verbosity, and check mode.

## Adding a New Module

1. Add both FQCN and short form to `KnownModules` in `types.go`
2. Add the dispatch case in `executeModule` switch in `modules.go`
3. Implement `module{Name}(ctx, client, args)` method on `Executor` in `modules.go`
4. Write tests in the appropriate `modules_*_test.go` file using mock SSH infrastructure

If adding new YAML keys to `Task`, update the `knownKeys` map in `Task.UnmarshalYAML` (`parser.go`) to prevent them being mistaken for module names.

## Test Organisation

| File | Coverage |
|------|----------|
| `types_test.go` | YAML unmarshalling for Task, RoleRef, Inventory, Facts |
| `parser_test.go` | Playbook, inventory, and task file parsing |
| `executor_test.go` | Executor lifecycle, conditions, templating, loops, tags |
| `ssh_test.go` | SSH client construction and defaults |
| `modules_cmd_test.go` | Command modules: shell, command, raw, script |
| `modules_file_test.go` | File modules: copy, template, file, lineinfile, stat, slurp, fetch, get_url |
| `modules_svc_test.go` | Service modules: service, systemd, user, group |
| `modules_infra_test.go` | Infrastructure modules: apt, pip, git, unarchive, ufw, docker_compose |
| `modules_adv_test.go` | Advanced modules: debug, fail, assert, set_fact, pause, wait_for, uri, blockinfile, cron, hostname, sysctl, reboot |

## Coding Standards

- **UK English** in comments and documentation (colour, organisation, centre)
- Test naming: `_Good` (happy path), `_Bad` (expected errors), `_Ugly` (edge cases/panics)
- Use `coreerr.E(scope, message, err)` from `go-log` for all errors in production code (never `fmt.Errorf`)
- Tests use `testify/assert` (soft) and `testify/require` (hard)
- Licence: EUPL-1.2
