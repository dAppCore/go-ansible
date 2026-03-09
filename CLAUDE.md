# CLAUDE.md

## Project Overview

`core/go-ansible` is a pure Go Ansible playbook engine. It parses YAML playbooks, inventories, and roles, then executes tasks on remote hosts via SSH. 174 module implementations, Jinja2-compatible templating, privilege escalation (become), and event-driven callbacks.

## Build & Development

```bash
go test ./...
go test -race ./...
```

## Architecture

Single `ansible` package:

- `types.go` — Playbook, Play, Task, TaskResult, Inventory, Host, Facts structs
- `parser.go` — YAML parser for playbooks, inventories, tasks, roles
- `executor.go` — Task execution engine with module dispatch, templating, condition evaluation
- `modules.go` — 30+ module implementations (shell, copy, apt, systemd, git, docker-compose, etc.)
- `ssh.go` — SSH client with known_hosts verification, become/sudo, file transfer

## Dependencies

- `go-log` — Structured logging
- `golang.org/x/crypto` — SSH protocol
- `gopkg.in/yaml.v3` — YAML parsing
- `testify` — Test assertions

## Coding Standards

- UK English
- All functions have typed params/returns
- Tests use testify + mock SSH client
- Test naming: `_Good`, `_Bad`, `_Ugly` suffixes
- License: EUPL-1.2
