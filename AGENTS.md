# Agent Guide

This repository implements Ansible-compatible orchestration primitives in Go. Keep changes close to the existing file boundaries: parser behavior belongs in `parser.go`, execution flow in `executor.go`, module handlers in `modules.go`, SSH transport in `ssh.go`, local transport in `local_client.go`, and command wiring in `cmd/ansible`.

## Repository Shape

- `types.go` contains the playbook, task, role, result, and inventory data model plus YAML decoding hooks.
- `parser.go` resolves playbooks, task files, inventory files, vars files, roles, host patterns, and module names.
- `executor.go` owns play execution, host selection, vars/facts, role inclusion, blocks, handlers, loops, retries, conditions, and templating.
- `modules.go` contains Ansible module implementations and shared module helpers.
- `ssh.go` and `local_client.go` implement the client contract used by the executor.
- `cmd/ansible` registers the CLI command and translates parsed command options into executor settings.

## Compliance Rules

Use `dappco.re/go` wrappers for formatting, JSON, filesystem, path, string, bytes, errors, and process-adjacent helpers. Do not add direct imports of `fmt`, `errors`, `strings`, `path`, `path/filepath`, `os`, `os/exec`, `encoding/json`, `bytes`, or `log` in production or tests.

Public symbols are covered file-by-file. A public symbol in `<file>.go` needs `Test<File>_<Symbol>_Good`, `Bad`, and `Ugly` in `<file>_test.go`, plus an `Example<Symbol>` or method example in `<file>_example_test.go`. Do not create monolithic compliance files or versioned test files.

## Local Workflow

Before stopping, run:

```sh
GOWORK=off go mod tidy
GOWORK=off go vet ./...
GOWORK=off go test -count=1 ./...
gofmt -l .
bash /Users/snider/Code/core/go/tests/cli/v090-upgrade/audit.sh .
```

`BRIEF.md` is an untracked work brief and should stay untouched.
