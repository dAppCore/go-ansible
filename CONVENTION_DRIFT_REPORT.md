# Convention Drift Report

Date: 2026-03-23
Branch: `agent/convention-drift-check--stdlib-core----u`

`CODEX.md` is not present in this repository. Conventions were taken from `CLAUDE.md` and `docs/development.md`.

Commands used for the test-gap pass:

```bash
go test -coverprofile=/tmp/ansible.cover ./...
go tool cover -func=/tmp/ansible.cover
```

## `stdlib→core.*`

No direct `stdlib`-to-`core.*` wrapper drift was found in the Go implementation. The remaining drift is stale migration residue around the `core.*` move:

- `go.mod:15`, `go.sum:7`, `go.sum:8`
  Legacy `forge.lthn.ai/core/go-log` references still remain in the dependency graph.
- `CLAUDE.md:37`, `docs/development.md:169`
  Repository guidance still refers to `dappco.re/go command registry`, while the current command registration lives on the `dappco.re/go` API at `cmd/ansible/cmd.go:8`.
- `CLAUDE.md:66`, `docs/development.md:86`
  Guidance still calls the logging package `go-log`, while production code imports `dappco.re/go/log` at `cmd/ansible/ansible.go:13`, `executor.go:15`, `modules.go:13`, `parser.go:12`, `ssh.go:16`.

## UK English

- `executor.go:248`
  Comment uses US spelling: `Initialize host results`.
- `parser.go:321`
  Comment uses US spelling: `NormalizeModule normalizes a module name to its canonical form.`
- `types.go:110`
  Comment uses US spelling: `LoopControl controls loop behavior.`

## Missing Tests

- `cmd/ansible/ansible.go:17`, `cmd/ansible/ansible.go:29`, `cmd/ansible/ansible.go:163`, `cmd/ansible/cmd.go:8`
  `go tool cover` reports `0.0%` coverage for the entire `cmd/ansible` package, so argument parsing, command registration, playbook execution wiring, and SSH test wiring have no tests.
- `executor.go:81`, `executor.go:97`, `executor.go:172`, `executor.go:210`, `executor.go:241`, `executor.go:307`, `executor.go:382`, `executor.go:420`, `executor.go:444`, `executor.go:499`, `executor.go:565`
  The main execution path is still uncovered: top-level run flow, play execution, roles, host task scheduling, loops, block handling, includes, SSH client creation, and fact gathering are all `0.0%` in the coverage report.
- `parser.go:119`
  `ParseRole` is `0.0%` covered.
- `ssh.go:77`, `ssh.go:187`, `ssh.go:200`, `ssh.go:276`, `ssh.go:283`, `ssh.go:377`, `ssh.go:396`, `ssh.go:410`
  Only constructor/default behaviour is tested; the real SSH transport methods are all `0.0%` covered.
- `modules.go:17`, `modules.go:178`, `modules.go:206`, `modules.go:234`, `modules.go:253`, `modules.go:281`, `modules.go:324`, `modules.go:352`, `modules.go:420`, `modules.go:463`, `modules.go:480`, `modules.go:502`, `modules.go:526`, `modules.go:550`, `modules.go:584`, `modules.go:615`, `modules.go:652`, `modules.go:665`, `modules.go:690`, `modules.go:732`, `modules.go:743`, `modules.go:800`, `modules.go:835`, `modules.go:941`, `modules.go:989`, `modules.go:1013`, `modules.go:1042`, `modules.go:1120`, `modules.go:1139`, `modules.go:1172`, `modules.go:1209`, `modules.go:1283`, `modules.go:1288`, `modules.go:1306`, `modules.go:1357`, `modules.go:1408`
  The real dispatcher and production module handlers are still `0.0%` covered.
- `mock_ssh_test.go:347`, `mock_ssh_test.go:433`
  Existing module tests bypass `Executor.executeModule` and the production handlers by routing through `executeModuleWithMock` and duplicated shim implementations, so module assertions do not exercise the shipped code paths.
- `CLAUDE.md:60`, `docs/index.md:141`, `docs/index.md:142`, `modules.go:941`, `modules.go:989`, `modules.go:1120`, `modules.go:1139`, `modules.go:1283`, `modules.go:1288`
  Documentation advertises support for `pause`, `wait_for`, `hostname`, `sysctl`, `setup`, and `reboot`, but there are no dedicated tests for those production handlers.

## Missing SPDX Headers

No tracked text file currently contains an SPDX header.

- Repo metadata: `.github/workflows/ci.yml:1`, `.gitignore:1`, `go.mod:1`, `go.sum:1`
- Documentation: `CLAUDE.md:1`, `CONSUMERS.md:1`, `docs/architecture.md:1`, `docs/development.md:1`, `docs/index.md:1`, `kb/Executor.md:1`, `kb/Home.md:1`
- Go source: `cmd/ansible/ansible.go:1`, `cmd/ansible/cmd.go:1`, `executor.go:1`, `modules.go:1`, `parser.go:1`, `ssh.go:1`, `types.go:1`
- Go tests: `executor_extra_test.go:1`, `executor_test.go:1`, `mock_ssh_test.go:1`, `modules_adv_test.go:1`, `modules_cmd_test.go:1`, `modules_file_test.go:1`, `modules_infra_test.go:1`, `modules_svc_test.go:1`, `parser_test.go:1`, `ssh_test.go:1`, `types_test.go:1`
