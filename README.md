# go-ansible

`github.com/dappcore/go-ansible/go` is a native Go implementation of core Ansible playbook mechanics. It parses playbooks, inventories, roles, variables, loops, handlers, and common built-in modules, then executes them through either SSH clients or a local controller-side client.

The package is designed for Go programs that need Ansible-style orchestration without shelling out to Python Ansible. It keeps the public surface small:

- `NewParser` reads playbooks, inventories, task files, vars files, and role directories.
- `NewExecutor` runs parsed playbooks with inventory, host variables, facts, handlers, loops, tags, check mode, and diff reporting.
- `NewSSHClient` provides the remote execution client used by the executor.
- `cmd/ansible.Register` wires the native runner into a `github.com/dappcore/go` command tree.

## Install

```sh
go get github.com/dappcore/go-ansible/go
```

## Quick Start

```go
executor := ansible.NewExecutor("/srv/playbooks")
if err := executor.SetInventory("/srv/inventory.yml"); err != nil {
    return err
}
executor.SetVar("release", "2026.04")
return executor.Run(context.Background(), "/srv/playbooks/site.yml")
```

For command integration:

```go
app := core.New()
ansiblecmd.Register(app)
```

## Development

This repository follows the `github.com/dappcore/go` v0.9 shape: core wrappers are used instead of direct imports for banned standard-library surfaces, public symbols have file-aware Good/Bad/Ugly test triplets, and examples live beside their source file. Run the full gate before handing changes off:

```sh
GOWORK=off go mod tidy
GOWORK=off go vet ./...
GOWORK=off go test -count=1 ./...
gofmt -l .
bash /Users/snider/Code/core/go/tests/cli/v090-upgrade/audit.sh .
```
