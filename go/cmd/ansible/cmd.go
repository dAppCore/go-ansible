package ansiblecmd

import (
	"dappco.re/go"
)

// Register registers the `ansible` command and its `ansible/test` subcommand.
// Returns core.Result per the Mantis #1336 canonical Register signature
// — Ok(nil) on success. Callers may discard the result if they don't
// care about wiring failures.
//
// Example:
//
//	app := core.New()
//	if r := Register(app); !r.OK { return r }
func Register(c *core.Core) core.Result {
	c.Command("ansible", core.Command{
		Description: "Run Ansible playbooks natively (no Python required)",
		Action:      runPlaybookCommand,
		Flags: core.NewOptions(
			core.Option{Key: "inventory", Value: ""},
			core.Option{Key: "i", Value: ""},
			core.Option{Key: "limit", Value: ""},
			core.Option{Key: "l", Value: ""},
			core.Option{Key: "tags", Value: ""},
			core.Option{Key: "t", Value: ""},
			core.Option{Key: "skip-tags", Value: ""},
			core.Option{Key: "extra-vars", Value: ""},
			core.Option{Key: "e", Value: ""},
			core.Option{Key: "verbose", Value: 0},
			core.Option{Key: "v", Value: false},
			core.Option{Key: "check", Value: false},
			core.Option{Key: "diff", Value: false},
		),
	})

	c.Command("ansible/test", core.Command{
		Description: "Test SSH connectivity to a host",
		Action:      runSSHTestCommand,
		Flags: core.NewOptions(
			core.Option{Key: "user", Value: "root"},
			core.Option{Key: "u", Value: "root"},
			core.Option{Key: "password", Value: ""},
			core.Option{Key: "key", Value: ""},
			core.Option{Key: "i", Value: ""},
			core.Option{Key: "port", Value: 22},
		),
	})

	return core.Ok(nil)
}
