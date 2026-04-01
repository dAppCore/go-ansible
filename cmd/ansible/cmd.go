package anscmd

import (
	"dappco.re/go/core"
)

// Register registers the 'ansible' command and all subcommands on the given Core instance.
//
// Example:
//
//	var app core.Core
//	Register(&app)
func Register(c *core.Core) {
	c.Command("ansible", core.Command{
		Description: "Run Ansible playbooks natively (no Python required)",
		Action:      runAnsible,
		Flags: core.NewOptions(
			core.Option{Key: "inventory", Value: ""},
			core.Option{Key: "limit", Value: ""},
			core.Option{Key: "tags", Value: ""},
			core.Option{Key: "skip-tags", Value: ""},
			core.Option{Key: "extra-vars", Value: ""},
			core.Option{Key: "verbose", Value: 0},
			core.Option{Key: "check", Value: false},
			core.Option{Key: "diff", Value: false},
		),
	})

	c.Command("ansible/test", core.Command{
		Description: "Test SSH connectivity to a host",
		Action:      runAnsibleTest,
		Flags: core.NewOptions(
			core.Option{Key: "user", Value: "root"},
			core.Option{Key: "password", Value: ""},
			core.Option{Key: "key", Value: ""},
			core.Option{Key: "port", Value: 22},
		),
	})
}
