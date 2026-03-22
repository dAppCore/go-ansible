package anscmd

import (
	"dappco.re/go/core"
)

// Register registers the 'ansible' command and all subcommands on the given Core instance.
func Register(c *core.Core) {
	c.Command("ansible", core.Command{
		Description: "Run Ansible playbooks natively (no Python required)",
		Action:      runAnsible,
		Flags: core.Options{
			{Key: "inventory", Value: ""},
			{Key: "limit", Value: ""},
			{Key: "tags", Value: ""},
			{Key: "skip-tags", Value: ""},
			{Key: "extra-vars", Value: ""},
			{Key: "verbose", Value: 0},
			{Key: "check", Value: false},
		},
	})

	c.Command("ansible/test", core.Command{
		Description: "Test SSH connectivity to a host",
		Action:      runAnsibleTest,
		Flags: core.Options{
			{Key: "user", Value: "root"},
			{Key: "password", Value: ""},
			{Key: "key", Value: ""},
			{Key: "port", Value: 22},
		},
	})
}
