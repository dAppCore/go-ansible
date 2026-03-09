package anscmd

import (
	"forge.lthn.ai/core/cli/pkg/cli"
)

func init() {
	cli.RegisterCommands(AddAnsibleCommands)
}

// AddAnsibleCommands registers the 'ansible' command and all subcommands.
func AddAnsibleCommands(root *cli.Command) {
	root.AddCommand(ansibleCmd)
}
