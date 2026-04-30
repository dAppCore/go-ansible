package ansible

import (
	core "dappco.re/go"
	"gopkg.in/yaml.v3"
)

func ExamplePlay_UnmarshalYAML() {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: all\nansible.builtin.import_playbook: child.yml\n"), &play)
	core.Println(err == nil, play.Hosts, play.ImportPlaybook)
	// Output: true all child.yml
}

func ExampleRoleRef_UnmarshalYAML() {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("name: web\ntags: [deploy]\n"), &ref)
	core.Println(err == nil, ref.Role, ref.Tags[0])
	// Output: true web deploy
}

func ExampleInventory_UnmarshalYAML() {
	var inv Inventory
	err := yaml.Unmarshal([]byte("all:\n  hosts:\n    web1: {}\n"), &inv)
	_, ok := inv.All.Hosts["web1"]
	core.Println(err == nil, ok)
	// Output: true true
}
