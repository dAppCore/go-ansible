package ansible

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// --- RoleRef UnmarshalYAML ---

func TestRoleRef_UnmarshalYAML_Good_StringForm(t *testing.T) {
	input := `common`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	require.NoError(t, err)
	assert.Equal(t, "common", ref.Role)
}

func TestRoleRef_UnmarshalYAML_Good_StructForm(t *testing.T) {
	input := `
role: webserver
vars:
  http_port: 80
tags:
  - web
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	require.NoError(t, err)
	assert.Equal(t, "webserver", ref.Role)
	assert.Equal(t, 80, ref.Vars["http_port"])
	assert.Equal(t, []string{"web"}, ref.Tags)
}

func TestRoleRef_UnmarshalYAML_Good_NameField(t *testing.T) {
	// Some playbooks use "name:" instead of "role:"
	input := `
name: myapp
tasks_from: install.yml
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	require.NoError(t, err)
	assert.Equal(t, "myapp", ref.Role) // Name is copied to Role
	assert.Equal(t, "install.yml", ref.TasksFrom)
}

func TestRoleRef_UnmarshalYAML_Good_WithWhen(t *testing.T) {
	input := `
role: conditional_role
when: ansible_os_family == "Debian"
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	require.NoError(t, err)
	assert.Equal(t, "conditional_role", ref.Role)
	assert.NotNil(t, ref.When)
}

// --- Task UnmarshalYAML ---

func TestTask_UnmarshalYAML_Good_ModuleWithArgs(t *testing.T) {
	input := `
name: Install nginx
apt:
  name: nginx
  state: present
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "Install nginx", task.Name)
	assert.Equal(t, "apt", task.Module)
	assert.Equal(t, "nginx", task.Args["name"])
	assert.Equal(t, "present", task.Args["state"])
}

func TestTask_UnmarshalYAML_Good_FreeFormModule(t *testing.T) {
	input := `
name: Run command
shell: echo hello world
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "shell", task.Module)
	assert.Equal(t, "echo hello world", task.Args["_raw_params"])
}

func TestTask_UnmarshalYAML_Good_ModuleNoArgs(t *testing.T) {
	input := `
name: Gather facts
setup:
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "setup", task.Module)
	assert.NotNil(t, task.Args)
}

func TestTask_UnmarshalYAML_Good_WithRegister(t *testing.T) {
	input := `
name: Check file
stat:
  path: /etc/hosts
register: stat_result
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "stat_result", task.Register)
	assert.Equal(t, "stat", task.Module)
}

func TestTask_UnmarshalYAML_Good_WithWhen(t *testing.T) {
	input := `
name: Conditional task
debug:
  msg: "hello"
when: some_var is defined
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.NotNil(t, task.When)
}

func TestTask_UnmarshalYAML_Good_WithLoop(t *testing.T) {
	input := `
name: Install packages
apt:
  name: "{{ item }}"
loop:
  - vim
  - git
  - curl
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	items, ok := task.Loop.([]any)
	require.True(t, ok)
	assert.Len(t, items, 3)
}

func TestTask_UnmarshalYAML_Good_WithItems(t *testing.T) {
	// with_items should be converted to loop
	input := `
name: Old-style loop
apt:
  name: "{{ item }}"
with_items:
  - vim
  - git
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	// with_items should have been stored in Loop
	items, ok := task.Loop.([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
}

func TestTask_UnmarshalYAML_Good_WithNotify(t *testing.T) {
	input := `
name: Install package
apt:
  name: nginx
notify: restart nginx
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "restart nginx", task.Notify)
}

func TestTask_UnmarshalYAML_Good_WithNotifyList(t *testing.T) {
	input := `
name: Install package
apt:
  name: nginx
notify:
  - restart nginx
  - reload config
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	notifyList, ok := task.Notify.([]any)
	require.True(t, ok)
	assert.Len(t, notifyList, 2)
}

func TestTask_UnmarshalYAML_Good_IncludeTasks(t *testing.T) {
	input := `
name: Include tasks
include_tasks: other-tasks.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "other-tasks.yml", task.IncludeTasks)
}

func TestTask_UnmarshalYAML_Good_IncludeRole(t *testing.T) {
	input := `
name: Include role
include_role:
  name: common
  tasks_from: setup.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.IncludeRole)
	assert.Equal(t, "common", task.IncludeRole.Name)
	assert.Equal(t, "setup.yml", task.IncludeRole.TasksFrom)
}

func TestTask_UnmarshalYAML_Good_BecomeFields(t *testing.T) {
	input := `
name: Privileged task
shell: systemctl restart nginx
become: true
become_user: root
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.Become)
	assert.True(t, *task.Become)
	assert.Equal(t, "root", task.BecomeUser)
}

func TestTask_UnmarshalYAML_Good_IgnoreErrors(t *testing.T) {
	input := `
name: Might fail
shell: some risky command
ignore_errors: true
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.True(t, task.IgnoreErrors)
}

// --- Inventory data structure ---

func TestInventory_UnmarshalYAML_Good_Complex(t *testing.T) {
	input := `
all:
  vars:
    ansible_user: admin
    ansible_ssh_private_key_file: ~/.ssh/id_ed25519
  hosts:
    bastion:
      ansible_host: 1.2.3.4
      ansible_port: 4819
  children:
    webservers:
      hosts:
        web1:
          ansible_host: 10.0.0.1
        web2:
          ansible_host: 10.0.0.2
      vars:
        http_port: 80
    databases:
      hosts:
        db1:
          ansible_host: 10.0.1.1
          ansible_connection: ssh
`
	var inv Inventory
	err := yaml.Unmarshal([]byte(input), &inv)

	require.NoError(t, err)
	require.NotNil(t, inv.All)

	// Check top-level vars
	assert.Equal(t, "admin", inv.All.Vars["ansible_user"])

	// Check top-level hosts
	require.NotNil(t, inv.All.Hosts["bastion"])
	assert.Equal(t, "1.2.3.4", inv.All.Hosts["bastion"].AnsibleHost)
	assert.Equal(t, 4819, inv.All.Hosts["bastion"].AnsiblePort)

	// Check children
	require.NotNil(t, inv.All.Children["webservers"])
	assert.Len(t, inv.All.Children["webservers"].Hosts, 2)
	assert.Equal(t, 80, inv.All.Children["webservers"].Vars["http_port"])

	require.NotNil(t, inv.All.Children["databases"])
	assert.Equal(t, "ssh", inv.All.Children["databases"].Hosts["db1"].AnsibleConnection)
}

// --- Facts ---

func TestFacts_Struct(t *testing.T) {
	facts := Facts{
		Hostname:     "web1",
		FQDN:         "web1.example.com",
		OS:           "Debian",
		Distribution: "ubuntu",
		Version:      "24.04",
		Architecture: "x86_64",
		Kernel:       "6.8.0",
		Memory:       16384,
		CPUs:         4,
		IPv4:         "10.0.0.1",
	}

	assert.Equal(t, "web1", facts.Hostname)
	assert.Equal(t, "web1.example.com", facts.FQDN)
	assert.Equal(t, "ubuntu", facts.Distribution)
	assert.Equal(t, "x86_64", facts.Architecture)
	assert.Equal(t, int64(16384), facts.Memory)
	assert.Equal(t, 4, facts.CPUs)
}

// --- TaskResult ---

func TestTaskResult_Struct(t *testing.T) {
	result := TaskResult{
		Changed: true,
		Failed:  false,
		Skipped: false,
		Msg:     "task completed",
		Stdout:  "output",
		Stderr:  "",
		RC:      0,
	}

	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, "task completed", result.Msg)
	assert.Equal(t, 0, result.RC)
}

func TestTaskResult_WithLoopResults(t *testing.T) {
	result := TaskResult{
		Changed: true,
		Results: []TaskResult{
			{Changed: true, RC: 0},
			{Changed: false, RC: 0},
			{Changed: true, RC: 0},
		},
	}

	assert.Len(t, result.Results, 3)
	assert.True(t, result.Results[0].Changed)
	assert.False(t, result.Results[1].Changed)
}

// --- KnownModules ---

func TestKnownModules_ContainsExpected(t *testing.T) {
	// Verify both FQCN and short forms are present
	fqcnModules := []string{
		"ansible.builtin.shell",
		"ansible.builtin.command",
		"ansible.builtin.copy",
		"ansible.builtin.file",
		"ansible.builtin.apt",
		"ansible.builtin.service",
		"ansible.builtin.systemd",
		"ansible.builtin.debug",
		"ansible.builtin.set_fact",
	}
	for _, mod := range fqcnModules {
		assert.Contains(t, KnownModules, mod, "expected FQCN module %s", mod)
	}

	shortModules := []string{
		"shell", "command", "copy", "file", "apt", "service",
		"systemd", "debug", "set_fact", "template", "user", "group",
	}
	for _, mod := range shortModules {
		assert.Contains(t, KnownModules, mod, "expected short-form module %s", mod)
	}
}
