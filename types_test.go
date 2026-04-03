package ansible

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// --- RoleRef UnmarshalYAML ---

func TestTypes_RoleRef_UnmarshalYAML_Good_StringForm(t *testing.T) {
	input := `common`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	require.NoError(t, err)
	assert.Equal(t, "common", ref.Role)
}

func TestTypes_RoleRef_UnmarshalYAML_Good_StructForm(t *testing.T) {
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

func TestTypes_RoleRef_UnmarshalYAML_Good_NameField(t *testing.T) {
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

func TestTypes_RoleRef_UnmarshalYAML_Good_WithWhen(t *testing.T) {
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

func TestTypes_RoleRef_UnmarshalYAML_Good_CustomRoleFiles(t *testing.T) {
	input := `
name: web
tasks_from: setup.yml
defaults_from: custom-defaults.yml
vars_from: custom-vars.yml
public: true
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	require.NoError(t, err)
	assert.Equal(t, "web", ref.Role)
	assert.Equal(t, "setup.yml", ref.TasksFrom)
	assert.Equal(t, "custom-defaults.yml", ref.DefaultsFrom)
	assert.Equal(t, "custom-vars.yml", ref.VarsFrom)
	assert.True(t, ref.Public)
}

// --- Task UnmarshalYAML ---

func TestTypes_Task_UnmarshalYAML_Good_ModuleWithArgs(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_FreeFormModule(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_ModuleNoArgs(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_WithRegister(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_WithWhen(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_WithLoop(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_WithItems(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_WithDict(t *testing.T) {
	input := `
name: Old-style dict loop
debug:
  msg: "{{ item.key }}={{ item.value }}"
with_dict:
  alpha: one
  beta: two
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	items, ok := task.Loop.([]any)
	require.True(t, ok)
	require.Len(t, items, 2)

	first, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alpha", first["key"])
	assert.Equal(t, "one", first["value"])

	second, ok := items[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "beta", second["key"])
	assert.Equal(t, "two", second["value"])
}

func TestTypes_Task_UnmarshalYAML_Good_WithIndexedItems(t *testing.T) {
	input := `
name: Indexed loop
debug:
  msg: "{{ item.0 }}={{ item.1 }}"
with_indexed_items:
  - apple
  - banana
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	items, ok := task.Loop.([]any)
	require.True(t, ok)
	require.Len(t, items, 2)

	first, ok := items[0].([]any)
	require.True(t, ok)
	assert.Equal(t, 0, first[0])
	assert.Equal(t, "apple", first[1])

	second, ok := items[1].([]any)
	require.True(t, ok)
	assert.Equal(t, 1, second[0])
	assert.Equal(t, "banana", second[1])
}

func TestTypes_Task_UnmarshalYAML_Good_WithFile(t *testing.T) {
	input := `
name: Read files
debug:
  msg: "{{ item }}"
with_file:
  - templates/a.txt
  - templates/b.txt
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.WithFile)
	files, ok := task.WithFile.([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"templates/a.txt", "templates/b.txt"}, files)
}

func TestTypes_Task_UnmarshalYAML_Good_WithFileGlob(t *testing.T) {
	input := `
name: Read globbed files
debug:
  msg: "{{ item }}"
with_fileglob:
  - templates/*.txt
  - files/*.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.WithFileGlob)
	files, ok := task.WithFileGlob.([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"templates/*.txt", "files/*.yml"}, files)
}

func TestTypes_Task_UnmarshalYAML_Good_WithSequence(t *testing.T) {
	input := `
name: Read sequence values
debug:
  msg: "{{ item }}"
with_sequence: "start=1 end=3 format=%02d"
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.WithSequence)
	sequence, ok := task.WithSequence.(string)
	require.True(t, ok)
	assert.Equal(t, "start=1 end=3 format=%02d", sequence)
}

func TestTypes_Task_UnmarshalYAML_Good_ActionAlias(t *testing.T) {
	input := `
name: Legacy action
action: command echo hello world
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "command", task.Module)
	require.NotNil(t, task.Args)
	assert.Equal(t, "echo hello world", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_ActionAliasKeyValue(t *testing.T) {
	input := `
name: Legacy action with args
action: module=copy src=/tmp/source dest=/tmp/dest mode=0644
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "copy", task.Module)
	require.NotNil(t, task.Args)
	assert.Equal(t, "/tmp/source", task.Args["src"])
	assert.Equal(t, "/tmp/dest", task.Args["dest"])
	assert.Equal(t, "0644", task.Args["mode"])
}

func TestTypes_Task_UnmarshalYAML_Good_LocalAction(t *testing.T) {
	input := `
name: Legacy local action
local_action: shell echo local
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "shell", task.Module)
	assert.Equal(t, "localhost", task.Delegate)
	require.NotNil(t, task.Args)
	assert.Equal(t, "echo local", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_LocalActionKeyValue(t *testing.T) {
	input := `
name: Legacy local action with args
local_action: module=command chdir=/tmp
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "command", task.Module)
	assert.Equal(t, "localhost", task.Delegate)
	require.NotNil(t, task.Args)
	assert.Equal(t, "/tmp", task.Args["chdir"])
}

func TestTypes_Task_UnmarshalYAML_Good_WithNested(t *testing.T) {
	input := `
name: Nested loop values
debug:
 msg: "{{ item.0 }} {{ item.1 }}"
with_nested:
  - - red
    - blue
  - - small
    - large
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	items, ok := task.Loop.([]any)
	require.True(t, ok)
	require.Len(t, items, 4)

	first, ok := items[0].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"red", "small"}, first)

	second, ok := items[1].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"red", "large"}, second)

	third, ok := items[2].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"blue", "small"}, third)

	fourth, ok := items[3].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"blue", "large"}, fourth)
}

func TestTypes_Task_UnmarshalYAML_Good_WithTogether(t *testing.T) {
	input := `
name: Together loop values
debug:
  msg: "{{ item.0 }} {{ item.1 }}"
with_together:
  - - red
    - blue
  - - small
    - large
    - medium
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.WithTogether)

	items, ok := task.Loop.([]any)
	require.True(t, ok)
	require.Len(t, items, 2)

	first, ok := items[0].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"red", "small"}, first)

	second, ok := items[1].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"blue", "large"}, second)
}

func TestTypes_Task_UnmarshalYAML_Good_WithSubelements(t *testing.T) {
	input := `
name: Subelement loop values
debug:
  msg: "{{ item.0.name }} {{ item.1 }}"
with_subelements:
  - users
  - authorized
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.WithSubelements)
	values, ok := task.WithSubelements.([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"users", "authorized"}, values)
}

func TestTypes_Task_UnmarshalYAML_Good_WithNotify(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_WithListen(t *testing.T) {
	input := `
name: Restart service
debug:
  msg: "handler"
listen: reload nginx
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "reload nginx", task.Listen)
}

func TestTypes_Task_UnmarshalYAML_Good_ShortFormSystemModules(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantModule string
	}{
		{
			name: "hostname",
			input: `
name: Set hostname
hostname:
  name: web01
`,
			wantModule: "hostname",
		},
		{
			name: "sysctl",
			input: `
name: Tune kernel
sysctl:
  name: net.ipv4.ip_forward
  value: "1"
`,
			wantModule: "sysctl",
		},
		{
			name: "reboot",
			input: `
name: Reboot host
reboot:
`,
			wantModule: "reboot",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var task Task
			err := yaml.Unmarshal([]byte(tc.input), &task)

			require.NoError(t, err)
			assert.Equal(t, tc.wantModule, task.Module)
			assert.NotNil(t, task.Args)
		})
	}
}

func TestTypes_Task_UnmarshalYAML_Good_ShortFormCommunityModules(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantModule string
	}{
		{
			name: "authorized_key",
			input: `
name: Install SSH key
authorized_key:
  user: deploy
  key: ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQD
`,
			wantModule: "authorized_key",
		},
		{
			name: "ufw",
			input: `
name: Allow SSH
ufw:
  rule: allow
  port: "22"
`,
			wantModule: "ufw",
		},
		{
			name: "docker_compose",
			input: `
name: Start stack
docker_compose:
  project_src: /opt/app
`,
			wantModule: "docker_compose",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var task Task
			err := yaml.Unmarshal([]byte(tc.input), &task)

			require.NoError(t, err)
			assert.Equal(t, tc.wantModule, task.Module)
		})
	}
}

func TestTypes_Task_UnmarshalYAML_Good_WithNotifyList(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_IncludeTasks(t *testing.T) {
	input := `
name: Include tasks
include_tasks: other-tasks.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "other-tasks.yml", task.IncludeTasks)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeTasksFQCN(t *testing.T) {
	input := `
name: Include tasks
ansible.builtin.include_tasks: other-tasks.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	assert.Equal(t, "other-tasks.yml", task.IncludeTasks)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeTasksApply(t *testing.T) {
	input := `
name: Include tasks
include_tasks: other-tasks.yml
apply:
  tags:
    - deploy
  become: true
  become_user: root
  environment:
    APP_ENV: production
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.Apply)
	assert.Equal(t, []string{"deploy"}, task.Apply.Tags)
	require.NotNil(t, task.Apply.Become)
	assert.True(t, *task.Apply.Become)
	assert.Equal(t, "root", task.Apply.BecomeUser)
	assert.Equal(t, "production", task.Apply.Environment["APP_ENV"])
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeRole(t *testing.T) {
	input := `
name: Include role
include_role:
  name: common
  tasks_from: setup.yml
  defaults_from: defaults.yml
  vars_from: vars.yml
  handlers_from: handlers.yml
  public: true
  apply:
    tags:
      - deploy
    when: apply_enabled
    become: true
    become_user: root
    environment:
      APP_ENV: production
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.IncludeRole)
	assert.Equal(t, "common", task.IncludeRole.Name)
	assert.Equal(t, "setup.yml", task.IncludeRole.TasksFrom)
	assert.Equal(t, "defaults.yml", task.IncludeRole.DefaultsFrom)
	assert.Equal(t, "vars.yml", task.IncludeRole.VarsFrom)
	assert.Equal(t, "handlers.yml", task.IncludeRole.HandlersFrom)
	assert.True(t, task.IncludeRole.Public)
	require.NotNil(t, task.IncludeRole.Apply)
	assert.Equal(t, []string{"deploy"}, task.IncludeRole.Apply.Tags)
	assert.Equal(t, "apply_enabled", task.IncludeRole.Apply.When)
	require.NotNil(t, task.IncludeRole.Apply.Become)
	assert.True(t, *task.IncludeRole.Apply.Become)
	assert.Equal(t, "root", task.IncludeRole.Apply.BecomeUser)
	assert.Equal(t, "production", task.IncludeRole.Apply.Environment["APP_ENV"])
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeRoleStringForm(t *testing.T) {
	input := `
name: Include role
include_role: common
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.IncludeRole)
	assert.Equal(t, "common", task.IncludeRole.Role)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeRoleFQCN(t *testing.T) {
	input := `
name: Include role
ansible.builtin.include_role:
  name: common
  tasks_from: setup.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.IncludeRole)
	assert.Equal(t, "common", task.IncludeRole.Role)
	assert.Equal(t, "setup.yml", task.IncludeRole.TasksFrom)
}

func TestTypes_Task_UnmarshalYAML_Good_ImportRoleStringForm(t *testing.T) {
	input := `
name: Import role
import_role: common
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.ImportRole)
	assert.Equal(t, "common", task.ImportRole.Role)
}

func TestTypes_Task_UnmarshalYAML_Good_ImportRoleFQCN(t *testing.T) {
	input := `
name: Import role
ansible.builtin.import_role: common
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	require.NoError(t, err)
	require.NotNil(t, task.ImportRole)
	assert.Equal(t, "common", task.ImportRole.Role)
}

func TestTypes_Task_UnmarshalYAML_Good_BecomeFields(t *testing.T) {
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

func TestTypes_Task_UnmarshalYAML_Good_IgnoreErrors(t *testing.T) {
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

func TestTypes_Inventory_UnmarshalYAML_Good_Complex(t *testing.T) {
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

func TestTypes_Facts_Good_Struct(t *testing.T) {
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

func TestTypes_TaskResult_Good_Struct(t *testing.T) {
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

func TestTypes_TaskResult_Good_WithLoopResults(t *testing.T) {
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

func TestTypes_KnownModules_Good_ContainsExpected(t *testing.T) {
	// Verify both FQCN and short forms are present
	fqcnModules := []string{
		"ansible.builtin.shell",
		"ansible.builtin.command",
		"ansible.builtin.copy",
		"ansible.builtin.file",
		"ansible.builtin.apt",
		"ansible.builtin.service",
		"ansible.builtin.systemd",
		"ansible.builtin.rpm",
		"ansible.builtin.debug",
		"ansible.builtin.set_fact",
		"ansible.builtin.ping",
		"community.general.ufw",
		"ansible.posix.authorized_key",
		"ansible.builtin.docker_compose",
		"ansible.builtin.docker_compose_v2",
		"ansible.builtin.hostname",
		"ansible.builtin.sysctl",
		"ansible.builtin.reboot",
		"community.docker.docker_compose",
		"community.docker.docker_compose_v2",
	}
	for _, mod := range fqcnModules {
		assert.Contains(t, KnownModules, mod, "expected FQCN module %s", mod)
	}

	shortModules := []string{
		"shell", "command", "copy", "file", "apt", "service",
		"systemd", "rpm", "debug", "set_fact", "ping", "template", "user", "group",
	}
	for _, mod := range shortModules {
		assert.Contains(t, KnownModules, mod, "expected short-form module %s", mod)
	}
}
