package ansible

import (
	core "dappco.re/go"

	"gopkg.in/yaml.v3"
)

// --- RoleRef UnmarshalYAML ---

func TestTypes_RoleRef_UnmarshalYAML_Good_StringForm(t *core.T) {
	input := `common`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "common", ref.Role)
}

func TestTypes_RoleRef_UnmarshalYAML_Good_StructForm(t *core.T) {
	input := `
role: webserver
vars:
  http_port: 80
tags:
  - web
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "webserver", ref.Role)
	core.AssertEqual(t, 80, ref.Vars["http_port"])
	core.AssertEqual(t, []string{"web"}, ref.Tags)
}

func TestTypes_RoleRef_UnmarshalYAML_Good_NameField(t *core.T) {
	// Some playbooks use "name:" instead of "role:"
	input := `
name: myapp
tasks_from: install.yml
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "myapp", ref.Role) // Name is copied to Role
	core.AssertEqual(t, "install.yml", ref.TasksFrom)
}

func TestTypes_RoleRef_UnmarshalYAML_Good_WithWhen(t *core.T) {
	input := `
role: conditional_role
when: ansible_os_family == "Debian"
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "conditional_role", ref.Role)
	core.AssertNotNil(t, ref.When)
}

func TestTypes_RoleRef_UnmarshalYAML_Good_CustomRoleFiles(t *core.T) {
	input := `
name: web
tasks_from: setup.yml
defaults_from: custom-defaults.yml
vars_from: custom-vars.yml
public: true
`
	var ref RoleRef
	err := yaml.Unmarshal([]byte(input), &ref)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "web", ref.Role)
	core.AssertEqual(t, "setup.yml", ref.TasksFrom)
	core.AssertEqual(t, "custom-defaults.yml", ref.DefaultsFrom)
	core.AssertEqual(t, "custom-vars.yml", ref.VarsFrom)
	core.AssertTrue(t, ref.Public)
}

// --- Task UnmarshalYAML ---

func TestTypes_Task_UnmarshalYAML_Good_ModuleWithArgs(t *core.T) {
	input := `
name: Install nginx
apt:
  name: nginx
  state: present
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "Install nginx", task.Name)
	core.AssertEqual(t, "apt", task.Module)
	core.AssertEqual(t, "nginx", task.Args["name"])
	core.AssertEqual(t, "present", task.Args["state"])
}

func TestTypes_Task_UnmarshalYAML_Good_FreeFormModule(t *core.T) {
	input := `
name: Run command
shell: echo hello world
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "shell", task.Module)
	core.AssertEqual(t, "echo hello world", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_ModuleNoArgs(t *core.T) {
	input := `
name: Gather facts
setup:
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "setup", task.Module)
	core.AssertNotNil(t, task.Args)
}

func TestTypes_Task_UnmarshalYAML_Good_WithRegister(t *core.T) {
	input := `
name: Check file
stat:
  path: /etc/hosts
register: stat_result
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "stat_result", task.Register)
	core.AssertEqual(t, "stat", task.Module)
}

func TestTypes_Task_UnmarshalYAML_Good_WithWhen(t *core.T) {
	input := `
name: Conditional task
debug:
  msg: "hello"
when: some_var is defined
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.When)
}

func TestTypes_Task_UnmarshalYAML_Good_WithCheckModeAndDiff(t *core.T) {
	input := `
name: Force a dry run
shell: echo hello
check_mode: false
diff: true
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.CheckMode)
	core.AssertNotNil(t, task.Diff)
	core.AssertFalse(t, *task.CheckMode)
	core.AssertTrue(t, *task.Diff)
}

func TestTypes_Task_UnmarshalYAML_Good_WithLoop(t *core.T) {
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

	core.RequireNoError(t, err)
	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 3)
}

func TestTypes_Task_UnmarshalYAML_Good_WithItems(t *core.T) {
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

	core.RequireNoError(t, err)
	// with_items should have been stored in Loop
	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 2)
}

func TestTypes_Task_UnmarshalYAML_Good_WithDict(t *core.T) {
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

	core.RequireNoError(t, err)
	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 2)

	first, ok := items[0].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "alpha", first["key"])
	core.AssertEqual(t, "one", first["value"])

	second, ok := items[1].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "beta", second["key"])
	core.AssertEqual(t, "two", second["value"])
}

func TestTypes_Task_UnmarshalYAML_Good_WithIndexedItems(t *core.T) {
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

	core.RequireNoError(t, err)
	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 2)

	first, ok := items[0].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, 0, first[0])
	core.AssertEqual(t, "apple", first[1])

	second, ok := items[1].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, 1, second[0])
	core.AssertEqual(t, "banana", second[1])
}

func TestTypes_Task_UnmarshalYAML_Good_WithFile(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.WithFile)
	files, ok := task.WithFile.([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"templates/a.txt", "templates/b.txt"}, files)
}

func TestTypes_Task_UnmarshalYAML_Good_WithFileGlob(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.WithFileGlob)
	files, ok := task.WithFileGlob.([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"templates/*.txt", "files/*.yml"}, files)
}

func TestTypes_Task_UnmarshalYAML_Good_WithSequence(t *core.T) {
	input := `
name: Read sequence values
debug:
  msg: "{{ item }}"
with_sequence: "start=1 end=3 format=%02d"
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.WithSequence)
	sequence, ok := task.WithSequence.(string)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "start=1 end=3 format=%02d", sequence)
}

func TestTypes_Task_UnmarshalYAML_Good_ActionAlias(t *core.T) {
	input := `
name: Legacy action
action: command echo hello world
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "command", task.Module)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "echo hello world", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_ActionAliasFQCN(t *core.T) {
	input := `
name: Legacy action
ansible.builtin.action: command echo hello world
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "command", task.Module)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "echo hello world", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_ActionAliasKeyValue(t *core.T) {
	input := `
name: Legacy action with args
action: module=copy src=/tmp/source dest=/tmp/dest mode=0644
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "copy", task.Module)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "/tmp/source", task.Args["src"])
	core.AssertEqual(t, "/tmp/dest", task.Args["dest"])
	core.AssertEqual(t, "0644", task.Args["mode"])
}

func TestTypes_Task_UnmarshalYAML_Good_ActionAliasMixedArgs(t *core.T) {
	input := `
name: Legacy action with mixed args
action: command chdir=/tmp echo hello world
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "command", task.Module)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "/tmp", task.Args["chdir"])
	core.AssertEqual(t, "echo hello world", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_LocalAction(t *core.T) {
	input := `
name: Legacy local action
local_action: shell echo local
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "shell", task.Module)
	core.AssertEqual(t, "localhost", task.Delegate)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "echo local", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_LocalActionFQCN(t *core.T) {
	input := `
name: Legacy local action
ansible.legacy.local_action: shell echo local
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "shell", task.Module)
	core.AssertEqual(t, "localhost", task.Delegate)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "echo local", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_LocalActionKeyValue(t *core.T) {
	input := `
name: Legacy local action with args
local_action: module=command chdir=/tmp
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "command", task.Module)
	core.AssertEqual(t, "localhost", task.Delegate)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "/tmp", task.Args["chdir"])
}

func TestTypes_Task_UnmarshalYAML_Good_LocalActionMixedArgs(t *core.T) {
	input := `
name: Legacy local action with mixed args
local_action: command chdir=/var/tmp echo local
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "command", task.Module)
	core.AssertEqual(t, "localhost", task.Delegate)
	core.AssertNotNil(t, task.Args)
	core.AssertEqual(t, "/var/tmp", task.Args["chdir"])
	core.AssertEqual(t, "echo local", task.Args["_raw_params"])
}

func TestTypes_Task_UnmarshalYAML_Good_WithNested(t *core.T) {
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

	core.RequireNoError(t, err)
	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 4)

	first, ok := items[0].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"red", "small"}, first)

	second, ok := items[1].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"red", "large"}, second)

	third, ok := items[2].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"blue", "small"}, third)

	fourth, ok := items[3].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"blue", "large"}, fourth)
}

func TestTypes_Task_UnmarshalYAML_Good_WithTogether(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.WithTogether)

	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 2)

	first, ok := items[0].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"red", "small"}, first)

	second, ok := items[1].([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"blue", "large"}, second)
}

func TestTypes_Task_UnmarshalYAML_Good_WithSubelements(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.WithSubelements)
	values, ok := task.WithSubelements.([]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, []any{"users", "authorized"}, values)
}

func TestTypes_Task_UnmarshalYAML_Good_WithNotify(t *core.T) {
	input := `
name: Install package
apt:
  name: nginx
notify: restart nginx
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "restart nginx", task.Notify)
}

func TestTypes_Task_UnmarshalYAML_Good_WithListen(t *core.T) {
	input := `
name: Restart service
debug:
  msg: "handler"
listen: reload nginx
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "reload nginx", task.Listen)
}

func TestTypes_Task_UnmarshalYAML_Good_ShortFormSystemModules(t *core.T) {
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
		t.Run(tc.name, func(t *core.T) {
			var task Task
			err := yaml.Unmarshal([]byte(tc.input), &task)

			core.RequireNoError(t, err)
			core.AssertEqual(t, tc.wantModule, task.Module)
			core.AssertNotNil(t, task.Args)
		})
	}
}

func TestTypes_Task_UnmarshalYAML_Good_ShortFormCommunityModules(t *core.T) {
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
		t.Run(tc.name, func(t *core.T) {
			var task Task
			err := yaml.Unmarshal([]byte(tc.input), &task)

			core.RequireNoError(t, err)
			core.AssertEqual(t, tc.wantModule, task.Module)
		})
	}
}

func TestTypes_Task_UnmarshalYAML_Good_WithNotifyList(t *core.T) {
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

	core.RequireNoError(t, err)
	notifyList, ok := task.Notify.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, notifyList, 2)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeTasks(t *core.T) {
	input := `
name: Include tasks
include_tasks: other-tasks.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "other-tasks.yml", task.IncludeTasks)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeTasksFQCN(t *core.T) {
	input := `
name: Include tasks
ansible.builtin.include_tasks: other-tasks.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "other-tasks.yml", task.IncludeTasks)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeTasksApply(t *core.T) {
	input := `
name: Include tasks
include_tasks: other-tasks.yml
apply:
  tags:
    - deploy
  become: true
  become_user: root
  delegate_facts: true
  environment:
    APP_ENV: production
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.Apply)
	core.AssertEqual(t, []string{"deploy"}, task.Apply.Tags)
	core.AssertNotNil(t, task.Apply.Become)
	core.AssertTrue(t, *task.Apply.Become)
	core.AssertEqual(t, "root", task.Apply.BecomeUser)
	core.AssertTrue(t, task.Apply.DelegateFacts)
	core.AssertEqual(t, "production", task.Apply.Environment["APP_ENV"])
}

func TestTypes_Task_UnmarshalYAML_Good_DelegateFacts(t *core.T) {
	input := `
name: Gather delegated facts
delegate_to: delegate1
delegate_facts: true
setup:
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, task.DelegateFacts)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeRole(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.IncludeRole)
	core.AssertEqual(t, "common", task.IncludeRole.Name)
	core.AssertEqual(t, "setup.yml", task.IncludeRole.TasksFrom)
	core.AssertEqual(t, "defaults.yml", task.IncludeRole.DefaultsFrom)
	core.AssertEqual(t, "vars.yml", task.IncludeRole.VarsFrom)
	core.AssertEqual(t, "handlers.yml", task.IncludeRole.HandlersFrom)
	core.AssertTrue(t, task.IncludeRole.Public)
	core.AssertNotNil(t, task.IncludeRole.Apply)
	core.AssertEqual(t, []string{"deploy"}, task.IncludeRole.Apply.Tags)
	core.AssertEqual(t, "apply_enabled", task.IncludeRole.Apply.When)
	core.AssertNotNil(t, task.IncludeRole.Apply.Become)
	core.AssertTrue(t, *task.IncludeRole.Apply.Become)
	core.AssertEqual(t, "root", task.IncludeRole.Apply.BecomeUser)
	core.AssertEqual(t, "production", task.IncludeRole.Apply.Environment["APP_ENV"])
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeRoleStringForm(t *core.T) {
	input := `
name: Include role
include_role: common
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.IncludeRole)
	core.AssertEqual(t, "common", task.IncludeRole.Role)
}

func TestTypes_Task_UnmarshalYAML_Good_IncludeRoleFQCN(t *core.T) {
	input := `
name: Include role
ansible.builtin.include_role:
  name: common
  tasks_from: setup.yml
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.IncludeRole)
	core.AssertEqual(t, "common", task.IncludeRole.Role)
	core.AssertEqual(t, "setup.yml", task.IncludeRole.TasksFrom)
}

func TestTypes_Task_UnmarshalYAML_Good_ImportRoleStringForm(t *core.T) {
	input := `
name: Import role
import_role: common
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.ImportRole)
	core.AssertEqual(t, "common", task.ImportRole.Role)
}

func TestTypes_Task_UnmarshalYAML_Good_ImportRoleFQCN(t *core.T) {
	input := `
name: Import role
ansible.builtin.import_role: common
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.ImportRole)
	core.AssertEqual(t, "common", task.ImportRole.Role)
}

func TestTypes_Task_UnmarshalYAML_Good_BecomeFields(t *core.T) {
	input := `
name: Privileged task
shell: systemctl restart nginx
become: true
become_user: root
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, task.Become)
	core.AssertTrue(t, *task.Become)
	core.AssertEqual(t, "root", task.BecomeUser)
}

func TestTypes_Task_UnmarshalYAML_Good_IgnoreErrors(t *core.T) {
	input := `
name: Might fail
shell: some risky command
ignore_errors: true
`
	var task Task
	err := yaml.Unmarshal([]byte(input), &task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, task.IgnoreErrors)
}

// --- Inventory data structure ---

func TestTypes_Inventory_UnmarshalYAML_Good_Complex(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, inv.All)

	// Check top-level vars
	core.AssertEqual(t, "admin", inv.All.Vars["ansible_user"])

	// Check top-level hosts
	core.AssertNotNil(t, inv.All.Hosts["bastion"])
	core.AssertEqual(t, "1.2.3.4", inv.All.Hosts["bastion"].AnsibleHost)
	core.AssertEqual(t, 4819, inv.All.Hosts["bastion"].AnsiblePort)

	// Check children
	core.AssertNotNil(t, inv.All.Children["webservers"])
	core.AssertLen(t, inv.All.Children["webservers"].Hosts, 2)
	core.AssertEqual(t, 80, inv.All.Children["webservers"].Vars["http_port"])

	core.AssertNotNil(t, inv.All.Children["databases"])
	core.AssertEqual(t, "ssh", inv.All.Children["databases"].Hosts["db1"].AnsibleConnection)
}

// --- Facts ---

func TestTypes_Facts_Good_Struct(t *core.T) {
	facts := Facts{
		Hostname:           "web1",
		FQDN:               "web1.example.com",
		OS:                 "Debian",
		Distribution:       "ubuntu",
		Version:            "24.04",
		Architecture:       "x86_64",
		Kernel:             "6.8.0",
		VirtualizationRole: "guest",
		VirtualizationType: "docker",
		Memory:             16384,
		CPUs:               4,
		IPv4:               "10.0.0.1",
	}

	core.AssertEqual(t, "web1", facts.Hostname)
	core.AssertEqual(t, "web1.example.com", facts.FQDN)
	core.AssertEqual(t, "ubuntu", facts.Distribution)
	core.AssertEqual(t, "x86_64", facts.Architecture)
	core.AssertEqual(t, "guest", facts.VirtualizationRole)
	core.AssertEqual(t, "docker", facts.VirtualizationType)
	core.AssertEqual(t, int64(16384), facts.Memory)
	core.AssertEqual(t, 4, facts.CPUs)
}

// --- TaskResult ---

func TestTypes_TaskResult_Good_Struct(t *core.T) {
	result := TaskResult{
		Changed: true,
		Failed:  false,
		Skipped: false,
		Msg:     "task completed",
		Stdout:  "output",
		Stderr:  "",
		RC:      0,
	}

	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "task completed", result.Msg)
	core.AssertEqual(t, 0, result.RC)
}

func TestTypes_TaskResult_Good_WithLoopResults(t *core.T) {
	result := TaskResult{
		Changed: true,
		Results: []TaskResult{
			{Changed: true, RC: 0},
			{Changed: false, RC: 0},
			{Changed: true, RC: 0},
		},
	}

	core.AssertLen(t, result.Results, 3)
	core.AssertTrue(t, result.Results[0].Changed)
	core.AssertFalse(t, result.Results[1].Changed)
}

// --- KnownModules ---

func TestTypes_KnownModules_Good_ContainsExpected(t *core.T) {
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
		core.AssertContains(t, KnownModules, mod, core.Sprintf("expected FQCN module %s", mod))
	}

	shortModules := []string{
		"shell", "command", "copy", "file", "apt", "service",
		"systemd", "rpm", "debug", "set_fact", "ping", "template", "user", "group",
	}
	for _, mod := range shortModules {
		core.AssertContains(t, KnownModules, mod, core.Sprintf("expected short-form module %s", mod))
	}
}

// --- File-aware public symbol triplets ---

func TestTypes_Play_UnmarshalYAML_Good(t *core.T) {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: all\nansible.builtin.import_playbook: child.yml\n"), &play)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "all", play.Hosts)
	core.AssertEqual(t, "child.yml", play.ImportPlaybook)
}

func TestTypes_Play_UnmarshalYAML_Bad(t *core.T) {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: ["), &play)
	core.AssertError(t, err)
	core.AssertEmpty(t, play.Hosts)
}

func TestTypes_Play_UnmarshalYAML_Ugly(t *core.T) {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: localhost\ngather_facts: false\n"), &play)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, play.GatherFacts)
	core.AssertFalse(t, *play.GatherFacts)
}

func TestTypes_RoleRef_UnmarshalYAML_Good(t *core.T) {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("web\n"), &ref)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "web", ref.Role)
}

func TestTypes_RoleRef_UnmarshalYAML_Bad(t *core.T) {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("- web\n"), &ref)
	core.AssertError(t, err)
	core.AssertEmpty(t, ref.Role)
}

func TestTypes_RoleRef_UnmarshalYAML_Ugly(t *core.T) {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("name: db\ntags: [setup]\n"), &ref)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "db", ref.Role)
	core.AssertEqual(t, []string{"setup"}, ref.Tags)
}

func TestTypes_Inventory_UnmarshalYAML_Good(t *core.T) {
	var inv Inventory
	err := yaml.Unmarshal([]byte("all:\n  hosts:\n    web1: {}\n"), &inv)
	core.AssertNoError(t, err)
	core.AssertContains(t, inv.All.Hosts, "web1")
}

func TestTypes_Inventory_UnmarshalYAML_Bad(t *core.T) {
	var inv Inventory
	err := yaml.Unmarshal([]byte("all: ["), &inv)
	core.AssertError(t, err)
	core.AssertNil(t, inv.All)
}

func TestTypes_Inventory_UnmarshalYAML_Ugly(t *core.T) {
	var inv Inventory
	err := yaml.Unmarshal([]byte("web:\n  hosts:\n    web1: {}\nhost_vars:\n  web1:\n    role: app\n"), &inv)
	core.AssertNoError(t, err)
	core.AssertContains(t, inv.All.Children, "web")
	core.AssertEqual(t, "app", inv.HostVars["web1"]["role"])
}
