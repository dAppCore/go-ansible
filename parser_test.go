package ansible

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ParsePlaybook ---

func TestParser_ParsePlaybook_Good_SimplePlay(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Configure webserver
  hosts: webservers
  become: true
  tasks:
    - name: Install nginx
      apt:
        name: nginx
        state: present
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays, 1)
	assert.Equal(t, "Configure webserver", plays[0].Name)
	assert.Equal(t, "webservers", plays[0].Hosts)
	assert.True(t, plays[0].Become)
	require.Len(t, plays[0].Tasks, 1)
	assert.Equal(t, "Install nginx", plays[0].Tasks[0].Name)
	assert.Equal(t, "apt", plays[0].Tasks[0].Module)
	assert.Equal(t, "nginx", plays[0].Tasks[0].Args["name"])
	assert.Equal(t, "present", plays[0].Tasks[0].Args["state"])
}

func TestParser_ParsePlaybook_Good_MultiplePlays(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Play one
  hosts: all
  tasks:
    - name: Say hello
      debug:
        msg: "Hello"

- name: Play two
  hosts: localhost
  connection: local
  tasks:
    - name: Say goodbye
      debug:
        msg: "Goodbye"
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays, 2)
	assert.Equal(t, "Play one", plays[0].Name)
	assert.Equal(t, "all", plays[0].Hosts)
	assert.Equal(t, "Play two", plays[1].Name)
	assert.Equal(t, "localhost", plays[1].Hosts)
	assert.Equal(t, "local", plays[1].Connection)
}

func TestParser_ParsePlaybook_Good_ImportPlaybook(t *testing.T) {
	dir := t.TempDir()
	mainPath := joinPath(dir, "site.yml")
	importDir := joinPath(dir, "plays")
	importPath := joinPath(importDir, "web.yml")

	yamlMain := `---
- name: Before import
  hosts: all
  tasks:
    - name: Say before
      debug:
        msg: "before"

- import_playbook: plays/web.yml

- name: After import
  hosts: all
  tasks:
    - name: Say after
      debug:
        msg: "after"
`
	yamlImported := `---
- name: Imported play
  hosts: webservers
  tasks:
    - name: Say imported
      debug:
        msg: "imported"
`
	require.NoError(t, os.MkdirAll(importDir, 0755))
	require.NoError(t, writeTestFile(mainPath, []byte(yamlMain), 0644))
	require.NoError(t, writeTestFile(importPath, []byte(yamlImported), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(mainPath)

	require.NoError(t, err)
	require.Len(t, plays, 3)
	assert.Equal(t, "Before import", plays[0].Name)
	assert.Equal(t, "Imported play", plays[1].Name)
	assert.Equal(t, "After import", plays[2].Name)
	assert.Equal(t, "webservers", plays[1].Hosts)
	assert.Len(t, plays[1].Tasks, 1)
	assert.Equal(t, "Say imported", plays[1].Tasks[0].Name)
}

func TestParser_ParsePlaybook_Good_WithVars(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: With vars
  hosts: all
  vars:
    http_port: 8080
    app_name: myapp
  tasks:
    - name: Print port
      debug:
        msg: "Port is {{ http_port }}"
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays, 1)
	assert.Equal(t, 8080, plays[0].Vars["http_port"])
	assert.Equal(t, "myapp", plays[0].Vars["app_name"])
}

func TestParser_ParsePlaybook_Good_PrePostTasks(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Full lifecycle
  hosts: all
  pre_tasks:
    - name: Pre task
      debug:
        msg: "pre"
  tasks:
    - name: Main task
      debug:
        msg: "main"
  post_tasks:
    - name: Post task
      debug:
        msg: "post"
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays, 1)
	assert.Len(t, plays[0].PreTasks, 1)
	assert.Len(t, plays[0].Tasks, 1)
	assert.Len(t, plays[0].PostTasks, 1)
	assert.Equal(t, "Pre task", plays[0].PreTasks[0].Name)
	assert.Equal(t, "Main task", plays[0].Tasks[0].Name)
	assert.Equal(t, "Post task", plays[0].PostTasks[0].Name)
}

func TestParser_ParsePlaybook_Good_Handlers(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: With handlers
  hosts: all
  tasks:
    - name: Install package
      apt:
        name: nginx
      notify: restart nginx
  handlers:
    - name: restart nginx
      service:
        name: nginx
        state: restarted
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays, 1)
	assert.Len(t, plays[0].Handlers, 1)
	assert.Equal(t, "restart nginx", plays[0].Handlers[0].Name)
	assert.Equal(t, "service", plays[0].Handlers[0].Module)
}

func TestParser_ParsePlaybook_Good_ShellFreeForm(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Shell tasks
  hosts: all
  tasks:
    - name: Run a command
      shell: echo hello world
    - name: Run raw command
      command: ls -la /tmp
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays[0].Tasks, 2)
	assert.Equal(t, "shell", plays[0].Tasks[0].Module)
	assert.Equal(t, "echo hello world", plays[0].Tasks[0].Args["_raw_params"])
	assert.Equal(t, "command", plays[0].Tasks[1].Module)
	assert.Equal(t, "ls -la /tmp", plays[0].Tasks[1].Args["_raw_params"])
}

func TestParser_ParsePlaybook_Good_WithTags(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Tagged play
  hosts: all
  tags:
    - setup
  tasks:
    - name: Tagged task
      debug:
        msg: "tagged"
      tags:
        - debug
        - always
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	assert.Equal(t, []string{"setup"}, plays[0].Tags)
	assert.Equal(t, []string{"debug", "always"}, plays[0].Tasks[0].Tags)
}

func TestParser_ParsePlaybook_Good_BlockRescueAlways(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: With blocks
  hosts: all
  tasks:
    - name: Protected block
      block:
        - name: Try this
          shell: echo try
      rescue:
        - name: Handle error
          debug:
            msg: "rescued"
      always:
        - name: Always runs
          debug:
            msg: "always"
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	task := plays[0].Tasks[0]
	assert.Len(t, task.Block, 1)
	assert.Len(t, task.Rescue, 1)
	assert.Len(t, task.Always, 1)
	assert.Equal(t, "Try this", task.Block[0].Name)
	assert.Equal(t, "Handle error", task.Rescue[0].Name)
	assert.Equal(t, "Always runs", task.Always[0].Name)
}

func TestParser_ParsePlaybook_Good_WithLoop(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Loop test
  hosts: all
  tasks:
    - name: Install packages
      apt:
        name: "{{ item }}"
        state: present
      loop:
        - vim
        - curl
        - git
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	task := plays[0].Tasks[0]
	assert.Equal(t, "apt", task.Module)
	items, ok := task.Loop.([]any)
	require.True(t, ok)
	assert.Len(t, items, 3)
}

func TestParser_ParsePlaybook_Good_RoleRefs(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: With roles
  hosts: all
  roles:
    - common
    - role: webserver
      vars:
        http_port: 80
      tags:
        - web
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.Len(t, plays[0].Roles, 2)
	assert.Equal(t, "common", plays[0].Roles[0].Role)
	assert.Equal(t, "webserver", plays[0].Roles[1].Role)
	assert.Equal(t, 80, plays[0].Roles[1].Vars["http_port"])
	assert.Equal(t, []string{"web"}, plays[0].Roles[1].Tags)
}

func TestParser_ParsePlaybook_Good_FullyQualifiedModules(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: FQCN modules
  hosts: all
  tasks:
    - name: Copy file
      ansible.builtin.copy:
        src: /tmp/foo
        dest: /tmp/bar
    - name: Run shell
      ansible.builtin.shell: echo hello
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	assert.Equal(t, "ansible.builtin.copy", plays[0].Tasks[0].Module)
	assert.Equal(t, "/tmp/foo", plays[0].Tasks[0].Args["src"])
	assert.Equal(t, "ansible.builtin.shell", plays[0].Tasks[1].Module)
	assert.Equal(t, "echo hello", plays[0].Tasks[1].Args["_raw_params"])
}

func TestParser_ParsePlaybook_Good_RegisterAndWhen(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Conditional play
  hosts: all
  tasks:
    - name: Check file
      stat:
        path: /etc/nginx/nginx.conf
      register: nginx_conf
    - name: Show result
      debug:
        msg: "File exists"
      when: nginx_conf.stat.exists
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	assert.Equal(t, "nginx_conf", plays[0].Tasks[0].Register)
	assert.NotNil(t, plays[0].Tasks[1].When)
}

func TestParser_ParsePlaybook_Good_EmptyPlaybook(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	require.NoError(t, writeTestFile(path, []byte("---\n[]"), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	assert.Empty(t, plays)
}

func TestParser_ParsePlaybook_Bad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "bad.yml")

	require.NoError(t, writeTestFile(path, []byte("{{invalid yaml}}"), 0644))

	p := NewParser(dir)
	_, err := p.ParsePlaybook(path)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse playbook")
}

func TestParser_ParsePlaybook_Bad_FileNotFound(t *testing.T) {
	p := NewParser(t.TempDir())
	_, err := p.ParsePlaybook("/nonexistent/playbook.yml")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read playbook")
}

func TestParser_ParsePlaybook_Good_GatherFactsDisabled(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: No facts
  hosts: all
  gather_facts: false
  tasks: []
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	require.NoError(t, err)
	require.NotNil(t, plays[0].GatherFacts)
	assert.False(t, *plays[0].GatherFacts)
}

// --- ParseInventory ---

func TestParser_ParseInventory_Good_SimpleInventory(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")

	yaml := `---
all:
  hosts:
    web1:
      ansible_host: 192.168.1.10
    web2:
      ansible_host: 192.168.1.11
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	require.NoError(t, err)
	require.NotNil(t, inv.All)
	assert.Len(t, inv.All.Hosts, 2)
	assert.Equal(t, "192.168.1.10", inv.All.Hosts["web1"].AnsibleHost)
	assert.Equal(t, "192.168.1.11", inv.All.Hosts["web2"].AnsibleHost)
}

func TestParser_ParseInventory_Good_WithGroups(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")

	yaml := `---
all:
  children:
    webservers:
      hosts:
        web1:
          ansible_host: 10.0.0.1
        web2:
          ansible_host: 10.0.0.2
    databases:
      hosts:
        db1:
          ansible_host: 10.0.1.1
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	require.NoError(t, err)
	require.NotNil(t, inv.All.Children["webservers"])
	assert.Len(t, inv.All.Children["webservers"].Hosts, 2)
	require.NotNil(t, inv.All.Children["databases"])
	assert.Len(t, inv.All.Children["databases"].Hosts, 1)
}

func TestParser_ParseInventory_Good_WithVars(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")

	yaml := `---
all:
  vars:
    ansible_user: admin
  children:
    production:
      vars:
        env: prod
      hosts:
        prod1:
          ansible_host: 10.0.0.1
          ansible_port: 2222
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	require.NoError(t, err)
	assert.Equal(t, "admin", inv.All.Vars["ansible_user"])
	assert.Equal(t, "prod", inv.All.Children["production"].Vars["env"])
	assert.Equal(t, 2222, inv.All.Children["production"].Hosts["prod1"].AnsiblePort)
}

func TestParser_ParseInventory_Bad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "bad.yml")

	require.NoError(t, writeTestFile(path, []byte("{{{bad"), 0644))

	p := NewParser(dir)
	_, err := p.ParseInventory(path)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse inventory")
}

func TestParser_ParseInventory_Bad_FileNotFound(t *testing.T) {
	p := NewParser(t.TempDir())
	_, err := p.ParseInventory("/nonexistent/inventory.yml")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read inventory")
}

// --- ParseTasks ---

func TestParser_ParseTasks_Good_TaskFile(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "tasks.yml")

	yaml := `---
- name: First task
  shell: echo first
- name: Second task
  copy:
    src: /tmp/a
    dest: /tmp/b
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	tasks, err := p.ParseTasks(path)

	require.NoError(t, err)
	require.Len(t, tasks, 2)
	assert.Equal(t, "shell", tasks[0].Module)
	assert.Equal(t, "echo first", tasks[0].Args["_raw_params"])
	assert.Equal(t, "copy", tasks[1].Module)
	assert.Equal(t, "/tmp/a", tasks[1].Args["src"])
}

func TestParser_ParseTasks_Bad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "bad.yml")

	require.NoError(t, writeTestFile(path, []byte("not: [valid: tasks"), 0644))

	p := NewParser(dir)
	_, err := p.ParseTasks(path)

	assert.Error(t, err)
}

// --- GetHosts ---

func TestParser_GetHosts_Good_AllPattern(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
			},
		},
	}

	hosts := GetHosts(inv, "all")
	assert.Len(t, hosts, 2)
	assert.Contains(t, hosts, "host1")
	assert.Contains(t, hosts, "host2")
}

func TestParser_GetHosts_Good_LocalhostPattern(t *testing.T) {
	inv := &Inventory{All: &InventoryGroup{}}
	hosts := GetHosts(inv, "localhost")
	assert.Equal(t, []string{"localhost"}, hosts)
}

func TestParser_GetHosts_Good_GroupPattern(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"web": {
					Hosts: map[string]*Host{
						"web1": {},
						"web2": {},
					},
				},
				"db": {
					Hosts: map[string]*Host{
						"db1": {},
					},
				},
			},
		},
	}

	hosts := GetHosts(inv, "web")
	assert.Len(t, hosts, 2)
	assert.Contains(t, hosts, "web1")
	assert.Contains(t, hosts, "web2")
}

func TestParser_GetHosts_Good_SpecificHost(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"servers": {
					Hosts: map[string]*Host{
						"myhost": {},
					},
				},
			},
		},
	}

	hosts := GetHosts(inv, "myhost")
	assert.Equal(t, []string{"myhost"}, hosts)
}

func TestParser_GetHosts_Good_ColonUnionIntersectionExclusion(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"web": {
					Hosts: map[string]*Host{
						"web1": {},
						"web2": {},
					},
				},
				"db": {
					Hosts: map[string]*Host{
						"db1":  {},
						"web2": {},
					},
				},
				"canary": {
					Hosts: map[string]*Host{
						"web2": {},
						"db1":  {},
					},
				},
			},
		},
	}

	assert.Equal(t, []string{"web1", "web2", "db1"}, GetHosts(inv, "web:db"))
	assert.Equal(t, []string{"web2"}, GetHosts(inv, "web:&db"))
	assert.Equal(t, []string{"web1"}, GetHosts(inv, "web:!canary"))
	assert.Equal(t, []string{"web1"}, GetHosts(inv, "web:db:!canary"))
}

func TestParser_GetHosts_Good_AllIncludesChildren(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{"top": {}},
			Children: map[string]*InventoryGroup{
				"group1": {
					Hosts: map[string]*Host{"child1": {}},
				},
			},
		},
	}

	hosts := GetHosts(inv, "all")
	assert.Len(t, hosts, 2)
	assert.Contains(t, hosts, "top")
	assert.Contains(t, hosts, "child1")
}

func TestParser_GetHosts_Bad_NoMatch(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{"host1": {}},
		},
	}

	hosts := GetHosts(inv, "nonexistent")
	assert.Empty(t, hosts)
}

func TestParser_GetHosts_Bad_NilGroup(t *testing.T) {
	inv := &Inventory{All: nil}
	hosts := GetHosts(inv, "all")
	assert.Empty(t, hosts)
}

// --- GetHostVars ---

func TestParser_GetHostVars_Good_DirectHost(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Vars: map[string]any{"global_var": "global"},
			Hosts: map[string]*Host{
				"myhost": {
					AnsibleHost: "10.0.0.1",
					AnsiblePort: 2222,
					AnsibleUser: "deploy",
				},
			},
		},
	}

	vars := GetHostVars(inv, "myhost")
	assert.Equal(t, "10.0.0.1", vars["ansible_host"])
	assert.Equal(t, 2222, vars["ansible_port"])
	assert.Equal(t, "deploy", vars["ansible_user"])
	assert.Equal(t, "global", vars["global_var"])
}

func TestParser_GetHostVars_Good_InheritedGroupVars(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Vars: map[string]any{"level": "all"},
			Children: map[string]*InventoryGroup{
				"production": {
					Vars: map[string]any{"env": "prod", "level": "group"},
					Hosts: map[string]*Host{
						"prod1": {
							AnsibleHost: "10.0.0.1",
						},
					},
				},
			},
		},
	}

	vars := GetHostVars(inv, "prod1")
	assert.Equal(t, "10.0.0.1", vars["ansible_host"])
	assert.Equal(t, "prod", vars["env"])
}

func TestParser_GetHostVars_Good_HostNotFound(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{"other": {}},
		},
	}

	vars := GetHostVars(inv, "nonexistent")
	assert.Empty(t, vars)
}

// --- isModule ---

func TestParser_IsModule_Good_KnownModules(t *testing.T) {
	assert.True(t, isModule("shell"))
	assert.True(t, isModule("command"))
	assert.True(t, isModule("copy"))
	assert.True(t, isModule("file"))
	assert.True(t, isModule("apt"))
	assert.True(t, isModule("service"))
	assert.True(t, isModule("systemd"))
	assert.True(t, isModule("debug"))
	assert.True(t, isModule("set_fact"))
}

func TestParser_IsModule_Good_FQCN(t *testing.T) {
	assert.True(t, isModule("ansible.builtin.shell"))
	assert.True(t, isModule("ansible.builtin.copy"))
	assert.True(t, isModule("ansible.builtin.apt"))
}

func TestParser_IsModule_Good_DottedUnknown(t *testing.T) {
	// Any key with dots is considered a module
	assert.True(t, isModule("community.general.ufw"))
	assert.True(t, isModule("ansible.posix.authorized_key"))
}

func TestParser_IsModule_Bad_NotAModule(t *testing.T) {
	assert.False(t, isModule("some_random_key"))
	assert.False(t, isModule("foobar"))
}

// --- NormalizeModule ---

func TestParser_NormalizeModule_Good(t *testing.T) {
	assert.Equal(t, "ansible.builtin.shell", NormalizeModule("shell"))
	assert.Equal(t, "ansible.builtin.copy", NormalizeModule("copy"))
	assert.Equal(t, "ansible.builtin.apt", NormalizeModule("apt"))
}

func TestParser_NormalizeModule_Good_CommunityAliases(t *testing.T) {
	assert.Equal(t, "ansible.posix.authorized_key", NormalizeModule("authorized_key"))
	assert.Equal(t, "community.general.ufw", NormalizeModule("ufw"))
	assert.Equal(t, "community.docker.docker_compose", NormalizeModule("docker_compose"))
	assert.Equal(t, "community.docker.docker_compose_v2", NormalizeModule("docker_compose_v2"))
	assert.Equal(t, "community.docker.docker_compose", NormalizeModule("ansible.builtin.docker_compose"))
}

func TestParser_NormalizeModule_Good_AlreadyFQCN(t *testing.T) {
	assert.Equal(t, "ansible.builtin.shell", NormalizeModule("ansible.builtin.shell"))
	assert.Equal(t, "community.general.ufw", NormalizeModule("community.general.ufw"))
}

// --- NewParser ---

func TestParser_NewParser_Good(t *testing.T) {
	p := NewParser("/some/path")
	assert.NotNil(t, p)
	assert.Equal(t, "/some/path", p.basePath)
	assert.NotNil(t, p.vars)
}
