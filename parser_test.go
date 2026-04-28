package ansible

import (
	core "dappco.re/go"
	"os"

	coreio "dappco.re/go/io"
)

// --- ParsePlaybook ---

func TestParser_ParsePlaybook_Good_SimplePlay(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertEqual(t, "Configure webserver", plays[0].Name)
	core.AssertEqual(t, "webservers", plays[0].Hosts)
	core.AssertTrue(t, plays[0].Become)
	core.AssertLen(t, plays[0].Tasks, 1)
	core.AssertEqual(t, "Install nginx", plays[0].Tasks[0].Name)
	core.AssertEqual(t, "apt", plays[0].Tasks[0].Module)
	core.AssertEqual(t, "nginx", plays[0].Tasks[0].Args["name"])
	core.AssertEqual(t, "present", plays[0].Tasks[0].Args["state"])
}

func TestParser_ParsePlaybook_Good_MultiplePlays(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 2)
	core.AssertEqual(t, "Play one", plays[0].Name)
	core.AssertEqual(t, "all", plays[0].Hosts)
	core.AssertEqual(t, "Play two", plays[1].Name)
	core.AssertEqual(t, "localhost", plays[1].Hosts)
	core.AssertEqual(t, "local", plays[1].Connection)
}

func TestParser_ParsePlaybook_Good_ImportPlaybook(t *core.T) {
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
	core.RequireNoError(t, os.MkdirAll(importDir, 0755))
	core.RequireNoError(t, writeTestFile(mainPath, []byte(yamlMain), 0644))
	core.RequireNoError(t, writeTestFile(importPath, []byte(yamlImported), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook("site.yml")

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 3)
	core.AssertEqual(t, "Before import", plays[0].Name)
	core.AssertEqual(t, "Imported play", plays[1].Name)
	core.AssertEqual(t, "After import", plays[2].Name)
	core.AssertEqual(t, "webservers", plays[1].Hosts)
	core.AssertLen(t, plays[1].Tasks, 1)
	core.AssertEqual(t, "Say imported", plays[1].Tasks[0].Name)
}

func TestParser_ParsePlaybook_Good_TemplatedImportPlaybook(t *core.T) {
	dir := t.TempDir()
	mainPath := joinPath(dir, "site.yml")
	importDir := joinPath(dir, "plays")
	importPath := joinPath(importDir, "web.yml")

	yamlMain := `---
- import_playbook: "{{ playbook_dir }}/plays/web.yml"
`
	yamlImported := `---
- name: Imported play
  hosts: all
  tasks:
    - name: Say imported
      debug:
        msg: "imported"
`
	core.RequireNoError(t, os.MkdirAll(importDir, 0755))
	core.RequireNoError(t, writeTestFile(mainPath, []byte(yamlMain), 0644))
	core.RequireNoError(t, writeTestFile(importPath, []byte(yamlImported), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook("site.yml")

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertEqual(t, "Imported play", plays[0].Name)
	core.AssertEqual(t, "all", plays[0].Hosts)
	core.AssertNotNil(t, plays[0].Vars)
	core.AssertEqual(t, dir, plays[0].Vars["playbook_dir"])
}

func TestParser_ParsePlaybook_Good_FQCNImportPlaybook(t *core.T) {
	dir := t.TempDir()
	mainPath := joinPath(dir, "site.yml")
	importDir := joinPath(dir, "plays")
	importPath := joinPath(importDir, "web.yml")

	yamlMain := `---
- ansible.builtin.import_playbook: plays/web.yml
`
	yamlImported := `---
- name: Imported play
  hosts: all
  tasks:
    - name: Say imported
      debug:
        msg: "imported"
`
	core.RequireNoError(t, os.MkdirAll(importDir, 0755))
	core.RequireNoError(t, writeTestFile(mainPath, []byte(yamlMain), 0644))
	core.RequireNoError(t, writeTestFile(importPath, []byte(yamlImported), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(mainPath)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertEqual(t, "Imported play", plays[0].Name)
	core.AssertEqual(t, "all", plays[0].Hosts)
}

func TestParser_ParsePlaybook_Good_NestedImportPlaybookDirScope(t *core.T) {
	dir := t.TempDir()
	mainPath := joinPath(dir, "site.yml")
	outerDir := joinPath(dir, "plays")
	outerPath := joinPath(outerDir, "outer.yml")
	innerDir := joinPath(outerDir, "nested")
	innerPath := joinPath(innerDir, "inner.yml")

	yamlMain := `---
- import_playbook: plays/outer.yml
`
	yamlOuter := `---
- import_playbook: "{{ playbook_dir }}/nested/inner.yml"
`
	yamlInner := `---
- name: Inner play
  hosts: all
  tasks:
    - name: Say inner
      debug:
        msg: "inner"
`
	core.RequireNoError(t, os.MkdirAll(innerDir, 0755))
	core.RequireNoError(t, writeTestFile(mainPath, []byte(yamlMain), 0644))
	core.RequireNoError(t, writeTestFile(outerPath, []byte(yamlOuter), 0644))
	core.RequireNoError(t, writeTestFile(innerPath, []byte(yamlInner), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook("site.yml")

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertEqual(t, "Inner play", plays[0].Name)
	core.AssertNotNil(t, plays[0].Vars)
	core.AssertEqual(t, dir, plays[0].Vars["playbook_dir"])
}

func TestParser_ParsePlaybook_Good_WithVars(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertEqual(t, 8080, plays[0].Vars["http_port"])
	core.AssertEqual(t, "myapp", plays[0].Vars["app_name"])
}

func TestParser_ParsePlaybook_Good_PrePostTasks(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertLen(t, plays[0].PreTasks, 1)
	core.AssertLen(t, plays[0].Tasks, 1)
	core.AssertLen(t, plays[0].PostTasks, 1)
	core.AssertEqual(t, "Pre task", plays[0].PreTasks[0].Name)
	core.AssertEqual(t, "Main task", plays[0].Tasks[0].Name)
	core.AssertEqual(t, "Post task", plays[0].PostTasks[0].Name)
}

func TestParser_ParsePlaybook_Good_Handlers(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertLen(t, plays[0].Handlers, 1)
	core.AssertEqual(t, "restart nginx", plays[0].Handlers[0].Name)
	core.AssertEqual(t, "service", plays[0].Handlers[0].Module)
}

func TestParser_ParsePlaybook_Good_ShellFreeForm(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays[0].Tasks, 2)
	core.AssertEqual(t, "shell", plays[0].Tasks[0].Module)
	core.AssertEqual(t, "echo hello world", plays[0].Tasks[0].Args["_raw_params"])
	core.AssertEqual(t, "command", plays[0].Tasks[1].Module)
	core.AssertEqual(t, "ls -la /tmp", plays[0].Tasks[1].Args["_raw_params"])
}

func TestParser_ParsePlaybook_Good_WithTags(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertEqual(t, []string{"setup"}, plays[0].Tags)
	core.AssertEqual(t, []string{"debug", "always"}, plays[0].Tasks[0].Tags)
}

func TestParser_ParsePlaybook_Good_BlockRescueAlways(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	task := plays[0].Tasks[0]
	core.AssertLen(t, task.Block, 1)
	core.AssertLen(t, task.Rescue, 1)
	core.AssertLen(t, task.Always, 1)
	core.AssertEqual(t, "Try this", task.Block[0].Name)
	core.AssertEqual(t, "Handle error", task.Rescue[0].Name)
	core.AssertEqual(t, "Always runs", task.Always[0].Name)
}

func TestParser_ParsePlaybook_Good_WithLoop(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	task := plays[0].Tasks[0]
	core.AssertEqual(t, "apt", task.Module)
	items, ok := task.Loop.([]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, items, 3)
}

func TestParser_ParsePlaybook_Good_RoleRefs(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays[0].Roles, 2)
	core.AssertEqual(t, "common", plays[0].Roles[0].Role)
	core.AssertEqual(t, "webserver", plays[0].Roles[1].Role)
	core.AssertEqual(t, 80, plays[0].Roles[1].Vars["http_port"])
	core.AssertEqual(t, []string{"web"}, plays[0].Roles[1].Tags)
}

func TestParser_ParsePlaybook_Good_FullyQualifiedModules(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "ansible.builtin.copy", plays[0].Tasks[0].Module)
	core.AssertEqual(t, "/tmp/foo", plays[0].Tasks[0].Args["src"])
	core.AssertEqual(t, "ansible.builtin.shell", plays[0].Tasks[1].Module)
	core.AssertEqual(t, "echo hello", plays[0].Tasks[1].Args["_raw_params"])
}

func TestParser_ParsePlaybook_Good_RegisterAndWhen(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "nginx_conf", plays[0].Tasks[0].Register)
	core.AssertNotNil(t, plays[0].Tasks[1].When)
}

func TestParser_ParsePlaybook_Good_EmptyPlaybook(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	core.RequireNoError(t, writeTestFile(path, []byte("---\n[]"), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertEmpty(t, plays)
}

func TestParser_ParsePlaybook_Bad_InvalidYAML(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "bad.yml")

	core.RequireNoError(t, writeTestFile(path, []byte("{{invalid yaml}}"), 0644))

	p := NewParser(dir)
	_, err := p.ParsePlaybook(path)

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "parse playbook")
}

func TestParser_ParsePlaybook_Bad_FileNotFound(t *core.T) {
	p := NewParser(t.TempDir())
	_, err := p.ParsePlaybook("/nonexistent/playbook.yml")

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "read playbook")
}

func TestParser_ParsePlaybook_Good_GatherFactsDisabled(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: No facts
  hosts: all
  gather_facts: false
  tasks: []
`
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, plays[0].GatherFacts)
	core.AssertFalse(t, *plays[0].GatherFacts)
}

func TestParser_ParsePlaybook_Good_ForceHandlers(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")

	yaml := `---
- name: Handler control
  hosts: all
  force_handlers: true
  any_errors_fatal: true
  tasks: []
`
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	plays, err := p.ParsePlaybook(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, plays, 1)
	core.AssertTrue(t, plays[0].ForceHandlers)
	core.AssertTrue(t, plays[0].AnyErrorsFatal)
}

// --- ParseInventory ---

func TestParser_ParseInventory_Good_SimpleInventory(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, inv.All)
	core.AssertLen(t, inv.All.Hosts, 2)
	core.AssertEqual(t, "192.168.1.10", inv.All.Hosts["web1"].AnsibleHost)
	core.AssertEqual(t, "192.168.1.11", inv.All.Hosts["web2"].AnsibleHost)
}

func TestParser_ParseInventory_Good_DirectoryInventory(t *core.T) {
	dir := t.TempDir()
	inventoryDir := joinPath(dir, "inventory")
	core.RequireNoError(t, os.MkdirAll(inventoryDir, 0755))

	path := joinPath(inventoryDir, "hosts.yml")
	yaml := `---
all:
  hosts:
    web1:
      ansible_host: 192.168.1.10
`
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(inventoryDir)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, inv.All)
	core.AssertContains(t, inv.All.Hosts, "web1")
	core.AssertEqual(t, "192.168.1.10", inv.All.Hosts["web1"].AnsibleHost)
}

func TestParser_ParseInventory_Good_WithGroups(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, inv.All.Children["webservers"])
	core.AssertLen(t, inv.All.Children["webservers"].Hosts, 2)
	core.AssertNotNil(t, inv.All.Children["databases"])
	core.AssertLen(t, inv.All.Children["databases"].Hosts, 1)
}

func TestParser_ParseInventory_Good_TopLevelGroups(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")

	yaml := `---
webservers:
  vars:
    tier: web
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, inv.All)
	core.AssertNotNil(t, inv.All.Children["webservers"])
	core.AssertNotNil(t, inv.All.Children["databases"])
	core.AssertLen(t, inv.All.Children["webservers"].Hosts, 2)
	core.AssertLen(t, inv.All.Children["databases"].Hosts, 1)
	core.AssertEqual(t, "web", inv.All.Children["webservers"].Vars["tier"])
	core.AssertElementsMatch(t, []string{"web1", "web2", "db1"}, GetHosts(inv, "all"))
	core.AssertEqual(t, []string{"web1", "web2"}, GetHosts(inv, "webservers"))
	core.AssertEqual(t, "web", GetHostVars(inv, "web1")["tier"])
}

func TestParser_ParseInventory_Good_WithVars(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	core.RequireNoError(t, err)
	core.AssertEqual(t, "admin", inv.All.Vars["ansible_user"])
	core.AssertEqual(t, "prod", inv.All.Children["production"].Vars["env"])
	core.AssertEqual(t, 2222, inv.All.Children["production"].Hosts["prod1"].AnsiblePort)
}

func TestParser_ParseInventory_Good_HostVars(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")

	yaml := `---
all:
  hosts:
    web1:
      ansible_host: 192.168.1.10
  vars:
    env: prod
host_vars:
  web1:
    env: staging
    owner: ops
`
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	inv, err := p.ParseInventory(path)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, inv.HostVars)
	core.AssertEqual(t, "staging", inv.HostVars["web1"]["env"])

	vars := GetHostVars(inv, "web1")
	core.AssertEqual(t, "staging", vars["env"])
	core.AssertEqual(t, "ops", vars["owner"])
	core.AssertEqual(t, "192.168.1.10", vars["ansible_host"])
}

func TestParser_ParseInventory_Bad_InvalidYAML(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "bad.yml")

	core.RequireNoError(t, writeTestFile(path, []byte("{{{bad"), 0644))

	p := NewParser(dir)
	_, err := p.ParseInventory(path)

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "parse inventory")
}

func TestParser_ParseInventory_Bad_FileNotFound(t *core.T) {
	p := NewParser(t.TempDir())
	_, err := p.ParseInventory("/nonexistent/inventory.yml")

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "read inventory")
}

// --- ParseTasks ---

func TestParser_ParseTasks_Good_TaskFile(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "tasks.yml")

	yaml := `---
- name: First task
  shell: echo first
- name: Second task
  copy:
    src: /tmp/a
    dest: /tmp/b
- name: Async task
  shell: sleep 5
  async: 30
  poll: 0
`
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	tasks, err := p.ParseTasks(path)

	core.RequireNoError(t, err)
	core.AssertLen(t, tasks, 3)
	core.AssertEqual(t, "shell", tasks[0].Module)
	core.AssertEqual(t, "echo first", tasks[0].Args["_raw_params"])
	core.AssertEqual(t, "copy", tasks[1].Module)
	core.AssertEqual(t, "/tmp/a", tasks[1].Args["src"])
	core.AssertEqual(t, "shell", tasks[2].Module)
	core.AssertEqual(t, 30, tasks[2].Async)
	core.AssertEqual(t, 0, tasks[2].Poll)
}

func TestParser_ParseTasks_Bad_InvalidYAML(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "bad.yml")

	core.RequireNoError(t, writeTestFile(path, []byte("not: [valid: tasks"), 0644))

	p := NewParser(dir)
	_, err := p.ParseTasks(path)

	core.AssertError(t, err)
}

func TestParser_ParseTasksFromDir_Good_MainFallback(t *core.T) {
	dir := t.TempDir()
	taskDir := joinPath(dir, "tasks")
	path := joinPath(taskDir, "main.yml")

	core.RequireNoError(t, os.MkdirAll(taskDir, 0o755))
	core.RequireNoError(t, writeTestFile(path, []byte(`---
- name: From dir
  debug:
    msg: "ok"
`), 0o644))

	p := NewParser(dir)
	tasks, err := p.ParseTasksFromDir(taskDir)

	core.RequireNoError(t, err)
	core.AssertLen(t, tasks, 1)
	core.AssertEqual(t, "From dir", tasks[0].Name)
	core.AssertEqual(t, "debug", tasks[0].Module)
}

func TestParser_ParseVarsFiles_Good_GlobMerge(t *core.T) {
	dir := t.TempDir()
	varsDir := joinPath(dir, "vars")
	core.RequireNoError(t, os.MkdirAll(varsDir, 0o755))
	core.RequireNoError(t, writeTestFile(joinPath(varsDir, "01.yml"), []byte("a: 1\nb: one\n"), 0o644))
	core.RequireNoError(t, writeTestFile(joinPath(varsDir, "02.yml"), []byte("b: two\nc: 3\n"), 0o644))

	p := NewParser(dir)
	vars, err := p.ParseVarsFiles(joinPath(varsDir, "*.yml"))

	core.RequireNoError(t, err)
	core.AssertEqual(t, 1, vars["a"])
	core.AssertEqual(t, "two", vars["b"])
	core.AssertEqual(t, 3, vars["c"])
}

func TestParser_ParseVarsFiles_Bad_WildcardWithNonLocalMedium(t *core.T) {
	medium := coreio.NewMemoryMedium()
	core.RequireNoError(t, medium.EnsureDir("vars"))
	core.RequireNoError(t, medium.Write("vars/01.yml", "a: 1\n"))

	p := NewParser("")
	p.SetMedium(medium)
	_, err := p.ParseVarsFiles("vars/*.yml")

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "wildcard patterns require")
}

func TestParser_ParseRoles_Good_RoleDirectory(t *core.T) {
	dir := t.TempDir()
	roleDir := joinPath(dir, "roles", "web")
	core.RequireNoError(t, os.MkdirAll(joinPath(roleDir, "tasks"), 0o755))
	core.RequireNoError(t, os.MkdirAll(joinPath(roleDir, "defaults"), 0o755))
	core.RequireNoError(t, os.MkdirAll(joinPath(roleDir, "vars"), 0o755))
	core.RequireNoError(t, os.MkdirAll(joinPath(roleDir, "handlers"), 0o755))
	core.RequireNoError(t, writeTestFile(joinPath(roleDir, "tasks", "main.yml"), []byte(`---
- name: Role task
  debug:
    msg: "role"
`), 0o644))
	core.RequireNoError(t, writeTestFile(joinPath(roleDir, "defaults", "main.yml"), []byte("role_default: true\n"), 0o644))
	core.RequireNoError(t, writeTestFile(joinPath(roleDir, "vars", "main.yml"), []byte("role_var: 42\n"), 0o644))
	core.RequireNoError(t, writeTestFile(joinPath(roleDir, "handlers", "main.yml"), []byte(`---
- name: Role handler
  debug:
    msg: "handler"
`), 0o644))

	p := NewParser(dir)
	roles, err := p.ParseRoles("roles")

	core.RequireNoError(t, err)
	role, ok := roles["web"]
	core.RequireTrue(t, ok)
	core.AssertNotNil(t, role)
	core.AssertEqual(t, "web", role.Name)
	core.AssertLen(t, role.Tasks, 1)
	core.AssertEqual(t, "Role task", role.Tasks[0].Name)
	core.AssertEqual(t, true, role.Defaults["role_default"])
	core.AssertEqual(t, 42, role.Vars["role_var"])
	core.AssertLen(t, role.Handlers, 1)
	core.AssertEqual(t, "Role handler", role.Handlers[0].Name)
}

func TestParser_ParseRole_Good_LoadsRoleVarsIntoParserContext(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "web", "tasks", "main.yml"), []byte(`---
- name: Role task
  debug:
    msg: "{{ role_default }} {{ role_value }} {{ shared_value }}"
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "web", "defaults", "main.yml"), []byte(`---
role_default: default-value
shared_value: default-shared
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "web", "vars", "main.yml"), []byte(`---
role_value: vars-value
shared_value: role-shared
`), 0644))

	p := NewParser(dir)
	p.vars["existing_value"] = "keep-me"

	tasks, err := p.ParseRole("web", "main.yml")

	core.RequireNoError(t, err)
	core.AssertLen(t, tasks, 1)
	core.AssertEqual(t, "debug", tasks[0].Module)
	core.AssertEqual(t, "keep-me", p.vars["existing_value"])
	core.AssertEqual(t, "default-value", p.vars["role_default"])
	core.AssertEqual(t, "vars-value", p.vars["role_value"])
	core.AssertEqual(t, "role-shared", p.vars["shared_value"])
}

// --- GetHosts ---

func TestParser_GetHosts_Good_AllPattern(t *core.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
			},
		},
	}

	hosts := GetHosts(inv, "all")
	core.AssertLen(t, hosts, 2)
	core.AssertContains(t, hosts, "host1")
	core.AssertContains(t, hosts, "host2")
}

func TestParser_GetHosts_Good_LocalhostPattern(t *core.T) {
	inv := &Inventory{All: &InventoryGroup{}}
	hosts := GetHosts(inv, "localhost")
	core.AssertEqual(t, []string{"localhost"}, hosts)
}

func TestParser_GetHosts_Good_GroupPattern(t *core.T) {
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
	core.AssertLen(t, hosts, 2)
	core.AssertContains(t, hosts, "web1")
	core.AssertContains(t, hosts, "web2")
}

func TestParser_GetHosts_Good_SpecificHost(t *core.T) {
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
	core.AssertEqual(t, []string{"myhost"}, hosts)
}

func TestParser_GetHosts_Good_ColonUnionIntersectionExclusion(t *core.T) {
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

	core.AssertEqual(t, []string{"web1", "web2", "db1"}, GetHosts(inv, "web:db"))
	core.AssertEqual(t, []string{"web2"}, GetHosts(inv, "web:&db"))
	core.AssertEqual(t, []string{"web1"}, GetHosts(inv, "web:!canary"))
	core.AssertEqual(t, []string{"web1"}, GetHosts(inv, "web:db:!canary"))
}

func TestParser_GetHosts_Good_AllIncludesChildren(t *core.T) {
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
	core.AssertLen(t, hosts, 2)
	core.AssertContains(t, hosts, "top")
	core.AssertContains(t, hosts, "child1")
}

func TestParser_GetHosts_Bad_NoMatch(t *core.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{"host1": {}},
		},
	}

	hosts := GetHosts(inv, "nonexistent")
	core.AssertEmpty(t, hosts)
}

func TestParser_GetHosts_Bad_NilGroup(t *core.T) {
	inv := &Inventory{All: nil}
	hosts := GetHosts(inv, "all")
	core.AssertEmpty(t, hosts)
}

// --- GetHostVars ---

func TestParser_GetHostVars_Good_DirectHost(t *core.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Vars: map[string]any{"global_var": "global"},
			Hosts: map[string]*Host{
				"myhost": {
					AnsibleHost:           "10.0.0.1",
					AnsiblePort:           2222,
					AnsibleUser:           "deploy",
					AnsibleBecomePassword: "secret",
				},
			},
		},
	}

	vars := GetHostVars(inv, "myhost")
	core.AssertEqual(t, "10.0.0.1", vars["ansible_host"])
	core.AssertEqual(t, 2222, vars["ansible_port"])
	core.AssertEqual(t, "deploy", vars["ansible_user"])
	core.AssertEqual(t, "secret", vars["ansible_become_password"])
	core.AssertEqual(t, "global", vars["global_var"])
}

func TestParser_GetHostVars_Good_InheritedGroupVars(t *core.T) {
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
	core.AssertEqual(t, "10.0.0.1", vars["ansible_host"])
	core.AssertEqual(t, "prod", vars["env"])
}

func TestParser_GetHostVars_Good_HostNotFound(t *core.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{"other": {}},
		},
	}

	vars := GetHostVars(inv, "nonexistent")
	core.AssertEmpty(t, vars)
}

// --- isModule ---

func TestParser_IsModule_Good_KnownModules(t *core.T) {
	core.AssertTrue(t, isModule("shell"))
	core.AssertTrue(t, isModule("command"))
	core.AssertTrue(t, isModule("copy"))
	core.AssertTrue(t, isModule("file"))
	core.AssertTrue(t, isModule("apt"))
	core.AssertTrue(t, isModule("service"))
	core.AssertTrue(t, isModule("systemd"))
	core.AssertTrue(t, isModule("rpm"))
	core.AssertTrue(t, isModule("debug"))
	core.AssertTrue(t, isModule("set_fact"))
	core.AssertTrue(t, isModule("ping"))
}

func TestParser_IsModule_Good_FQCN(t *core.T) {
	core.AssertTrue(t, isModule("ansible.builtin.shell"))
	core.AssertTrue(t, isModule("ansible.builtin.copy"))
	core.AssertTrue(t, isModule("ansible.builtin.apt"))
	core.AssertTrue(t, isModule("ansible.builtin.rpm"))
}

func TestParser_IsModule_Good_DottedUnknown(t *core.T) {
	// Any key with dots is considered a module
	core.AssertTrue(t, isModule("community.general.ufw"))
	core.AssertTrue(t, isModule("ansible.posix.authorized_key"))
	core.AssertTrue(t, isModule("custom.namespace.module"))
}

func TestParser_IsModule_Bad_NotAModule(t *core.T) {
	core.AssertFalse(t, isModule("some_random_key"))
	core.AssertFalse(t, isModule("foobar"))
	core.AssertFalse(t, isModule("with-hyphen"))
}

// --- NormalizeModule ---

func TestParser_NormalizeModule_Good(t *core.T) {
	core.AssertEqual(t, "ansible.builtin.shell", NormalizeModule("shell"))
	core.AssertEqual(t, "ansible.builtin.copy", NormalizeModule("copy"))
	core.AssertEqual(t, "ansible.builtin.apt", NormalizeModule("apt"))
	core.AssertEqual(t, "ansible.builtin.rpm", NormalizeModule("rpm"))
	core.AssertEqual(t, "ansible.builtin.ping", NormalizeModule("ping"))
}

func TestParser_NormalizeModule_Good_CommunityAliases(t *core.T) {
	core.AssertEqual(t, "ansible.posix.authorized_key", NormalizeModule("authorized_key"))
	core.AssertEqual(t, "ansible.posix.authorized_key", NormalizeModule("ansible.builtin.authorized_key"))
	core.AssertEqual(t, "community.general.ufw", NormalizeModule("ufw"))
	core.AssertEqual(t, "community.general.ufw", NormalizeModule("ansible.builtin.ufw"))
	core.AssertEqual(t, "community.docker.docker_compose", NormalizeModule("docker_compose"))
	core.AssertEqual(t, "community.docker.docker_compose_v2", NormalizeModule("docker_compose_v2"))
	core.AssertEqual(t, "community.docker.docker_compose", NormalizeModule("ansible.builtin.docker_compose"))
	core.AssertEqual(t, "community.docker.docker_compose_v2", NormalizeModule("ansible.builtin.docker_compose_v2"))
}

func TestParser_NormalizeModule_Good_AlreadyFQCN(t *core.T) {
	core.AssertEqual(t, "ansible.builtin.shell", NormalizeModule("ansible.builtin.shell"))
	core.AssertEqual(t, "community.general.ufw", NormalizeModule("community.general.ufw"))
	core.AssertEqual(t, "custom.namespace.module", NormalizeModule("custom.namespace.module"))
}

func TestParser_IsModule_Good_AdditionalFQCN(t *core.T) {
	core.AssertTrue(t, isModule("ansible.builtin.hostname"))
	core.AssertTrue(t, isModule("ansible.builtin.sysctl"))
	core.AssertTrue(t, isModule("ansible.builtin.reboot"))
}

func TestParser_NormalizeModule_Good_LegacyNamespace(t *core.T) {
	core.AssertEqual(t, "ansible.builtin.command", NormalizeModule("ansible.legacy.command"))
	core.AssertEqual(t, "ansible.posix.authorized_key", NormalizeModule("ansible.legacy.authorized_key"))
	core.AssertEqual(t, "community.general.ufw", NormalizeModule("ansible.legacy.ufw"))
}

// --- NewParser ---

func TestParser_NewParser_Good(t *core.T) {
	p := NewParser("/some/path")
	core.AssertNotNil(t, p)
	core.AssertEqual(t, "/some/path", p.basePath)
	core.AssertNotNil(t, p.vars)
}
