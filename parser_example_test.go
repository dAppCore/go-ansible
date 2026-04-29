package ansible

import (
	core "dappco.re/go"
	coreio "dappco.re/go/io"
	"gopkg.in/yaml.v3"
)

func exampleDir() string {
	r := core.MkdirTemp("", "ansible-example-*")
	if !r.OK {
		return core.TempDir()
	}
	return r.Value.(string)
}

func exampleWrite(path, data string) {
	_ = core.MkdirAll(pathDir(path), 0o755)
	_ = core.WriteFile(path, []byte(data), 0o644)
}

func ExampleNewParser() {
	parser := NewParser("/tmp/playbooks")
	core.Println(parser != nil, parser.basePath)
	// Output: true /tmp/playbooks
}

func ExampleParser_SetMedium() {
	parser := NewParser("/tmp/playbooks")
	parser.SetMedium(coreio.Local)
	core.Println(parser.configuredMedium() != nil)
	// Output: true
}

func ExampleParser_ParsePlaybook() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "site.yml")
	exampleWrite(file, "- hosts: all\n  tasks: []\n")
	plays, err := NewParser(dir).ParsePlaybook(file)
	core.Println(err == nil, len(plays), plays[0].Hosts)
	// Output: true 1 all
}

func ExampleParser_ParsePlaybookIter() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "site.yml")
	exampleWrite(file, "- hosts: web\n  tasks: []\n")
	seq, err := NewParser(dir).ParsePlaybookIter(file)
	count := 0
	for range seq {
		count++
	}
	core.Println(err == nil, count)
	// Output: true 1
}

func ExampleParser_ParseInventory() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "inventory.yml")
	exampleWrite(file, "all:\n  hosts:\n    web1: {}\n")
	inv, err := NewParser(dir).ParseInventory(file)
	_, ok := inv.All.Hosts["web1"]
	core.Println(err == nil, ok)
	// Output: true true
}

func ExampleParser_ParseTasks() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "tasks.yml")
	exampleWrite(file, "- shell: echo ok\n")
	tasks, err := NewParser(dir).ParseTasks(file)
	core.Println(err == nil, len(tasks), tasks[0].Module)
	// Output: true 1 shell
}

func ExampleParser_ParseTasksFromDir() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	tasksDir := joinPath(dir, "tasks")
	exampleWrite(joinPath(tasksDir, "main.yml"), "- debug:\n    msg: ok\n")
	tasks, err := NewParser(dir).ParseTasksFromDir(tasksDir)
	core.Println(err == nil, len(tasks), tasks[0].Module)
	// Output: true 1 debug
}

func ExampleParser_ParseVarsFiles() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "vars.yml")
	exampleWrite(file, "answer: 42\n")
	vars, err := NewParser(dir).ParseVarsFiles(file)
	core.Println(err == nil, vars["answer"])
	// Output: true 42
}

func ExampleParser_ParseRoles() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	exampleWrite(joinPath(dir, "roles", "web", "tasks", "main.yml"), "- debug:\n    msg: ok\n")
	roles, err := NewParser(dir).ParseRoles(joinPath(dir, "roles"))
	_, ok := roles["web"]
	core.Println(err == nil, ok)
	// Output: true true
}

func ExampleParser_ParseTasksIter() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "tasks.yml")
	exampleWrite(file, "- shell: echo one\n- shell: echo two\n")
	seq, err := NewParser(dir).ParseTasksIter(file)
	count := 0
	for range seq {
		count++
	}
	core.Println(err == nil, count)
	// Output: true 2
}

func ExampleParser_ParseRole() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	exampleWrite(joinPath(dir, "roles", "web", "tasks", "alt.yml"), "- debug:\n    msg: alt\n")
	tasks, err := NewParser(dir).ParseRole("web", "alt.yml")
	core.Println(err == nil, len(tasks), tasks[0].Module)
	// Output: true 1 debug
}

func ExampleTask_UnmarshalYAML() {
	var task Task
	err := yaml.Unmarshal([]byte("name: hello\nshell: echo hi\n"), &task)
	core.Println(err == nil, task.Name, task.Module)
	// Output: true hello shell
}

func ExampleNormalizeModule() {
	core.Println(NormalizeModule("shell"))
	// Output: ansible.builtin.shell
}

func ExampleGetHosts() {
	hosts := GetHosts(testInventory(), "db")
	core.Println(len(hosts), hosts[0])
	// Output: 1 db1
}

func ExampleGetHostsIter() {
	count := 0
	for range GetHostsIter(testInventory(), "all") {
		count++
	}
	core.Println(count)
	// Output: 2
}

func ExampleAllHostsIter() {
	count := 0
	for range AllHostsIter(testInventory().All) {
		count++
	}
	core.Println(count)
	// Output: 2
}

func ExampleGetHostVars() {
	vars := GetHostVars(testInventory(), "db1")
	core.Println(vars["env"], vars["tier"], vars["role"])
	// Output: test database primary
}
