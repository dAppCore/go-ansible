package ansible

import (
	"iter"

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
	result := NewParser(dir).ParsePlaybook(file)
	plays := result.Value.([]Play)
	core.Println(result.OK, len(plays), plays[0].Hosts)
	// Output: true 1 all
}

func ExampleParser_ParsePlaybookIter() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "site.yml")
	exampleWrite(file, "- hosts: web\n  tasks: []\n")
	result := NewParser(dir).ParsePlaybookIter(file)
	seq := result.Value.(iter.Seq[Play])
	count := 0
	for range seq {
		count++
	}
	core.Println(result.OK, count)
	// Output: true 1
}

func ExampleParser_ParseInventory() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "inventory.yml")
	exampleWrite(file, "all:\n  hosts:\n    web1: {}\n")
	result := NewParser(dir).ParseInventory(file)
	inv := result.Value.(*Inventory)
	_, ok := inv.All.Hosts["web1"]
	core.Println(result.OK, ok)
	// Output: true true
}

func ExampleParser_ParseTasks() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "tasks.yml")
	exampleWrite(file, "- shell: echo ok\n")
	result := NewParser(dir).ParseTasks(file)
	tasks := result.Value.([]Task)
	core.Println(result.OK, len(tasks), tasks[0].Module)
	// Output: true 1 shell
}

func ExampleParser_ParseTasksFromDir() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	tasksDir := joinPath(dir, "tasks")
	exampleWrite(joinPath(tasksDir, "main.yml"), "- debug:\n    msg: ok\n")
	result := NewParser(dir).ParseTasksFromDir(tasksDir)
	tasks := result.Value.([]Task)
	core.Println(result.OK, len(tasks), tasks[0].Module)
	// Output: true 1 debug
}

func ExampleParser_ParseVarsFiles() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "vars.yml")
	exampleWrite(file, "answer: 42\n")
	result := NewParser(dir).ParseVarsFiles(file)
	vars := result.Value.(map[string]any)
	core.Println(result.OK, vars["answer"])
	// Output: true 42
}

func ExampleParser_ParseRoles() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	exampleWrite(joinPath(dir, "roles", "web", "tasks", "main.yml"), "- debug:\n    msg: ok\n")
	result := NewParser(dir).ParseRoles(joinPath(dir, "roles"))
	roles := result.Value.(map[string]*Role)
	_, ok := roles["web"]
	core.Println(result.OK, ok)
	// Output: true true
}

func ExampleParser_ParseTasksIter() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "tasks.yml")
	exampleWrite(file, "- shell: echo one\n- shell: echo two\n")
	result := NewParser(dir).ParseTasksIter(file)
	seq := result.Value.(iter.Seq[Task])
	count := 0
	for range seq {
		count++
	}
	core.Println(result.OK, count)
	// Output: true 2
}

func ExampleParser_ParseRole() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	exampleWrite(joinPath(dir, "roles", "web", "tasks", "alt.yml"), "- debug:\n    msg: alt\n")
	result := NewParser(dir).ParseRole("web", "alt.yml")
	tasks := result.Value.([]Task)
	core.Println(result.OK, len(tasks), tasks[0].Module)
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
