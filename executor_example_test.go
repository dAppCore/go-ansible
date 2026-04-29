package ansible

import (
	"context"

	core "dappco.re/go"
	coreio "dappco.re/go/io"
)

func ExampleSSHClient_Run_environment() {
	mock := NewMockSSHClient()
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: "export APP_ENV=prod; "}
	_, _, code, err := client.Run(context.Background(), "echo $APP_ENV")
	core.Println(err == nil, code, mock.lastCommand().Cmd)
	// Output: true 0 export APP_ENV=prod; echo $APP_ENV
}

func ExampleSSHClient_RunScript_environment() {
	mock := NewMockSSHClient()
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: "export APP_ENV=prod\n"}
	_, _, code, err := client.RunScript(context.Background(), "echo $APP_ENV")
	core.Println(err == nil, code, mock.lastCommand().Cmd)
	// Output:
	// true 0 export APP_ENV=prod
	// echo $APP_ENV
}

func ExampleNewExecutor() {
	executor := NewExecutor("/tmp/playbooks")
	core.Println(executor != nil, executor.parser.basePath)
	// Output: true /tmp/playbooks
}

func ExampleExecutor_SetInventory() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "inventory.yml")
	exampleWrite(file, "all:\n  hosts:\n    web1: {}\n")
	executor := NewExecutor(dir)
	err := executor.SetInventory(file)
	_, ok := executor.inventory.All.Hosts["web1"]
	core.Println(err == nil, ok)
	// Output: true true
}

func ExampleExecutor_SetInventoryDirect() {
	executor := NewExecutor("/tmp")
	executor.SetInventoryDirect(testInventory())
	core.Println(executor.inventory != nil, len(executor.inventory.All.Hosts))
	// Output: true 1
}

func ExampleExecutor_SetVar() {
	executor := NewExecutor("/tmp")
	executor.SetVar("env", "prod")
	core.Println(executor.vars["env"])
	// Output: prod
}

func ExampleExecutor_SetMedium() {
	executor := NewExecutor("/tmp")
	executor.SetMedium(coreio.Local)
	core.Println(executor.parser.configuredMedium() != nil)
	// Output: true
}

func ExampleExecutor_Run() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "site.yml")
	exampleWrite(file, "- hosts: localhost\n  gather_facts: false\n  tasks: []\n")
	err := NewExecutor(dir).Run(context.Background(), file)
	core.Println(err == nil)
	// Output: true
}

func ExampleExecutor_Close() {
	executor := NewExecutor("/tmp")
	executor.clients["local"] = newLocalClient()
	executor.Close()
	core.Println(len(executor.clients))
	// Output: 0
}

func ExampleExecutor_TemplateFile() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "template.j2")
	exampleWrite(file, "hello {{ name }}")
	executor := NewExecutor(dir)
	executor.SetVar("name", "world")
	rendered, err := executor.TemplateFile(file, "", nil)
	core.Println(err == nil, rendered)
	// Output: true hello world
}
