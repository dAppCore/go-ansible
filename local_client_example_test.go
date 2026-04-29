package ansible

import (
	"context"

	core "dappco.re/go"
)

type Client = localClient

func ExampleClient_BecomeState() {
	client := newLocalClient()
	client.SetBecome(true, "root", "secret")
	become, user, password := client.BecomeState()
	core.Println(become, user, password != "")
	// Output: true root true
}

func ExampleClient_SetBecome() {
	client := newLocalClient()
	client.SetBecome(true, "deploy", "secret")
	_, user, _ := client.BecomeState()
	core.Println(user)
	// Output: deploy
}

func ExampleClient_Close() {
	client := newLocalClient()
	err := client.Close()
	core.Println(err == nil)
	// Output: true
}

func ExampleClient_Run() {
	client := newLocalClient()
	stdout, _, code, err := client.Run(context.Background(), "printf local")
	core.Println(err == nil, code, stdout)
	// Output: true 0 local
}

func ExampleClient_RunScript() {
	client := newLocalClient()
	stdout, _, code, err := client.RunScript(context.Background(), "printf script")
	core.Println(err == nil, code, stdout)
	// Output: true 0 script
}

func ExampleClient_Upload() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	client := newLocalClient()
	err := client.Upload(context.Background(), newReader("payload"), file, 0o644)
	core.Println(err == nil)
	// Output: true
}

func ExampleClient_Download() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	exampleWrite(file, "payload")
	client := newLocalClient()
	data, err := client.Download(context.Background(), file)
	core.Println(err == nil, string(data))
	// Output: true payload
}

func ExampleClient_FileExists() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	exampleWrite(file, "payload")
	client := newLocalClient()
	exists, err := client.FileExists(context.Background(), file)
	core.Println(err == nil, exists)
	// Output: true true
}

func ExampleClient_Stat() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	exampleWrite(file, "payload")
	client := newLocalClient()
	info, err := client.Stat(context.Background(), file)
	core.Println(err == nil, info["exists"], info["isdir"])
	// Output: true true false
}
