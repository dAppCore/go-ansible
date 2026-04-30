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
	result := client.Close()
	core.Println(result.OK)
	// Output: true
}

func ExampleClient_Run() {
	client := newLocalClient()
	result := client.Run(context.Background(), "printf local")
	out := commandRunValue(result)
	core.Println(result.OK, out.ExitCode, out.Stdout)
	// Output: true 0 local
}

func ExampleClient_RunScript() {
	client := newLocalClient()
	result := client.RunScript(context.Background(), "printf script")
	out := commandRunValue(result)
	core.Println(result.OK, out.ExitCode, out.Stdout)
	// Output: true 0 script
}

func ExampleClient_Upload() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	client := newLocalClient()
	result := client.Upload(context.Background(), newReader("payload"), file, 0o644)
	core.Println(result.OK)
	// Output: true
}

func ExampleClient_Download() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	exampleWrite(file, "payload")
	client := newLocalClient()
	result := client.Download(context.Background(), file)
	core.Println(result.OK, string(result.Value.([]byte)))
	// Output: true payload
}

func ExampleClient_FileExists() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	exampleWrite(file, "payload")
	client := newLocalClient()
	result := client.FileExists(context.Background(), file)
	core.Println(result.OK, result.Value)
	// Output: true true
}

func ExampleClient_Stat() {
	dir := exampleDir()
	defer core.RemoveAll(dir)
	file := joinPath(dir, "remote.txt")
	exampleWrite(file, "payload")
	client := newLocalClient()
	result := client.Stat(context.Background(), file)
	info := result.Value.(map[string]any)
	core.Println(result.OK, info["exists"], info["isdir"])
	// Output: true true false
}
