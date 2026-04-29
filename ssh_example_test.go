package ansible

import (
	"context"

	core "dappco.re/go"
)

func ExampleNewSSHClient() {
	client, err := NewSSHClient(SSHConfig{Host: "web1"})
	core.Println(err == nil, client.host, client.port, client.user)
	// Output: true web1 22 root
}

func ExampleSSHClient_Connect() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	err := client.Connect(context.Background())
	core.Println(err != nil)
	// Output: true
}

func ExampleSSHClient_Close() {
	client := &SSHClient{}
	err := client.Close()
	core.Println(err == nil, client.client == nil)
	// Output: true true
}

func ExampleSSHClient_BecomeState() {
	client := &SSHClient{become: true, becomeUser: "root", becomePass: "secret"}
	become, user, password := client.BecomeState()
	core.Println(become, user, password != "")
	// Output: true root true
}

func ExampleSSHClient_Run_remote() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	_, _, code, err := client.Run(context.Background(), "hostname")
	core.Println(err != nil, code)
	// Output: true -1
}

func ExampleSSHClient_RunScript_remote() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	_, _, code, err := client.RunScript(context.Background(), "echo ok")
	core.Println(err != nil, code)
	// Output: true -1
}

func ExampleSSHClient_Upload() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	err := client.Upload(context.Background(), newReader("payload"), "/tmp/file.txt", 0o644)
	core.Println(err != nil)
	// Output: true
}

func ExampleSSHClient_Download() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	data, err := client.Download(context.Background(), "/tmp/file.txt")
	core.Println(err != nil, data == nil)
	// Output: true true
}

func ExampleSSHClient_FileExists() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	exists, err := client.FileExists(context.Background(), "/tmp/file.txt")
	core.Println(err != nil, exists)
	// Output: true false
}

func ExampleSSHClient_Stat() {
	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
	info, err := client.Stat(context.Background(), "/tmp/file.txt")
	core.Println(err != nil, info == nil)
	// Output: true true
}

func ExampleSSHClient_SetBecome() {
	client := &SSHClient{}
	client.SetBecome(true, "admin", "secret")
	become, user, _ := client.BecomeState()
	core.Println(become, user)
	// Output: true admin
}
