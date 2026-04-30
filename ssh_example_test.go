package ansible

import (
	"context"

	core "dappco.re/go"
)

func ExampleNewSSHClient() {
	result := NewSSHClient(SSHConfig{Host: "web1"})
	client := result.Value.(*SSHClient)
	core.Println(result.OK, client.host, client.port, client.user)
	// Output: true web1 22 root
}

func ExampleSSHClient_Connect() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.Connect(context.Background())
	core.Println(!result.OK)
	// Output: true
}

func ExampleSSHClient_Close() {
	client := &SSHClient{}
	result := client.Close()
	core.Println(result.OK, client.client == nil)
	// Output: true true
}

func ExampleSSHClient_BecomeState() {
	client := &SSHClient{become: true, becomeUser: "root", becomePass: "secret"}
	become, user, password := client.BecomeState()
	core.Println(become, user, password != "")
	// Output: true root true
}

func ExampleSSHClient_Run_remote() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.Run(context.Background(), "hostname")
	core.Println(!result.OK)
	// Output: true
}

func ExampleSSHClient_RunScript_remote() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.RunScript(context.Background(), "echo ok")
	core.Println(!result.OK)
	// Output: true
}

func ExampleSSHClient_Upload() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.Upload(context.Background(), newReader("payload"), "/tmp/file.txt", 0o644)
	core.Println(!result.OK)
	// Output: true
}

func ExampleSSHClient_Download() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.Download(context.Background(), "/tmp/file.txt")
	core.Println(!result.OK, result.Value == nil)
	// Output: true false
}

func ExampleSSHClient_FileExists() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.FileExists(context.Background(), "/tmp/file.txt")
	core.Println(!result.OK)
	// Output: true
}

func ExampleSSHClient_Stat() {
	client := NewSSHClient(SSHConfig{Host: "web1"}).Value.(*SSHClient)
	result := client.Stat(context.Background(), "/tmp/file.txt")
	core.Println(!result.OK, result.Value == nil)
	// Output: true false
}

func ExampleSSHClient_SetBecome() {
	client := &SSHClient{}
	client.SetBecome(true, "admin", "secret")
	become, user, _ := client.BecomeState()
	core.Println(become, user)
	// Output: true admin
}
