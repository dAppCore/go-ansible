package ansible

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	core "dappco.re/go"
	coreio "dappco.re/go/io"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"
)

func ax7WriteFile(t *core.T, path string, data string) {
	t.Helper()
	core.RequireNoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	core.RequireNoError(t, os.WriteFile(path, []byte(data), 0o644))
}

func ax7Inventory() *Inventory {
	return &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {AnsibleHost: "10.0.0.1"},
			},
			Children: map[string]*InventoryGroup{
				"db": {
					Hosts: map[string]*Host{
						"db1": {AnsibleHost: "10.0.0.2"},
					},
					Vars: map[string]any{"tier": "database"},
				},
			},
			Vars: map[string]any{"env": "test"},
		},
		HostVars: map[string]map[string]any{
			"db1": {"role": "primary"},
		},
	}
}

func ax7ConnectedSSHClient(t *core.T, responses map[string]string) *SSHClient {
	t.Helper()
	serverConfig := &ssh.ServerConfig{NoClientAuth: true}
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	core.RequireNoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	core.RequireNoError(t, err)
	serverConfig.AddHostKey(signer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	core.RequireNoError(t, err)
	go func() {
		defer listener.Close()
		serverConn, err := listener.Accept()
		if err != nil {
			return
		}
		conn, chans, reqs, err := ssh.NewServerConn(serverConn, serverConfig)
		if err != nil {
			return
		}
		defer conn.Close()
		go ssh.DiscardRequests(reqs)
		for next := range chans {
			if next.ChannelType() != "session" {
				_ = next.Reject(ssh.UnknownChannelType, "session required")
				continue
			}
			channel, requests, err := next.Accept()
			if err != nil {
				continue
			}
			go func() {
				defer channel.Close()
				for req := range requests {
					if req.Type != "exec" {
						_ = req.Reply(false, nil)
						continue
					}
					var payload struct{ Command string }
					ssh.Unmarshal(req.Payload, &payload)
					if req.WantReply {
						_ = req.Reply(true, nil)
					}
					out := responses[payload.Command]
					if out == "" {
						for key, value := range responses {
							if strings.Contains(payload.Command, key) {
								out = value
								break
							}
						}
					}
					_, _ = io.WriteString(channel, out)
					_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
					return
				}
			}()
		}
	}()
	clientConn, err := net.Dial("tcp", listener.Addr().String())
	core.RequireNoError(t, err)

	clientConfig := &ssh.ClientConfig{
		User:            "tester",
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         time.Second,
	}
	conn, chans, reqs, err := ssh.NewClientConn(clientConn, "pipe", clientConfig)
	core.RequireNoError(t, err)
	return &SSHClient{client: ssh.NewClient(conn, chans, reqs), host: "pipe", port: 22, user: "tester"}
}

func TestAX7_Client_BecomeState_Good(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "deploy", "secret")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "deploy", user)
	core.AssertEqual(t, "secret", password)
}

func TestAX7_Client_BecomeState_Bad(t *core.T) {
	client := newLocalClient()
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAX7_Client_BecomeState_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "", "")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAX7_Client_SetBecome_Good(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "root", user)
	core.AssertEqual(t, "pw", password)
}

func TestAX7_Client_SetBecome_Bad(t *core.T) {
	client := newLocalClient()
	client.SetBecome(false, "root", "pw")
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAX7_Client_SetBecome_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	client.SetBecome(true, "", "")
	_, user, password := client.BecomeState()
	core.AssertEqual(t, "root", user)
	core.AssertEqual(t, "pw", password)
}

func TestAX7_Client_Close_Good(t *core.T) {
	client := newLocalClient()
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client)
}

func TestAX7_Client_Close_Bad(t *core.T) {
	client := newLocalClient()
	first := client.Close()
	second := client.Close()
	core.AssertNoError(t, first)
	core.AssertNoError(t, second)
}

func TestAX7_Client_Close_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client)
}

func TestAX7_Client_Run_Good(t *core.T) {
	client := newLocalClient()
	stdout, stderr, code, err := client.Run(context.Background(), "printf local")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "local", stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestAX7_Client_Run_Bad(t *core.T) {
	client := newLocalClient()
	stdout, _, code, err := client.Run(context.Background(), "exit 7")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEqual(t, 7, code)
}

func TestAX7_Client_Run_Ugly(t *core.T) {
	client := newLocalClient()
	stdout, stderr, code, err := client.Run(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestAX7_Client_RunScript_Good(t *core.T) {
	client := newLocalClient()
	stdout, _, code, err := client.RunScript(context.Background(), "printf script")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "script", stdout)
	core.AssertEqual(t, 0, code)
}

func TestAX7_Client_RunScript_Bad(t *core.T) {
	client := newLocalClient()
	_, _, code, err := client.RunScript(context.Background(), "exit 9")
	core.AssertNoError(t, err)
	core.AssertEqual(t, 9, code)
}

func TestAX7_Client_RunScript_Ugly(t *core.T) {
	client := newLocalClient()
	stdout, stderr, code, err := client.RunScript(context.Background(), "\n")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestAX7_Client_Upload_Good(t *core.T) {
	client := newLocalClient()
	path := filepath.Join(t.TempDir(), "remote.txt")
	err := client.Upload(context.Background(), strings.NewReader("payload"), path, 0o644)
	core.AssertNoError(t, err)
	content, readErr := os.ReadFile(path)
	core.AssertNoError(t, readErr)
	core.AssertEqual(t, "payload", string(content))
}

func TestAX7_Client_Upload_Bad(t *core.T) {
	client := newLocalClient()
	err := client.Upload(context.Background(), strings.NewReader("payload"), "", 0o644)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "write remote file")
}

func TestAX7_Client_Upload_Ugly(t *core.T) {
	client := newLocalClient()
	path := filepath.Join(t.TempDir(), "nested", "remote.txt")
	err := client.Upload(context.Background(), strings.NewReader(""), path, 0)
	core.AssertNoError(t, err)
	info, statErr := os.Stat(path)
	core.AssertNoError(t, statErr)
	core.AssertEqual(t, os.FileMode(0), info.Mode().Perm())
}

func TestAX7_Client_Download_Good(t *core.T) {
	client := newLocalClient()
	path := filepath.Join(t.TempDir(), "remote.txt")
	ax7WriteFile(t, path, "download")
	data, err := client.Download(context.Background(), path)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "download", string(data))
}

func TestAX7_Client_Download_Bad(t *core.T) {
	client := newLocalClient()
	data, err := client.Download(context.Background(), filepath.Join(t.TempDir(), "missing.txt"))
	core.AssertError(t, err)
	core.AssertNil(t, data)
}

func TestAX7_Client_Download_Ugly(t *core.T) {
	client := newLocalClient()
	data, err := client.Download(context.Background(), "")
	core.AssertError(t, err)
	core.AssertNil(t, data)
}

func TestAX7_Client_FileExists_Good(t *core.T) {
	client := newLocalClient()
	path := filepath.Join(t.TempDir(), "exists.txt")
	ax7WriteFile(t, path, "x")
	exists, err := client.FileExists(context.Background(), path)
	core.AssertNoError(t, err)
	core.AssertTrue(t, exists)
}

func TestAX7_Client_FileExists_Bad(t *core.T) {
	client := newLocalClient()
	exists, err := client.FileExists(context.Background(), filepath.Join(t.TempDir(), "missing.txt"))
	core.AssertNoError(t, err)
	core.AssertFalse(t, exists)
}

func TestAX7_Client_FileExists_Ugly(t *core.T) {
	client := newLocalClient()
	exists, err := client.FileExists(context.Background(), string([]byte{'b', 0, 'd'}))
	core.AssertError(t, err)
	core.AssertFalse(t, exists)
}

func TestAX7_Client_Stat_Good(t *core.T) {
	client := newLocalClient()
	path := filepath.Join(t.TempDir(), "exists.txt")
	ax7WriteFile(t, path, "x")
	info, err := client.Stat(context.Background(), path)
	core.AssertNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])
}

func TestAX7_Client_Stat_Bad(t *core.T) {
	client := newLocalClient()
	info, err := client.Stat(context.Background(), filepath.Join(t.TempDir(), "missing.txt"))
	core.AssertNoError(t, err)
	core.AssertEqual(t, false, info["exists"])
}

func TestAX7_Client_Stat_Ugly(t *core.T) {
	client := newLocalClient()
	dir := t.TempDir()
	info, err := client.Stat(context.Background(), dir)
	core.AssertNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, true, info["isdir"])
}

func TestAX7_NewSSHClient_Good(t *core.T) {
	client, err := NewSSHClient(SSHConfig{Host: "web1", Port: 2200, User: "deploy"})
	core.AssertNoError(t, err)
	core.AssertEqual(t, "web1", client.host)
	core.AssertEqual(t, 2200, client.port)
	core.AssertEqual(t, "deploy", client.user)
}

func TestAX7_NewSSHClient_Bad(t *core.T) {
	client, err := NewSSHClient(SSHConfig{Host: "web1"})
	core.AssertNoError(t, err)
	core.AssertEqual(t, 22, client.port)
	core.AssertEqual(t, "root", client.user)
}

func TestAX7_NewSSHClient_Ugly(t *core.T) {
	client, err := NewSSHClient(SSHConfig{Host: "", Timeout: time.Nanosecond})
	core.AssertNoError(t, err)
	core.AssertEqual(t, "", client.host)
	core.AssertEqual(t, time.Nanosecond, client.timeout)
}

func TestAX7_SSHClient_SetBecome_Good(t *core.T) {
	client := &SSHClient{}
	client.SetBecome(true, "admin", "pw")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "pw", password)
}

func TestAX7_SSHClient_SetBecome_Bad(t *core.T) {
	client := &SSHClient{}
	client.SetBecome(false, "admin", "pw")
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAX7_SSHClient_SetBecome_Ugly(t *core.T) {
	client := &SSHClient{}
	client.SetBecome(true, "admin", "pw")
	client.SetBecome(true, "", "")
	_, user, password := client.BecomeState()
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "pw", password)
}

func TestAX7_SSHClient_BecomeState_Good(t *core.T) {
	client := &SSHClient{become: true, becomeUser: "root", becomePass: "pw"}
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "root", user)
	core.AssertEqual(t, "pw", password)
}

func TestAX7_SSHClient_BecomeState_Bad(t *core.T) {
	client := &SSHClient{}
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAX7_SSHClient_BecomeState_Ugly(t *core.T) {
	client := &SSHClient{become: true}
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAX7_SSHClient_Close_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, nil)
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNil(t, client.client)
}

func TestAX7_SSHClient_Close_Bad(t *core.T) {
	client := &SSHClient{}
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNil(t, client.client)
}

func TestAX7_SSHClient_Close_Ugly(t *core.T) {
	client := ax7ConnectedSSHClient(t, nil)
	first := client.Close()
	second := client.Close()
	core.AssertNoError(t, first)
	core.AssertNoError(t, second)
}

func TestAX7_SSHClient_Connect_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, nil)
	err := client.Connect(context.Background())
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client.client)
}

func TestAX7_SSHClient_Connect_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	err := client.Connect(context.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no authentication method")
}

func TestAX7_SSHClient_Connect_Ugly(t *core.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root", password: "pw"}
	err := client.Connect(ctx)
	core.AssertError(t, err)
}

func TestAX7_SSHClient_Run_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"hostname": "web1\n"})
	stdout, stderr, code, err := client.Run(context.Background(), "hostname")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "web1\n", stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestAX7_SSHClient_Run_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	stdout, _, code, err := client.Run(context.Background(), "hostname")
	core.AssertError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEqual(t, -1, code)
}

func TestAX7_SSHClient_Run_Ugly(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"": ""})
	stdout, stderr, code, err := client.Run(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestAX7_SSHClient_RunScript_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"ANSIBLE_SCRIPT_EOF": "script\n"})
	stdout, _, code, err := client.RunScript(context.Background(), "echo script")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "script\n", stdout)
	core.AssertEqual(t, 0, code)
}

func TestAX7_SSHClient_RunScript_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	_, _, code, err := client.RunScript(context.Background(), "echo script")
	core.AssertError(t, err)
	core.AssertEqual(t, -1, code)
}

func TestAX7_SSHClient_RunScript_Ugly(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"ANSIBLE_SCRIPT_EOF": ""})
	stdout, stderr, code, err := client.RunScript(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestAX7_SSHClient_Upload_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"mkdir -p": ""})
	err := client.Upload(context.Background(), strings.NewReader("payload"), "/tmp/file.txt", 0o644)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client.client)
}

func TestAX7_SSHClient_Upload_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	err := client.Upload(context.Background(), strings.NewReader("payload"), "/tmp/file.txt", 0o644)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no authentication method")
}

func TestAX7_SSHClient_Upload_Ugly(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"mkdir -p": ""})
	err := client.Upload(context.Background(), strings.NewReader(""), "/tmp/empty.txt", 0)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client.client)
}

func TestAX7_SSHClient_Download_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"cat": "downloaded"})
	data, err := client.Download(context.Background(), "/tmp/file.txt")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "downloaded", string(data))
}

func TestAX7_SSHClient_Download_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	data, err := client.Download(context.Background(), "/tmp/file.txt")
	core.AssertError(t, err)
	core.AssertNil(t, data)
}

func TestAX7_SSHClient_Download_Ugly(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"cat": ""})
	data, err := client.Download(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, data)
}

func TestAX7_SSHClient_FileExists_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"test -e": "yes\n"})
	exists, err := client.FileExists(context.Background(), "/tmp/file.txt")
	core.AssertNoError(t, err)
	core.AssertTrue(t, exists)
}

func TestAX7_SSHClient_FileExists_Bad(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"test -e": "no\n"})
	exists, err := client.FileExists(context.Background(), "/tmp/missing.txt")
	core.AssertNoError(t, err)
	core.AssertFalse(t, exists)
}

func TestAX7_SSHClient_FileExists_Ugly(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	exists, err := client.FileExists(context.Background(), "/tmp/file.txt")
	core.AssertError(t, err)
	core.AssertFalse(t, exists)
}

func TestAX7_SSHClient_Stat_Good(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"if [ -e": "exists=true isdir=false\n"})
	info, err := client.Stat(context.Background(), "/tmp/file.txt")
	core.AssertNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])
}

func TestAX7_SSHClient_Stat_Bad(t *core.T) {
	client := ax7ConnectedSSHClient(t, map[string]string{"if [ -e": "exists=false\n"})
	info, err := client.Stat(context.Background(), "/tmp/missing.txt")
	core.AssertNoError(t, err)
	core.AssertEqual(t, false, info["exists"])
}

func TestAX7_SSHClient_Stat_Ugly(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	info, err := client.Stat(context.Background(), "/tmp/file.txt")
	core.AssertError(t, err)
	core.AssertNil(t, info)
}

func TestAX7_NewExecutor_Bad(t *core.T) {
	executor := NewExecutor("")
	core.AssertNotNil(t, executor)
	core.AssertNotNil(t, executor.parser)
	core.AssertEmpty(t, executor.parser.basePath)
}

func TestAX7_NewExecutor_Ugly(t *core.T) {
	executor := NewExecutor("relative/base")
	core.AssertNotNil(t, executor)
	core.AssertEqual(t, "relative/base", executor.parser.basePath)
	core.AssertNotNil(t, executor.clients)
}

func TestAX7_Executor_SetVar_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetVar("answer", 42)
	core.AssertEqual(t, 42, executor.vars["answer"])
	core.AssertLen(t, executor.vars, 1)
}

func TestAX7_Executor_SetVar_Bad(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetVar("", "empty")
	core.AssertEqual(t, "empty", executor.vars[""])
	core.AssertTrue(t, executor.vars != nil)
}

func TestAX7_Executor_SetVar_Ugly(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetVar("nil", nil)
	core.AssertContains(t, executor.vars, "nil")
	core.AssertNil(t, executor.vars["nil"])
}

func TestAX7_Executor_SetInventoryDirect_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	inv := ax7Inventory()
	executor.SetInventoryDirect(inv)
	core.AssertEqual(t, inv, executor.inventory)
	core.AssertEmpty(t, executor.inventoryPath)
}

func TestAX7_Executor_SetInventoryDirect_Bad(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetInventoryDirect(nil)
	core.AssertNil(t, executor.inventory)
	core.AssertEmpty(t, executor.inventoryPath)
}

func TestAX7_Executor_SetInventoryDirect_Ugly(t *core.T) {
	executor := NewExecutor("/tmp")
	first := ax7Inventory()
	second := &Inventory{All: &InventoryGroup{}}
	executor.SetInventoryDirect(first)
	executor.SetInventoryDirect(second)
	core.AssertEqual(t, second, executor.inventory)
}

func TestAX7_Executor_SetInventory_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inventory.yml")
	ax7WriteFile(t, path, "all:\n  hosts:\n    web1: {}\n")
	executor := NewExecutor(dir)
	err := executor.SetInventory(path)
	core.AssertNoError(t, err)
	core.AssertContains(t, executor.inventory.All.Hosts, "web1")
}

func TestAX7_Executor_SetInventory_Bad(t *core.T) {
	executor := NewExecutor(t.TempDir())
	err := executor.SetInventory("missing.yml")
	core.AssertError(t, err)
	core.AssertNil(t, executor.inventory)
}

func TestAX7_Executor_SetInventory_Ugly(t *core.T) {
	dir := t.TempDir()
	ax7WriteFile(t, filepath.Join(dir, "hosts.yml"), "all:\n  hosts:\n    edge1: {}\n")
	executor := NewExecutor(dir)
	err := executor.SetInventory(dir)
	core.AssertNoError(t, err)
	core.AssertContains(t, executor.inventory.All.Hosts, "edge1")
}

func TestAX7_Executor_SetMedium_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetMedium(coreio.Local)
	core.AssertNotNil(t, executor.parser.configuredMedium())
	core.AssertEqual(t, coreio.Local, executor.parser.configuredMedium())
}

func TestAX7_Executor_SetMedium_Bad(t *core.T) {
	var executor *Executor
	core.AssertNotPanics(t, func() { executor.SetMedium(coreio.Local) })
	core.AssertNil(t, executor)
}

func TestAX7_Executor_SetMedium_Ugly(t *core.T) {
	executor := &Executor{}
	core.AssertNotPanics(t, func() { executor.SetMedium(coreio.Local) })
	core.AssertNil(t, executor.parser)
}

func TestAX7_Executor_Run_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "site.yml")
	ax7WriteFile(t, path, "- hosts: localhost\n  gather_facts: false\n  tasks: []\n")
	executor := NewExecutor(dir)
	err := executor.Run(context.Background(), path)
	core.AssertNoError(t, err)
}

func TestAX7_Executor_Run_Bad(t *core.T) {
	executor := NewExecutor(t.TempDir())
	err := executor.Run(context.Background(), "missing.yml")
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "parse playbook")
}

func TestAX7_Executor_Run_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "site.yml")
	ax7WriteFile(t, path, "[]\n")
	executor := NewExecutor(dir)
	err := executor.Run(context.Background(), path)
	core.AssertNoError(t, err)
}

func TestAX7_Executor_Close_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.clients["local"] = newLocalClient()
	executor.Close()
	core.AssertNotNil(t, executor.clients)
	core.AssertLen(t, executor.clients, 0)
}

func TestAX7_Executor_Close_Bad(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.Close()
	core.AssertNotNil(t, executor.clients)
	core.AssertLen(t, executor.clients, 0)
}

func TestAX7_Executor_Close_Ugly(t *core.T) {
	executor := &Executor{clients: nil}
	core.AssertNotPanics(t, executor.Close)
	core.AssertNotNil(t, executor.clients)
	core.AssertLen(t, executor.clients, 0)
}

func TestAX7_Executor_TemplateFile_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "template.j2")
	ax7WriteFile(t, path, "hello {{ name }}")
	executor := NewExecutor(dir)
	executor.SetVar("name", "world")
	content, err := executor.TemplateFile(path, "", nil)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "hello world", content)
}

func TestAX7_Executor_TemplateFile_Bad(t *core.T) {
	executor := NewExecutor(t.TempDir())
	content, err := executor.TemplateFile("", "", nil)
	core.AssertError(t, err)
	core.AssertEmpty(t, content)
}

func TestAX7_Executor_TemplateFile_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "template.j2")
	ax7WriteFile(t, path, "hello {{ missing }}")
	executor := NewExecutor(dir)
	content, err := executor.TemplateFile(path, "", nil)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "hello {{ missing }}", content)
}

func TestAX7_NewParser_Bad(t *core.T) {
	parser := NewParser("")
	core.AssertNotNil(t, parser)
	core.AssertEmpty(t, parser.basePath)
	core.AssertNotNil(t, parser.vars)
}

func TestAX7_NewParser_Ugly(t *core.T) {
	parser := NewParser("relative/base")
	core.AssertNotNil(t, parser)
	core.AssertEqual(t, "relative/base", parser.basePath)
	core.AssertNotNil(t, parser.vars)
}

func TestAX7_Parser_SetMedium_Good(t *core.T) {
	parser := NewParser("/tmp")
	parser.SetMedium(coreio.Local)
	core.AssertNotNil(t, parser.configuredMedium())
	core.AssertEqual(t, coreio.Local, parser.configuredMedium())
}

func TestAX7_Parser_SetMedium_Bad(t *core.T) {
	var parser *Parser
	core.AssertNotPanics(t, func() { parser.SetMedium(coreio.Local) })
	core.AssertNil(t, parser)
}

func TestAX7_Parser_SetMedium_Ugly(t *core.T) {
	parser := NewParser("/tmp")
	parser.SetMedium(nil)
	core.AssertNil(t, parser.configuredMedium())
	core.AssertNotNil(t, parser.vars)
}

func TestAX7_Parser_ParsePlaybook_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "site.yml")
	ax7WriteFile(t, path, "- hosts: all\n  tasks: []\n")
	parser := NewParser(dir)
	plays, err := parser.ParsePlaybook(path)
	core.AssertNoError(t, err)
	core.AssertLen(t, plays, 1)
}

func TestAX7_Parser_ParsePlaybook_Bad(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yml")
	ax7WriteFile(t, path, "not: [valid")
	parser := NewParser(dir)
	plays, err := parser.ParsePlaybook(path)
	core.AssertError(t, err)
	core.AssertNil(t, plays)
}

func TestAX7_Parser_ParsePlaybook_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.yml")
	ax7WriteFile(t, path, "[]\n")
	parser := NewParser(dir)
	plays, err := parser.ParsePlaybook(path)
	core.AssertNoError(t, err)
	core.AssertLen(t, plays, 0)
}

func TestAX7_Parser_ParsePlaybookIter_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "site.yml")
	ax7WriteFile(t, path, "- hosts: all\n  tasks: []\n")
	parser := NewParser(dir)
	seq, err := parser.ParsePlaybookIter(path)
	core.AssertNoError(t, err)
	var names []string
	for play := range seq {
		names = append(names, play.Hosts)
	}
	core.AssertEqual(t, []string{"all"}, names)
}

func TestAX7_Parser_ParsePlaybookIter_Bad(t *core.T) {
	parser := NewParser(t.TempDir())
	seq, err := parser.ParsePlaybookIter("missing.yml")
	core.AssertError(t, err)
	core.AssertNil(t, seq)
}

func TestAX7_Parser_ParsePlaybookIter_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "site.yml")
	ax7WriteFile(t, path, "- hosts: one\n  tasks: []\n- hosts: two\n  tasks: []\n")
	parser := NewParser(dir)
	seq, err := parser.ParsePlaybookIter(path)
	core.AssertNoError(t, err)
	count := 0
	seq(func(Play) bool {
		count++
		return false
	})
	core.AssertEqual(t, 1, count)
}

func TestAX7_Parser_ParseInventory_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inventory.yml")
	ax7WriteFile(t, path, "all:\n  hosts:\n    web1: {}\n")
	parser := NewParser(dir)
	inv, err := parser.ParseInventory(path)
	core.AssertNoError(t, err)
	core.AssertContains(t, inv.All.Hosts, "web1")
}

func TestAX7_Parser_ParseInventory_Bad(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "inventory.yml")
	ax7WriteFile(t, path, "all: [")
	parser := NewParser(dir)
	inv, err := parser.ParseInventory(path)
	core.AssertError(t, err)
	core.AssertNil(t, inv)
}

func TestAX7_Parser_ParseInventory_Ugly(t *core.T) {
	dir := t.TempDir()
	ax7WriteFile(t, filepath.Join(dir, "hosts.yml"), "all:\n  hosts:\n    db1: {}\n")
	parser := NewParser(dir)
	inv, err := parser.ParseInventory(dir)
	core.AssertNoError(t, err)
	core.AssertContains(t, inv.All.Hosts, "db1")
}

func TestAX7_Parser_ParseTasks_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yml")
	ax7WriteFile(t, path, "- name: hello\n  shell: echo hello\n")
	parser := NewParser(dir)
	tasks, err := parser.ParseTasks(path)
	core.AssertNoError(t, err)
	core.AssertLen(t, tasks, 1)
	core.AssertEqual(t, "shell", tasks[0].Module)
}

func TestAX7_Parser_ParseTasks_Bad(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yml")
	ax7WriteFile(t, path, "not: [valid")
	parser := NewParser(dir)
	tasks, err := parser.ParseTasks(path)
	core.AssertError(t, err)
	core.AssertNil(t, tasks)
}

func TestAX7_Parser_ParseTasks_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yml")
	ax7WriteFile(t, path, "[]\n")
	parser := NewParser(dir)
	tasks, err := parser.ParseTasks(path)
	core.AssertNoError(t, err)
	core.AssertLen(t, tasks, 0)
}

func TestAX7_Parser_ParseTasksFromDir_Good(t *core.T) {
	dir := t.TempDir()
	tasksDir := filepath.Join(dir, "tasks")
	ax7WriteFile(t, filepath.Join(tasksDir, "main.yml"), "- debug:\n    msg: ok\n")
	parser := NewParser(dir)
	tasks, err := parser.ParseTasksFromDir(tasksDir)
	core.AssertNoError(t, err)
	core.AssertLen(t, tasks, 1)
}

func TestAX7_Parser_ParseTasksFromDir_Bad(t *core.T) {
	parser := NewParser(t.TempDir())
	tasks, err := parser.ParseTasksFromDir("missing")
	core.AssertError(t, err)
	core.AssertNil(t, tasks)
}

func TestAX7_Parser_ParseTasksFromDir_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yml")
	ax7WriteFile(t, path, "- debug:\n    msg: ok\n")
	parser := NewParser(dir)
	tasks, err := parser.ParseTasksFromDir(path)
	core.AssertNoError(t, err)
	core.AssertLen(t, tasks, 1)
}

func TestAX7_Parser_ParseTasksIter_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yml")
	ax7WriteFile(t, path, "- shell: echo one\n- shell: echo two\n")
	parser := NewParser(dir)
	seq, err := parser.ParseTasksIter(path)
	core.AssertNoError(t, err)
	count := 0
	for range seq {
		count++
	}
	core.AssertEqual(t, 2, count)
}

func TestAX7_Parser_ParseTasksIter_Bad(t *core.T) {
	parser := NewParser(t.TempDir())
	seq, err := parser.ParseTasksIter("missing.yml")
	core.AssertError(t, err)
	core.AssertNil(t, seq)
}

func TestAX7_Parser_ParseTasksIter_Ugly(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tasks.yml")
	ax7WriteFile(t, path, "- shell: echo one\n- shell: echo two\n")
	parser := NewParser(dir)
	seq, err := parser.ParseTasksIter(path)
	core.AssertNoError(t, err)
	count := 0
	seq(func(Task) bool {
		count++
		return false
	})
	core.AssertEqual(t, 1, count)
}

func TestAX7_Parser_ParseVarsFiles_Good(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vars.yml")
	ax7WriteFile(t, path, "answer: 42\n")
	parser := NewParser(dir)
	vars, err := parser.ParseVarsFiles(path)
	core.AssertNoError(t, err)
	core.AssertEqual(t, 42, vars["answer"])
}

func TestAX7_Parser_ParseVarsFiles_Bad(t *core.T) {
	parser := NewParser(t.TempDir())
	vars, err := parser.ParseVarsFiles("*.missing")
	core.AssertError(t, err)
	core.AssertNil(t, vars)
}

func TestAX7_Parser_ParseVarsFiles_Ugly(t *core.T) {
	parser := NewParser(t.TempDir())
	vars, err := parser.ParseVarsFiles("")
	core.AssertNoError(t, err)
	core.AssertNil(t, vars)
}

func TestAX7_Parser_ParseRoles_Good(t *core.T) {
	dir := t.TempDir()
	ax7WriteFile(t, filepath.Join(dir, "roles", "web", "tasks", "main.yml"), "- debug:\n    msg: ok\n")
	parser := NewParser(dir)
	roles, err := parser.ParseRoles(filepath.Join(dir, "roles"))
	core.AssertNoError(t, err)
	core.AssertContains(t, roles, "web")
}

func TestAX7_Parser_ParseRoles_Bad(t *core.T) {
	parser := NewParser(t.TempDir())
	roles, err := parser.ParseRoles("missing")
	core.AssertError(t, err)
	core.AssertNil(t, roles)
}

func TestAX7_Parser_ParseRoles_Ugly(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, os.MkdirAll(filepath.Join(dir, "roles"), 0o755))
	parser := NewParser(dir)
	roles, err := parser.ParseRoles(filepath.Join(dir, "roles"))
	core.AssertNoError(t, err)
	core.AssertLen(t, roles, 0)
}

func TestAX7_Parser_ParseRole_Good(t *core.T) {
	dir := t.TempDir()
	ax7WriteFile(t, filepath.Join(dir, "roles", "web", "tasks", "main.yml"), "- debug:\n    msg: ok\n")
	parser := NewParser(dir)
	tasks, err := parser.ParseRole("web", "main.yml")
	core.AssertNoError(t, err)
	core.AssertLen(t, tasks, 1)
}

func TestAX7_Parser_ParseRole_Bad(t *core.T) {
	parser := NewParser(t.TempDir())
	tasks, err := parser.ParseRole("missing", "main.yml")
	core.AssertError(t, err)
	core.AssertNil(t, tasks)
}

func TestAX7_Parser_ParseRole_Ugly(t *core.T) {
	dir := t.TempDir()
	ax7WriteFile(t, filepath.Join(dir, "roles", "web", "tasks", "alt.yml"), "- debug:\n    msg: alt\n")
	parser := NewParser(dir)
	tasks, err := parser.ParseRole("web", "alt.yml")
	core.AssertNoError(t, err)
	core.AssertLen(t, tasks, 1)
}

func TestAX7_NormalizeModule_Bad(t *core.T) {
	module := NormalizeModule("custom.collection.module")
	core.AssertEqual(t, "custom.collection.module", module)
	core.AssertNotEqual(t, "ansible.builtin.custom.collection.module", module)
}

func TestAX7_NormalizeModule_Ugly(t *core.T) {
	module := NormalizeModule("ansible.legacy.shell")
	core.AssertEqual(t, "ansible.builtin.shell", module)
	core.AssertNotEqual(t, "ansible.legacy.shell", module)
}

func TestAX7_GetHosts_Good(t *core.T) {
	inv := ax7Inventory()
	hosts := GetHosts(inv, "db")
	core.AssertEqual(t, []string{"db1"}, hosts)
	core.AssertContains(t, hosts, "db1")
}

func TestAX7_GetHosts_Bad(t *core.T) {
	inv := ax7Inventory()
	hosts := GetHosts(inv, "missing")
	core.AssertNil(t, hosts)
	core.AssertLen(t, hosts, 0)
}

func TestAX7_GetHosts_Ugly(t *core.T) {
	inv := ax7Inventory()
	hosts := GetHosts(inv, "all:!db")
	core.AssertEqual(t, []string{"web1"}, hosts)
	core.AssertNotContains(t, hosts, "db1")
}

func TestAX7_GetHostsIter_Bad(t *core.T) {
	inv := ax7Inventory()
	seq := GetHostsIter(inv, "missing")
	count := 0
	for range seq {
		count++
	}
	core.AssertEqual(t, 0, count)
}

func TestAX7_GetHostsIter_Ugly(t *core.T) {
	inv := ax7Inventory()
	seq := GetHostsIter(inv, "all")
	count := 0
	seq(func(string) bool {
		count++
		return false
	})
	core.AssertEqual(t, 1, count)
}

func TestAX7_AllHostsIter_Bad(t *core.T) {
	seq := AllHostsIter(nil)
	count := 0
	for range seq {
		count++
	}
	core.AssertEqual(t, 0, count)
}

func TestAX7_AllHostsIter_Ugly(t *core.T) {
	seq := AllHostsIter(ax7Inventory().All)
	count := 0
	seq(func(string) bool {
		count++
		return false
	})
	core.AssertEqual(t, 1, count)
}

func TestAX7_GetHostVars_Good(t *core.T) {
	inv := ax7Inventory()
	vars := GetHostVars(inv, "db1")
	core.AssertEqual(t, "test", vars["env"])
	core.AssertEqual(t, "database", vars["tier"])
	core.AssertEqual(t, "primary", vars["role"])
}

func TestAX7_GetHostVars_Bad(t *core.T) {
	inv := ax7Inventory()
	vars := GetHostVars(inv, "missing")
	core.AssertLen(t, vars, 0)
	core.AssertNotContains(t, vars, "role")
}

func TestAX7_GetHostVars_Ugly(t *core.T) {
	vars := GetHostVars(nil, "db1")
	core.AssertNotNil(t, vars)
	core.AssertLen(t, vars, 0)
}

func TestAX7_Play_UnmarshalYAML_Good(t *core.T) {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: all\nansible.builtin.import_playbook: child.yml\n"), &play)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "all", play.Hosts)
	core.AssertEqual(t, "child.yml", play.ImportPlaybook)
}

func TestAX7_Play_UnmarshalYAML_Bad(t *core.T) {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: ["), &play)
	core.AssertError(t, err)
	core.AssertEmpty(t, play.Hosts)
}

func TestAX7_Play_UnmarshalYAML_Ugly(t *core.T) {
	var play Play
	err := yaml.Unmarshal([]byte("hosts: localhost\ngather_facts: false\n"), &play)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, play.GatherFacts)
	core.AssertFalse(t, *play.GatherFacts)
}

func TestAX7_RoleRef_UnmarshalYAML_Good(t *core.T) {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("web\n"), &ref)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "web", ref.Role)
}

func TestAX7_RoleRef_UnmarshalYAML_Bad(t *core.T) {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("- web\n"), &ref)
	core.AssertError(t, err)
	core.AssertEmpty(t, ref.Role)
}

func TestAX7_RoleRef_UnmarshalYAML_Ugly(t *core.T) {
	var ref RoleRef
	err := yaml.Unmarshal([]byte("name: db\ntags: [setup]\n"), &ref)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "db", ref.Role)
	core.AssertEqual(t, []string{"setup"}, ref.Tags)
}

func TestAX7_Task_UnmarshalYAML_Good(t *core.T) {
	var task Task
	err := yaml.Unmarshal([]byte("name: hello\nshell: echo hi\n"), &task)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "hello", task.Name)
	core.AssertEqual(t, "shell", task.Module)
}

func TestAX7_Task_UnmarshalYAML_Bad(t *core.T) {
	var task Task
	err := yaml.Unmarshal([]byte("42\n"), &task)
	core.AssertError(t, err)
	core.AssertEmpty(t, task.Module)
}

func TestAX7_Task_UnmarshalYAML_Ugly(t *core.T) {
	var task Task
	err := yaml.Unmarshal([]byte("ansible.builtin.include_role:\n  name: web\n"), &task)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, task.IncludeRole)
	core.AssertEqual(t, "web", task.IncludeRole.Role)
}

func TestAX7_Inventory_UnmarshalYAML_Good(t *core.T) {
	var inv Inventory
	err := yaml.Unmarshal([]byte("all:\n  hosts:\n    web1: {}\n"), &inv)
	core.AssertNoError(t, err)
	core.AssertContains(t, inv.All.Hosts, "web1")
}

func TestAX7_Inventory_UnmarshalYAML_Bad(t *core.T) {
	var inv Inventory
	err := yaml.Unmarshal([]byte("all: ["), &inv)
	core.AssertError(t, err)
	core.AssertNil(t, inv.All)
}

func TestAX7_Inventory_UnmarshalYAML_Ugly(t *core.T) {
	var inv Inventory
	err := yaml.Unmarshal([]byte("web:\n  hosts:\n    web1: {}\nhost_vars:\n  web1:\n    role: app\n"), &inv)
	core.AssertNoError(t, err)
	core.AssertContains(t, inv.All.Children, "web")
	core.AssertEqual(t, "app", inv.HostVars["web1"]["role"])
}
