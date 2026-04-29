package ansible

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"io"
	"net"

	core "dappco.re/go"
	"golang.org/x/crypto/ssh"
	"time"
)

func connectedSSHClient(t *core.T, responses map[string]string) *SSHClient {
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
							if contains(payload.Command, key) {
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

func TestSSH_NewSSHClient_Good_CustomConfig(t *core.T) {
	cfg := SSHConfig{
		Host: "localhost",
		Port: 2222,
		User: "root",
	}

	client, err := NewSSHClient(cfg)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client)
	core.AssertEqual(t, "localhost", client.host)
	core.AssertEqual(t, 2222, client.port)
	core.AssertEqual(t, "root", client.user)
	core.AssertEqual(t, 30*time.Second, client.timeout)
}

func TestSSH_NewSSHClient_Good_Defaults(t *core.T) {
	cfg := SSHConfig{
		Host: "localhost",
	}

	client, err := NewSSHClient(cfg)
	core.AssertNoError(t, err)
	core.AssertEqual(t, 22, client.port)
	core.AssertEqual(t, "root", client.user)
	core.AssertEqual(t, 30*time.Second, client.timeout)
}

func TestSSH_SetBecome_Good_DisablesAndClearsState(t *core.T) {
	client := &SSHClient{}

	client.SetBecome(true, "admin", "secret")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "secret", password)

	client.SetBecome(false, "", "")
	become, user, password = client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

// --- File-aware public symbol triplets ---

func TestSsh_NewSSHClient_Good(t *core.T) {
	client, err := NewSSHClient(SSHConfig{Host: "web1", Port: 2200, User: "deploy"})
	core.AssertNoError(t, err)
	core.AssertEqual(t, "web1", client.host)
	core.AssertEqual(t, 2200, client.port)
	core.AssertEqual(t, "deploy", client.user)
}

func TestSsh_NewSSHClient_Bad(t *core.T) {
	client, err := NewSSHClient(SSHConfig{Host: "web1"})
	core.AssertNoError(t, err)
	core.AssertEqual(t, 22, client.port)
	core.AssertEqual(t, "root", client.user)
}

func TestSsh_NewSSHClient_Ugly(t *core.T) {
	client, err := NewSSHClient(SSHConfig{Host: "", Timeout: time.Nanosecond})
	core.AssertNoError(t, err)
	core.AssertEqual(t, "", client.host)
	core.AssertEqual(t, time.Nanosecond, client.timeout)
}

func TestSsh_SSHClient_SetBecome_Good(t *core.T) {
	client := &SSHClient{}
	client.SetBecome(true, "admin", "pw")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "pw", password)
}

func TestSsh_SSHClient_SetBecome_Bad(t *core.T) {
	client := &SSHClient{}
	client.SetBecome(false, "admin", "pw")
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestSsh_SSHClient_SetBecome_Ugly(t *core.T) {
	client := &SSHClient{}
	client.SetBecome(true, "admin", "pw")
	client.SetBecome(true, "", "")
	_, user, password := client.BecomeState()
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "pw", password)
}

func TestSsh_SSHClient_BecomeState_Good(t *core.T) {
	client := &SSHClient{become: true, becomeUser: "root", becomePass: "pw"}
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "root", user)
	core.AssertEqual(t, "pw", password)
}

func TestSsh_SSHClient_BecomeState_Bad(t *core.T) {
	client := &SSHClient{}
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestSsh_SSHClient_BecomeState_Ugly(t *core.T) {
	client := &SSHClient{become: true}
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestSsh_SSHClient_Close_Good(t *core.T) {
	client := connectedSSHClient(t, nil)
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNil(t, client.client)
}

func TestSsh_SSHClient_Close_Bad(t *core.T) {
	client := &SSHClient{}
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNil(t, client.client)
}

func TestSsh_SSHClient_Close_Ugly(t *core.T) {
	client := connectedSSHClient(t, nil)
	first := client.Close()
	second := client.Close()
	core.AssertNoError(t, first)
	core.AssertNoError(t, second)
}

func TestSsh_SSHClient_Connect_Good(t *core.T) {
	client := connectedSSHClient(t, nil)
	err := client.Connect(context.Background())
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client.client)
}

func TestSsh_SSHClient_Connect_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	err := client.Connect(context.Background())
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no authentication method")
}

func TestSsh_SSHClient_Connect_Ugly(t *core.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root", password: "pw"}
	err := client.Connect(ctx)
	core.AssertError(t, err)
}

func TestSsh_SSHClient_Run_Good(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"hostname": "web1\n"})
	stdout, stderr, code, err := client.Run(context.Background(), "hostname")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "web1\n", stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestSsh_SSHClient_Run_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	stdout, _, code, err := client.Run(context.Background(), "hostname")
	core.AssertError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEqual(t, -1, code)
}

func TestSsh_SSHClient_Run_Ugly(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"": ""})
	stdout, stderr, code, err := client.Run(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestSsh_SSHClient_RunScript_Good(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"ANSIBLE_SCRIPT_EOF": "script\n"})
	stdout, _, code, err := client.RunScript(context.Background(), "echo script")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "script\n", stdout)
	core.AssertEqual(t, 0, code)
}

func TestSsh_SSHClient_RunScript_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	_, _, code, err := client.RunScript(context.Background(), "echo script")
	core.AssertError(t, err)
	core.AssertEqual(t, -1, code)
}

func TestSsh_SSHClient_RunScript_Ugly(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"ANSIBLE_SCRIPT_EOF": ""})
	stdout, stderr, code, err := client.RunScript(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestSsh_SSHClient_Upload_Good(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"mkdir -p": ""})
	err := client.Upload(context.Background(), newReader("payload"), "/tmp/file.txt", 0o644)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client.client)
}

func TestSsh_SSHClient_Upload_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	err := client.Upload(context.Background(), newReader("payload"), "/tmp/file.txt", 0o644)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no authentication method")
}

func TestSsh_SSHClient_Upload_Ugly(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"mkdir -p": ""})
	err := client.Upload(context.Background(), newReader(""), "/tmp/empty.txt", 0)
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client.client)
}

func TestSsh_SSHClient_Download_Good(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"cat": "downloaded"})
	data, err := client.Download(context.Background(), "/tmp/file.txt")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "downloaded", string(data))
}

func TestSsh_SSHClient_Download_Bad(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	data, err := client.Download(context.Background(), "/tmp/file.txt")
	core.AssertError(t, err)
	core.AssertNil(t, data)
}

func TestSsh_SSHClient_Download_Ugly(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"cat": ""})
	data, err := client.Download(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, data)
}

func TestSsh_SSHClient_FileExists_Good(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"test -e": "yes\n"})
	exists, err := client.FileExists(context.Background(), "/tmp/file.txt")
	core.AssertNoError(t, err)
	core.AssertTrue(t, exists)
}

func TestSsh_SSHClient_FileExists_Bad(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"test -e": "no\n"})
	exists, err := client.FileExists(context.Background(), "/tmp/missing.txt")
	core.AssertNoError(t, err)
	core.AssertFalse(t, exists)
}

func TestSsh_SSHClient_FileExists_Ugly(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	exists, err := client.FileExists(context.Background(), "/tmp/file.txt")
	core.AssertError(t, err)
	core.AssertFalse(t, exists)
}

func TestSsh_SSHClient_Stat_Good(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"if [ -e": "exists=true isdir=false\n"})
	info, err := client.Stat(context.Background(), "/tmp/file.txt")
	core.AssertNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])
}

func TestSsh_SSHClient_Stat_Bad(t *core.T) {
	client := connectedSSHClient(t, map[string]string{"if [ -e": "exists=false\n"})
	info, err := client.Stat(context.Background(), "/tmp/missing.txt")
	core.AssertNoError(t, err)
	core.AssertEqual(t, false, info["exists"])
}

func TestSsh_SSHClient_Stat_Ugly(t *core.T) {
	client := &SSHClient{host: "127.0.0.1", port: 1, user: "root"}
	info, err := client.Stat(context.Background(), "/tmp/file.txt")
	core.AssertError(t, err)
	core.AssertNil(t, info)
}
