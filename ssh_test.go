package ansible

import (
	core "dappco.re/go"
	"time"
)

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
