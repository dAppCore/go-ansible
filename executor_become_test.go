package ansible

import (
	"context"
	"io"
	"io/fs"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type becomeRecordingClient struct {
	mu            sync.Mutex
	become        bool
	becomeUser    string
	becomePass    string
	runBecomeSeen []bool
	runBecomePass []string
}

func (c *becomeRecordingClient) Run(_ context.Context, _ string) (string, string, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runBecomeSeen = append(c.runBecomeSeen, c.become)
	c.runBecomePass = append(c.runBecomePass, c.becomePass)
	return "", "", 0, nil
}

func (c *becomeRecordingClient) RunScript(_ context.Context, _ string) (string, string, int, error) {
	return c.Run(context.Background(), "")
}

func (c *becomeRecordingClient) Upload(_ context.Context, _ io.Reader, _ string, _ fs.FileMode) error {
	return nil
}

func (c *becomeRecordingClient) Download(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

func (c *becomeRecordingClient) FileExists(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (c *becomeRecordingClient) Stat(_ context.Context, _ string) (map[string]any, error) {
	return map[string]any{}, nil
}

func (c *becomeRecordingClient) BecomeState() (bool, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.become, c.becomeUser, c.becomePass
}

func (c *becomeRecordingClient) SetBecome(become bool, user, password string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.become = become
	if !become {
		c.becomeUser = ""
		c.becomePass = ""
		return
	}
	if user != "" {
		c.becomeUser = user
	}
	if password != "" {
		c.becomePass = password
	}
}

func (c *becomeRecordingClient) Close() error {
	return nil
}

func TestExecutor_RunTaskOnHost_Good_TaskBecomeFalseOverridesPlayBecome(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {AnsibleHost: "127.0.0.1"},
			},
		},
	})

	client := &becomeRecordingClient{}
	client.SetBecome(true, "root", "secret")
	e.clients["host1"] = client

	play := &Play{Become: true, BecomeUser: "admin"}
	task := &Task{
		Name:   "Disable become for this task",
		Module: "command",
		Args:   map[string]any{"cmd": "echo ok"},
		Become: func() *bool { v := false; return &v }(),
	}

	require.NoError(t, e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, play))
	require.Len(t, client.runBecomeSeen, 1)
	assert.False(t, client.runBecomeSeen[0])
	assert.True(t, client.become)
	assert.Equal(t, "admin", client.becomeUser)
	assert.Equal(t, "secret", client.becomePass)
}

func TestExecutor_RunTaskOnHost_Good_TaskBecomeUsesInventoryPassword(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {
					AnsibleHost:           "127.0.0.1",
					AnsibleBecomePassword: "secret",
				},
			},
		},
	})

	client := &becomeRecordingClient{}
	e.clients["host1"] = client

	play := &Play{}
	task := &Task{
		Name:   "Enable become for this task",
		Module: "command",
		Args:   map[string]any{"cmd": "echo ok"},
		Become: func() *bool { v := true; return &v }(),
	}

	require.NoError(t, e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, play))
	require.Len(t, client.runBecomeSeen, 1)
	assert.True(t, client.runBecomeSeen[0])
	require.Len(t, client.runBecomePass, 1)
	assert.Equal(t, "secret", client.runBecomePass[0])
}
