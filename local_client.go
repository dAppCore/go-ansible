package ansible

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// localClient executes commands and file operations on the controller host.
// It satisfies sshExecutorClient so the executor can reuse the same module
// handlers for `connection: local` playbooks.
type localClient struct {
	mu         sync.Mutex
	become     bool
	becomeUser string
	becomePass string
}

func newLocalClient() *localClient {
	return &localClient{}
}

func (c *localClient) BecomeState() (bool, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.become, c.becomeUser, c.becomePass
}

func (c *localClient) SetBecome(become bool, user, password string) {
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

func (c *localClient) Close() error {
	return nil
}

func (c *localClient) Run(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error) {
	c.mu.Lock()
	become, becomeUser, becomePass := c.becomeStateLocked()
	c.mu.Unlock()

	command := cmd
	if become {
		command = wrapLocalBecomeCommand(command, becomeUser, becomePass)
	}

	if become {
		return runLocalShell(ctx, command, becomePass)
	}
	return runLocalShell(ctx, command, "")
}

func (c *localClient) RunScript(ctx context.Context, script string) (stdout, stderr string, exitCode int, err error) {
	return c.Run(ctx, "bash <<'ANSIBLE_SCRIPT_EOF'\n"+script+"\nANSIBLE_SCRIPT_EOF")
}

func (c *localClient) Upload(_ context.Context, localReader io.Reader, remote string, mode os.FileMode) error {
	content, err := io.ReadAll(localReader)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		return err
	}
	return os.WriteFile(remote, content, mode)
}

func (c *localClient) Download(_ context.Context, remote string) ([]byte, error) {
	return os.ReadFile(remote)
}

func (c *localClient) FileExists(_ context.Context, path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (c *localClient) Stat(_ context.Context, path string) (map[string]any, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{"exists": false}, nil
		}
		return nil, err
	}
	return map[string]any{
		"exists": true,
		"isdir":  info.IsDir(),
	}, nil
}

func (c *localClient) becomeStateLocked() (bool, string, string) {
	return c.become, c.becomeUser, c.becomePass
}

func runLocalShell(ctx context.Context, command, password string) (stdout, stderr string, exitCode int, err error) {
	cmd := exec.CommandContext(ctx, "bash", "-lc", command)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if password != "" {
		stdin, stdinErr := cmd.StdinPipe()
		if stdinErr != nil {
			return "", "", -1, stdinErr
		}
		go func() {
			defer func() { _ = stdin.Close() }()
			_, _ = io.WriteString(stdin, password+"\n")
		}()
	}

	err = cmd.Run()
	stdout = stdoutBuf.String()
	stderr = stderrBuf.String()
	if err == nil {
		return stdout, stderr, 0, nil
	}

	if exitErr, ok := err.(*exec.ExitError); ok {
		return stdout, stderr, exitErr.ExitCode(), nil
	}

	return stdout, stderr, -1, err
}

func wrapLocalBecomeCommand(command, user, password string) string {
	if user == "" {
		user = "root"
	}

	escaped := strings.ReplaceAll(command, "'", "'\\''")
	if password != "" {
		return "sudo -S -u " + user + " bash -lc '" + escaped + "'"
	}
	return "sudo -n -u " + user + " bash -lc '" + escaped + "'"
}
