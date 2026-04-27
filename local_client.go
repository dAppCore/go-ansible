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

	coreerr "dappco.re/go/log"
)

// localClient executes commands and file operations on the controller host.
// It satisfies sshExecutorClient so the executor can reuse the same module
// handlers for `connection: local` playbooks.
//
// Example:
//
//	client := newLocalClient()
type localClient struct {
	mu         sync.Mutex
	become     bool
	becomeUser string
	becomePass string
}

// newLocalClient creates a controller-side client for `connection: local`.
//
// Example:
//
//	client := newLocalClient()
func newLocalClient() *localClient {
	return &localClient{}
}

// BecomeState returns the current become flag, user, and password under
// the lock. Used by playbook tasks to decide whether to wrap commands in
// sudo.
//
//	become, user, pass := c.BecomeState()
func (c *localClient) BecomeState() (bool, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.become, c.becomeUser, c.becomePass
}

// SetBecome updates the become flag and credentials. When become is false,
// any stored user/password is cleared. Empty user/password arguments are
// ignored when become is true (so callers can update fields incrementally).
//
//	c.SetBecome(true, "ansible", "secret")
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

// Close is a no-op for the local client — there is no remote connection
// to tear down. Kept on the interface to match SSH variants.
//
//	_ = c.Close()
func (c *localClient) Close() error {
	return nil
}

// Run executes cmd via local shell, optionally wrapped in sudo when
// become is enabled. Returns stdout/stderr/exit-code/error.
//
//	out, _, code, err := c.Run(ctx, "uname -a")
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

// RunScript executes a multi-line shell script as a local heredoc.
//
//	out, _, code, err := c.RunScript(ctx, "#!/bin/bash\necho hi")
func (c *localClient) RunScript(ctx context.Context, script string) (stdout, stderr string, exitCode int, err error) {
	return c.Run(ctx, "bash <<'ANSIBLE_SCRIPT_EOF'\n"+script+"\nANSIBLE_SCRIPT_EOF")
}

// Upload writes the contents of localReader into a file at remote with the
// given permission mode. Creates parent directories as needed.
//
//	_ = c.Upload(ctx, bytes.NewReader(data), "/etc/foo.conf", 0o644)
func (c *localClient) Upload(_ context.Context, localReader io.Reader, remote string, mode os.FileMode) error {
	content, err := io.ReadAll(localReader)
	if err != nil {
		return coreerr.E("localClient.Upload", "read upload content", err)
	}

	if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
		return coreerr.E("localClient.Upload", "create remote directory", err)
	}
	if err := os.WriteFile(remote, content, mode); err != nil {
		return coreerr.E("localClient.Upload", "write remote file", err)
	}
	return nil
}

// Download reads the bytes of a local file path. Despite the name, the
// "remote" is local — the localClient implements the SSHClient interface
// transparently so playbooks targeting `connection: local` work unchanged.
//
//	data, err := c.Download(ctx, "/etc/foo.conf")
func (c *localClient) Download(_ context.Context, remote string) ([]byte, error) {
	data, err := os.ReadFile(remote)
	if err != nil {
		return nil, coreerr.E("localClient.Download", "read remote file", err)
	}
	return data, nil
}

// FileExists reports whether the given local path exists. Returns
// (false, nil) for non-existent paths and (false, err) for stat failures
// other than not-exist.
//
//	exists, err := c.FileExists(ctx, "/etc/foo.conf")
func (c *localClient) FileExists(_ context.Context, path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, coreerr.E("localClient.FileExists", "stat path", err)
}

// Stat returns a map describing the path: at minimum {"exists": bool}, plus
// "isdir" / "size" / "mode" / "mtime" when the file exists. Used by the
// `stat` Ansible module.
//
//	info, err := c.Stat(ctx, "/etc/foo.conf")
func (c *localClient) Stat(_ context.Context, path string) (map[string]any, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{"exists": false}, nil
		}
		return nil, coreerr.E("localClient.Stat", "stat path", err)
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
	cmd := exec.CommandContext(ctx, "bash", "--noprofile", "--norc", "-lc", command)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if password != "" {
		stdin, stdinErr := cmd.StdinPipe()
		if stdinErr != nil {
			return "", "", -1, coreerr.E("localClient.runLocalShell", "open stdin", stdinErr)
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

	return stdout, stderr, -1, coreerr.E("localClient.runLocalShell", "execute command", err)
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
