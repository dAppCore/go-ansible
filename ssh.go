package ansible

import (
	"context"
	"io"
	"io/fs"
	"net"
	"sync"
	"time"

	core "dappco.re/go"
	coreio "dappco.re/go/io"
	coreerr "dappco.re/go/log"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHClient handles SSH connections to remote hosts.
//
// Example:
//
//	client, _ := NewSSHClient(SSHConfig{Host: "web1"})
type SSHClient struct {
	host       string
	port       int
	user       string
	password   string
	keyFile    string
	client     *ssh.Client
	mu         sync.Mutex
	become     bool
	becomeUser string
	becomePass string
	timeout    time.Duration
}

// SSHConfig holds SSH connection configuration.
//
// Example:
//
//	config := SSHConfig{Host: "web1", User: "deploy", Port: 22}
type SSHConfig struct {
	Host       string
	Port       int
	User       string
	Password   string
	KeyFile    string
	Become     bool
	BecomeUser string
	BecomePass string
	Timeout    time.Duration
}

// NewSSHClient creates a new SSH client.
//
// Example:
//
//	result := NewSSHClient(SSHConfig{Host: "web1", User: "deploy"})
func NewSSHClient(config SSHConfig) core.Result {
	if config.Port == 0 {
		config.Port = 22
	}
	if config.User == "" {
		config.User = "root"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	client := &SSHClient{
		host:       config.Host,
		port:       config.Port,
		user:       config.User,
		password:   config.Password,
		keyFile:    config.KeyFile,
		become:     config.Become,
		becomeUser: config.BecomeUser,
		becomePass: config.BecomePass,
		timeout:    config.Timeout,
	}

	return core.Ok(client)
}

// Connect establishes the SSH connection.
//
// Example:
//
//	result := client.Connect(context.Background())
func (c *SSHClient) Connect(
	ctx context.Context,
) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return core.Ok(nil)
	}

	var authMethods []ssh.AuthMethod

	// Try key-based auth first
	if c.keyFile != "" {
		keyPath := c.keyFile
		if corexHasPrefix(keyPath, "~") {
			keyPath = joinPath(env("DIR_HOME"), keyPath[1:])
		}

		if key, err := coreio.Local.Read(keyPath); err == nil {
			if signer, err := ssh.ParsePrivateKey([]byte(key)); err == nil {
				authMethods = append(authMethods, ssh.PublicKeys(signer))
			}
		}
	}

	// Try default SSH keys
	if len(authMethods) == 0 {
		home := env("DIR_HOME")
		defaultKeys := []string{
			joinPath(home, ".ssh", "id_ed25519"),
			joinPath(home, ".ssh", "id_rsa"),
		}
		for _, keyPath := range defaultKeys {
			if key, err := coreio.Local.Read(keyPath); err == nil {
				if signer, err := ssh.ParsePrivateKey([]byte(key)); err == nil {
					authMethods = append(authMethods, ssh.PublicKeys(signer))
					break
				}
			}
		}
	}

	// Fall back to password auth
	if c.password != "" {
		authMethods = append(authMethods, ssh.Password(c.password))
	}

	if len(authMethods) == 0 {
		return core.Fail(coreerr.E("ssh.Connect", "no authentication method available", nil))
	}

	// Host key verification
	var hostKeyCallback ssh.HostKeyCallback

	home := env("DIR_HOME")
	if home == "" {
		return core.Fail(coreerr.E("ssh.Connect", "failed to get user home dir", nil))
	}
	knownHostsPath := joinPath(home, ".ssh", "known_hosts")

	// Ensure known_hosts file exists
	if !coreio.Local.Exists(knownHostsPath) {
		if err := coreio.Local.EnsureDir(pathDir(knownHostsPath)); err != nil {
			return core.Fail(coreerr.E("ssh.Connect", "failed to create .ssh dir", err))
		}
		if err := coreio.Local.Write(knownHostsPath, ""); err != nil {
			return core.Fail(coreerr.E("ssh.Connect", "failed to create known_hosts file", err))
		}
	}

	cb, err := knownhosts.New(knownHostsPath)
	if err != nil {
		return core.Fail(coreerr.E("ssh.Connect", "failed to load known_hosts", err))
	}
	hostKeyCallback = cb

	config := &ssh.ClientConfig{
		User:            c.user,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         c.timeout,
	}

	addr := sprintf("%s:%d", c.host, c.port)

	// Connect with context timeout
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return core.Fail(coreerr.E("ssh.Connect", sprintf("dial %s", addr), err))
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(conn, addr, config)
	if err != nil {
		// conn is closed by NewClientConn on error
		return core.Fail(coreerr.E("ssh.Connect", sprintf("ssh connect %s", addr), err))
	}

	c.client = ssh.NewClient(sshConn, chans, reqs)
	return core.Ok(nil)
}

// Close closes the SSH connection.
//
// Example:
//
//	result := client.Close()
func (c *SSHClient) Close() core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		err := c.client.Close()
		c.client = nil
		if err != nil {
			return core.Fail(err)
		}
	}
	return core.Ok(nil)
}

// BecomeState returns the current privilege escalation settings.
//
// Example:
//
//	become, user, password := client.BecomeState()
func (c *SSHClient) BecomeState() (bool, string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.become, c.becomeUser, c.becomePass
}

// Run executes a command on the remote host.
//
// Example:
//
//	result := client.Run(context.Background(), "hostname")
func (c *SSHClient) Run(ctx context.Context, cmd string) core.Result {
	if r := c.Connect(ctx); !r.OK {
		return r
	}

	session, err := c.client.NewSession()
	if err != nil {
		return core.Fail(coreerr.E("ssh.Run", "new session", err))
	}
	defer session.Close()

	stdoutBuf := core.NewBuffer()
	stderrBuf := core.NewBuffer()
	session.Stdout = stdoutBuf
	session.Stderr = stderrBuf

	// Apply become if needed
	if c.become {
		becomeUser := c.becomeUser
		if becomeUser == "" {
			becomeUser = "root"
		}
		// Escape single quotes in the command
		escapedCmd := replaceAll(cmd, "'", "'\\''")
		if c.becomePass != "" {
			// Use sudo with password via stdin (-S flag)
			// We launch a goroutine to write the password to stdin
			cmd = sprintf("sudo -S -u %s bash -c '%s'", becomeUser, escapedCmd)
			stdin, err := session.StdinPipe()
			if err != nil {
				return core.Fail(coreerr.E("ssh.Run", "stdin pipe", err))
			}
			go func() {
				defer stdin.Close()
				writeString(stdin, c.becomePass+"\n")
			}()
		} else if c.password != "" {
			// Try using connection password for sudo
			cmd = sprintf("sudo -S -u %s bash -c '%s'", becomeUser, escapedCmd)
			stdin, err := session.StdinPipe()
			if err != nil {
				return core.Fail(coreerr.E("ssh.Run", "stdin pipe", err))
			}
			go func() {
				defer stdin.Close()
				writeString(stdin, c.password+"\n")
			}()
		} else {
			// Try passwordless sudo
			cmd = sprintf("sudo -n -u %s bash -c '%s'", becomeUser, escapedCmd)
		}
	}

	// Run with context
	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		if err := session.Signal(ssh.SIGKILL); err != nil && ctx.Err() == nil {
			return commandRunFail(stdoutBuf.String(), stderrBuf.String(), -1, err)
		}
		return commandRunFail(stdoutBuf.String(), stderrBuf.String(), -1, ctx.Err())
	case err := <-done:
		exitCode := 0
		if err != nil {
			if exitErr, ok := err.(*ssh.ExitError); ok {
				exitCode = exitErr.ExitStatus()
			} else {
				return commandRunFail(stdoutBuf.String(), stderrBuf.String(), -1, err)
			}
		}
		return commandRunOK(stdoutBuf.String(), stderrBuf.String(), exitCode)
	}
}

// RunScript runs a script on the remote host.
//
// Example:
//
//	result := client.RunScript(context.Background(), "echo hello")
func (c *SSHClient) RunScript(ctx context.Context, script string) core.Result {
	// Escape the script for heredoc
	cmd := sprintf("bash <<'ANSIBLE_SCRIPT_EOF'\n%s\nANSIBLE_SCRIPT_EOF", script)
	return c.Run(ctx, cmd)
}

// Upload copies a file to the remote host.
//
// Example:
//
//	result := client.Upload(context.Background(), newReader("hello"), "/tmp/hello.txt", 0644)
func (c *SSHClient) Upload(
	ctx context.Context, local io.Reader, remote string, mode fs.FileMode,
) core.Result {
	if r := c.Connect(ctx); !r.OK {
		return r
	}

	// Read content
	contentResult := readAllString(local)
	if !contentResult.OK {
		return wrapFailure(contentResult, "ssh.Upload", "read content")
	}
	content := contentResult.Value.(string)

	// Create parent directory
	dir := pathDir(remote)
	dirCmd := sprintf("mkdir -p %q", dir)
	if c.become {
		dirCmd = sprintf("sudo mkdir -p %q", dir)
	}
	if r := c.Run(ctx, dirCmd); !r.OK {
		return wrapFailure(r, "ssh.Upload", "create parent dir")
	}

	// Use cat to write the file (simpler than SCP)
	writeCmd := sprintf("cat > %q && chmod %o %q", remote, mode, remote)

	// If become is needed, we construct a command that reads password then content from stdin
	// But we need to be careful with handling stdin for sudo + cat.
	// We'll use a session with piped stdin.

	session2, err := c.client.NewSession()
	if err != nil {
		return core.Fail(coreerr.E("ssh.Upload", "new session for write", err))
	}
	defer session2.Close()

	stdin, err := session2.StdinPipe()
	if err != nil {
		return core.Fail(coreerr.E("ssh.Upload", "stdin pipe", err))
	}

	stderrBuf := core.NewBuffer()
	session2.Stderr = stderrBuf

	if c.become {
		becomeUser := c.becomeUser
		if becomeUser == "" {
			becomeUser = "root"
		}

		pass := c.becomePass
		if pass == "" {
			pass = c.password
		}

		if pass != "" {
			// Use sudo -S with password from stdin
			writeCmd = sprintf("sudo -S -u %s bash -c 'cat > %q && chmod %o %q'",
				becomeUser, remote, mode, remote)
		} else {
			// Use passwordless sudo (sudo -n) to avoid consuming file content as password
			writeCmd = sprintf("sudo -n -u %s bash -c 'cat > %q && chmod %o %q'",
				becomeUser, remote, mode, remote)
		}

		if err := session2.Start(writeCmd); err != nil {
			return core.Fail(coreerr.E("ssh.Upload", "start write", err))
		}

		go func() {
			defer stdin.Close()
			if pass != "" {
				writeString(stdin, pass+"\n")
			}
			if _, writeErr := stdin.Write([]byte(content)); writeErr != nil {
				return
			}
		}()
	} else {
		// Normal write
		if err := session2.Start(writeCmd); err != nil {
			return core.Fail(coreerr.E("ssh.Upload", "start write", err))
		}

		go func() {
			defer stdin.Close()
			if _, writeErr := stdin.Write([]byte(content)); writeErr != nil {
				return
			}
		}()
	}

	if err := session2.Wait(); err != nil {
		return core.Fail(coreerr.E("ssh.Upload", sprintf("write failed (stderr: %s)", stderrBuf.String()), err))
	}

	return core.Ok(nil)
}

// Download copies a file from the remote host.
//
// Example:
//
//	result := client.Download(context.Background(), "/etc/hostname")
func (c *SSHClient) Download(ctx context.Context, remote string) core.Result {
	if r := c.Connect(ctx); !r.OK {
		return r
	}

	cmd := sprintf("cat %q", remote)

	run := c.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK {
		return run
	}
	if out.ExitCode != 0 {
		return core.Fail(coreerr.E("ssh.Download", sprintf("cat failed: %s", out.Stderr), nil))
	}

	return core.Ok([]byte(out.Stdout))
}

// FileExists checks if a file exists on the remote host.
//
// Example:
//
//	result := client.FileExists(context.Background(), "/etc/hosts")
func (c *SSHClient) FileExists(ctx context.Context, path string) core.Result {
	cmd := sprintf("test -e %q && echo yes || echo no", path)
	run := c.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK {
		return run
	}
	if out.ExitCode != 0 {
		// test command failed but didn't error - file doesn't exist
		return core.Ok(false)
	}
	return core.Ok(corexTrimSpace(out.Stdout) == "yes")
}

// Stat returns file info from the remote host.
//
// Example:
//
//	result := client.Stat(context.Background(), "/etc/hosts")
func (c *SSHClient) Stat(ctx context.Context, path string) core.Result {
	// Simple approach - get basic file info
	cmd := sprintf(`
if [ -e %q ]; then
  if [ -d %q ]; then
    echo "exists=true isdir=true"
  else
    echo "exists=true isdir=false"
  fi
else
  echo "exists=false"
fi
`, path, path)

	run := c.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK {
		return run
	}

	result := make(map[string]any)
	parts := fields(corexTrimSpace(out.Stdout))
	for _, part := range parts {
		kv := splitN(part, "=", 2)
		if len(kv) == 2 {
			result[kv[0]] = kv[1] == "true"
		}
	}

	return core.Ok(result)
}

// SetBecome enables privilege escalation.
//
// Example:
//
//	client.SetBecome(true, "root", "")
func (c *SSHClient) SetBecome(become bool, user, password string) {
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
