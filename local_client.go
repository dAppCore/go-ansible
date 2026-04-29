package ansible

import (
	"context"
	"io"
	"sync"
	"syscall"

	core "dappco.re/go"
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
func (c *localClient) Close() (err error) {
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
func (c *localClient) Upload(
	_ context.Context, localReader io.Reader, remote string, mode core.FileMode,
) error {
	read := core.ReadAll(localReader)
	if !read.OK {
		err, _ := read.Value.(error)
		return coreerr.E("localClient.Upload", "read upload content", err)
	}
	content := []byte(read.Value.(string))

	if r := core.MkdirAll(pathDir(remote), 0o755); !r.OK {
		err, _ := r.Value.(error)
		return coreerr.E("localClient.Upload", "create remote directory", err)
	}
	if r := core.WriteFile(remote, content, mode); !r.OK {
		err, _ := r.Value.(error)
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
	read := core.ReadFile(remote)
	if !read.OK {
		err, _ := read.Value.(error)
		return nil, coreerr.E("localClient.Download", "read remote file", err)
	}
	return read.Value.([]byte), nil
}

// FileExists reports whether the given local path exists. Returns
// (false, nil) for non-existent paths and (false, err) for stat failures
// other than not-exist.
//
//	exists, err := c.FileExists(ctx, "/etc/foo.conf")
func (c *localClient) FileExists(_ context.Context, path string) (bool, error) {
	stat := core.Stat(path)
	if stat.OK {
		return true, nil
	}
	err, _ := stat.Value.(error)
	if core.IsNotExist(err) {
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
	stat := core.Stat(path)
	if !stat.OK {
		err, _ := stat.Value.(error)
		if core.IsNotExist(err) {
			return map[string]any{"exists": false}, nil
		}
		return nil, coreerr.E("localClient.Stat", "stat path", err)
	}
	info := stat.Value.(core.FsFileInfo)
	return map[string]any{
		"exists": true,
		"isdir":  info.IsDir(),
	}, nil
}

func (c *localClient) becomeStateLocked() (bool, string, string) {
	return c.become, c.becomeUser, c.becomePass
}

func runLocalShell(ctx context.Context, command, password string) (stdout, stderr string, exitCode int, err error) {
	stdinPipe, err := makePipe()
	if err != nil {
		return "", "", -1, coreerr.E("localClient.runLocalShell", "open stdin", err)
	}
	stdoutPipe, err := makePipe()
	if err != nil {
		closePipe(stdinPipe)
		return "", "", -1, coreerr.E("localClient.runLocalShell", "open stdout", err)
	}
	stderrPipe, err := makePipe()
	if err != nil {
		closePipe(stdinPipe)
		closePipe(stdoutPipe)
		return "", "", -1, coreerr.E("localClient.runLocalShell", "open stderr", err)
	}

	argv := []string{"bash", "--noprofile", "--norc", "-lc", command}
	pid, err := syscall.ForkExec("/bin/bash", argv, &syscall.ProcAttr{
		Env:   core.Environ(),
		Files: []uintptr{uintptr(stdinPipe[0]), uintptr(stdoutPipe[1]), uintptr(stderrPipe[1])},
	})
	syscall.Close(stdinPipe[0])
	syscall.Close(stdoutPipe[1])
	syscall.Close(stderrPipe[1])
	if err != nil {
		closeFD(stdinPipe[1])
		closeFD(stdoutPipe[0])
		closeFD(stderrPipe[0])
		return "", "", -1, coreerr.E("localClient.runLocalShell", "execute command", err)
	}

	if password != "" {
		go func() {
			defer closeFD(stdinPipe[1])
			_, _ = syscall.Write(stdinPipe[1], []byte(password+"\n"))
		}()
	} else {
		closeFD(stdinPipe[1])
	}

	stdoutBuf := core.NewBuffer()
	stderrBuf := core.NewBuffer()
	doneOut := make(chan error, 1)
	doneErr := make(chan error, 1)
	go func() { doneOut <- readPipe(stdoutPipe[0], stdoutBuf) }()
	go func() { doneErr <- readPipe(stderrPipe[0], stderrBuf) }()

	waitCh := make(chan waitResult, 1)
	go func() {
		var status syscall.WaitStatus
		_, waitErr := syscall.Wait4(pid, &status, 0, nil)
		waitCh <- waitResult{status: status, err: waitErr}
	}()

	var wait waitResult
	select {
	case <-ctx.Done():
		if killErr := syscall.Kill(pid, syscall.SIGKILL); killErr != nil {
			return stdoutBuf.String(), stderrBuf.String(), -1, killErr
		}
		wait = <-waitCh
		<-doneOut
		<-doneErr
		return stdoutBuf.String(), stderrBuf.String(), -1, ctx.Err()
	case wait = <-waitCh:
	}

	<-doneOut
	<-doneErr
	if wait.err != nil {
		return stdoutBuf.String(), stderrBuf.String(), -1, coreerr.E("localClient.runLocalShell", "wait command", wait.err)
	}
	if wait.status.Exited() {
		return stdoutBuf.String(), stderrBuf.String(), wait.status.ExitStatus(), nil
	}
	if wait.status.Signaled() {
		return stdoutBuf.String(), stderrBuf.String(), 128 + int(wait.status.Signal()), nil
	}
	return stdoutBuf.String(), stderrBuf.String(), -1, nil
}

func wrapLocalBecomeCommand(command, user, password string) string {
	if user == "" {
		user = "root"
	}

	escaped := replaceAll(command, "'", "'\\''")
	if password != "" {
		return "sudo -S -u " + user + " bash -lc '" + escaped + "'"
	}
	return "sudo -n -u " + user + " bash -lc '" + escaped + "'"
}

type waitResult struct {
	status syscall.WaitStatus
	err    error
}

func makePipe() ([2]int, error) {
	var pipe [2]int
	err := syscall.Pipe(pipe[:])
	return pipe, err
}

func closePipe(pipe [2]int) {
	closeFD(pipe[0])
	closeFD(pipe[1])
}

func closeFD(fd int) {
	if err := syscall.Close(fd); err != nil {
		return
	}
}

func readPipe(
	fd int, buf core.Writer,
) error {
	defer closeFD(fd)
	tmp := make([]byte, 32*1024)
	for {
		n, err := syscall.Read(fd, tmp)
		if n > 0 {
			if _, writeErr := buf.Write(tmp[:n]); writeErr != nil {
				return writeErr
			}
		}
		if n == 0 && err == nil {
			return nil
		}
		if err == nil {
			continue
		}
		if err == syscall.EINTR {
			continue
		}
		if err == syscall.EAGAIN {
			continue
		}
		return nil
	}
}
