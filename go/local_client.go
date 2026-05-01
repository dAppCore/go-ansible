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
//	result := c.Close()
func (c *localClient) Close() core.Result {
	return core.Ok(nil)
}

// Run executes cmd via local shell, optionally wrapped in sudo when
// become is enabled.
//
//	result := c.Run(ctx, "uname -a")
func (c *localClient) Run(ctx context.Context, cmd string) core.Result {
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
//	result := c.RunScript(ctx, "#!/bin/bash\necho hi")
func (c *localClient) RunScript(ctx context.Context, script string) core.Result {
	return c.Run(ctx, "bash <<'ANSIBLE_SCRIPT_EOF'\n"+script+"\nANSIBLE_SCRIPT_EOF")
}

// Upload writes the contents of localReader into a file at remote with the
// given permission mode. Creates parent directories as needed.
//
//	result := c.Upload(ctx, bytes.NewReader(data), "/etc/foo.conf", 0o644)
func (c *localClient) Upload(
	_ context.Context, localReader io.Reader, remote string, mode core.FileMode,
) core.Result {
	read := core.ReadAll(localReader)
	if !read.OK {
		return wrapFailure(read, "localClient.Upload", "read upload content")
	}
	content := []byte(read.Value.(string))

	if r := core.MkdirAll(pathDir(remote), 0o755); !r.OK {
		return wrapFailure(r, "localClient.Upload", "create remote directory")
	}
	if r := core.WriteFile(remote, content, mode); !r.OK {
		return wrapFailure(r, "localClient.Upload", "write remote file")
	}
	return core.Ok(nil)
}

// Download reads the bytes of a local file path. Despite the name, the
// "remote" is local — the localClient implements the SSHClient interface
// transparently so playbooks targeting `connection: local` work unchanged.
//
//	result := c.Download(ctx, "/etc/foo.conf")
func (c *localClient) Download(_ context.Context, remote string) core.Result {
	read := core.ReadFile(remote)
	if !read.OK {
		return wrapFailure(read, "localClient.Download", "read remote file")
	}
	return core.Ok(read.Value.([]byte))
}

// FileExists reports whether the given local path exists. Returns
// false for non-existent paths and a failed Result for stat failures
// other than not-exist.
//
//	result := c.FileExists(ctx, "/etc/foo.conf")
func (c *localClient) FileExists(_ context.Context, path string) core.Result {
	stat := core.Stat(path)
	if stat.OK {
		return core.Ok(true)
	}
	err, _ := stat.Value.(error)
	if core.IsNotExist(err) {
		return core.Ok(false)
	}
	return core.Fail(coreerr.E("localClient.FileExists", "stat path", err))
}

// Stat returns a map describing the path: at minimum {"exists": bool}, plus
// "isdir" / "size" / "mode" / "mtime" when the file exists. Used by the
// `stat` Ansible module.
//
//	result := c.Stat(ctx, "/etc/foo.conf")
func (c *localClient) Stat(_ context.Context, path string) core.Result {
	stat := core.Stat(path)
	if !stat.OK {
		err, _ := stat.Value.(error)
		if core.IsNotExist(err) {
			return core.Ok(map[string]any{"exists": false})
		}
		return core.Fail(coreerr.E("localClient.Stat", "stat path", err))
	}
	info := stat.Value.(core.FsFileInfo)
	return core.Ok(map[string]any{
		"exists": true,
		"isdir":  info.IsDir(),
	})
}

func (c *localClient) becomeStateLocked() (bool, string, string) {
	return c.become, c.becomeUser, c.becomePass
}

func runLocalShell(ctx context.Context, command, password string) core.Result {
	stdinResult := makePipe()
	if !stdinResult.OK {
		return wrapFailure(stdinResult, "localClient.runLocalShell", "open stdin")
	}
	stdinPipe := stdinResult.Value.([2]int)
	stdoutResult := makePipe()
	if !stdoutResult.OK {
		closePipe(stdinPipe)
		return wrapFailure(stdoutResult, "localClient.runLocalShell", "open stdout")
	}
	stdoutPipe := stdoutResult.Value.([2]int)
	stderrResult := makePipe()
	if !stderrResult.OK {
		closePipe(stdinPipe)
		closePipe(stdoutPipe)
		return wrapFailure(stderrResult, "localClient.runLocalShell", "open stderr")
	}
	stderrPipe := stderrResult.Value.([2]int)

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
		return core.Fail(coreerr.E("localClient.runLocalShell", "execute command", err))
	}

	if password != "" {
		go func() {
			defer closeFD(stdinPipe[1])
			if _, writeErr := syscall.Write(stdinPipe[1], []byte(password+"\n")); writeErr != nil {
				return
			}
		}()
	} else {
		closeFD(stdinPipe[1])
	}

	stdoutBuf := core.NewBuffer()
	stderrBuf := core.NewBuffer()
	doneOut := make(chan core.Result, 1)
	doneErr := make(chan core.Result, 1)
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
			return commandRunFail(stdoutBuf.String(), stderrBuf.String(), -1, killErr)
		}
		wait = <-waitCh
		<-doneOut
		<-doneErr
		return commandRunFail(stdoutBuf.String(), stderrBuf.String(), -1, ctx.Err())
	case wait = <-waitCh:
	}

	<-doneOut
	<-doneErr
	if wait.err != nil {
		return commandRunFail(stdoutBuf.String(), stderrBuf.String(), -1, coreerr.E("localClient.runLocalShell", "wait command", wait.err))
	}
	if wait.status.Exited() {
		return commandRunOK(stdoutBuf.String(), stderrBuf.String(), wait.status.ExitStatus())
	}
	if wait.status.Signaled() {
		return commandRunOK(stdoutBuf.String(), stderrBuf.String(), 128+int(wait.status.Signal()))
	}
	return commandRunOK(stdoutBuf.String(), stderrBuf.String(), -1)
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

func makePipe() core.Result {
	var pipe [2]int
	err := syscall.Pipe(pipe[:])
	if err != nil {
		return core.Fail(err)
	}
	return core.Ok(pipe)
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
) core.Result {
	defer closeFD(fd)
	tmp := make([]byte, 32*1024)
	for {
		n, err := syscall.Read(fd, tmp)
		if n > 0 {
			if _, writeErr := buf.Write(tmp[:n]); writeErr != nil {
				return core.Fail(writeErr)
			}
		}
		if n == 0 && err == nil {
			return core.Ok(nil)
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
		return core.Ok(nil)
	}
}
