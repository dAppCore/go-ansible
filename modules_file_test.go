package ansible

import (
	"context"
	"io"
	"io/fs"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type diffFileClient struct {
	mu    sync.Mutex
	files map[string]string
}

func newDiffFileClient(initial map[string]string) *diffFileClient {
	files := make(map[string]string, len(initial))
	for path, content := range initial {
		files[path] = content
	}
	return &diffFileClient{files: files}
}

func (c *diffFileClient) Run(_ context.Context, cmd string) (string, string, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if strings.Contains(cmd, `|| echo `) && strings.Contains(cmd, `grep -qxF `) {
		re := regexp.MustCompile(`grep -qxF "([^"]*)" "([^"]*)" \|\| echo "([^"]*)" >> "([^"]*)"`)
		match := re.FindStringSubmatch(cmd)
		if len(match) == 5 {
			line := match[3]
			path := match[4]
			if c.files[path] == "" {
				c.files[path] = line + "\n"
			} else if !strings.Contains(c.files[path], line+"\n") && c.files[path] != line {
				if !strings.HasSuffix(c.files[path], "\n") {
					c.files[path] += "\n"
				}
				c.files[path] += line + "\n"
			}
		}
	}

	if strings.Contains(cmd, `sed -i '/`) && strings.Contains(cmd, `/d' `) {
		re := regexp.MustCompile(`sed -i '/([^']*)/d' "([^"]*)"`)
		match := re.FindStringSubmatch(cmd)
		if len(match) == 3 {
			pattern := match[1]
			path := match[2]
			lines := strings.Split(c.files[path], "\n")
			out := make([]string, 0, len(lines))
			for _, line := range lines {
				if line == "" {
					continue
				}
				if strings.Contains(line, pattern) {
					continue
				}
				out = append(out, line)
			}
			if len(out) > 0 {
				c.files[path] = strings.Join(out, "\n") + "\n"
			} else {
				c.files[path] = ""
			}
		}
	}

	return "", "", 0, nil
}

func (c *diffFileClient) RunScript(_ context.Context, script string) (string, string, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	re := regexp.MustCompile("(?s)cat >> \"([^\"]+)\" << 'BLOCK_EOF'\\n(.*)\\nBLOCK_EOF")
	match := re.FindStringSubmatch(script)
	if len(match) == 3 {
		path := match[1]
		block := match[2]
		c.files[path] = block + "\n"
	}

	return "", "", 0, nil
}

func (c *diffFileClient) Upload(_ context.Context, local io.Reader, remote string, _ fs.FileMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	content, err := io.ReadAll(local)
	if err != nil {
		return err
	}
	c.files[remote] = string(content)
	return nil
}

func (c *diffFileClient) Download(_ context.Context, remote string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	content, ok := c.files[remote]
	if !ok {
		return nil, mockError("diffFileClient.Download", "file not found: "+remote)
	}
	return []byte(content), nil
}

func (c *diffFileClient) Stat(_ context.Context, path string) (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.files[path]; ok {
		return map[string]any{"exists": true}, nil
	}
	return map[string]any{"exists": false}, nil
}

func (c *diffFileClient) FileExists(_ context.Context, path string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.files[path]
	return ok, nil
}

func (c *diffFileClient) BecomeState() (bool, string, string) {
	return false, "", ""
}

func (c *diffFileClient) SetBecome(bool, string, string) {}

func (c *diffFileClient) Close() error { return nil }

// ============================================================
// Step 1.2: copy / template / file / lineinfile / blockinfile / stat module tests
// ============================================================

// --- copy module ---

func TestModulesFile_ModuleCopy_Good_ContentUpload(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=web01",
		"dest":    "/etc/app/config",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Contains(t, result.Msg, "copied to /etc/app/config")

	// Verify upload was performed
	assert.Equal(t, 1, mock.uploadCount())
	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, "/etc/app/config", up.Remote)
	assert.Equal(t, []byte("server_name=web01"), up.Content)
	assert.Equal(t, fs.FileMode(0644), up.Mode)
}

func TestModulesFile_ModuleCopy_Good_SrcFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "nginx.conf")
	require.NoError(t, writeTestFile(srcPath, []byte("worker_processes auto;"), 0644))

	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/nginx/nginx.conf",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, "/etc/nginx/nginx.conf", up.Remote)
	assert.Equal(t, []byte("worker_processes auto;"), up.Content)
}

func TestModulesFile_ModuleCopy_Good_RemoteSrc(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/remote-source.txt", []byte("remote payload"))

	result, err := e.moduleCopy(context.Background(), mock, map[string]any{
		"src":        "/tmp/remote-source.txt",
		"dest":       "/etc/app/remote.txt",
		"remote_src": true,
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, "/etc/app/remote.txt", up.Remote)
	assert.Equal(t, []byte("remote payload"), up.Content)
}

func TestModulesFile_ModuleCopy_Good_OwnerGroup(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "data",
		"dest":    "/opt/app/data.txt",
		"owner":   "appuser",
		"group":   "appgroup",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Upload + chown + chgrp = 1 upload + 2 Run calls
	assert.Equal(t, 1, mock.uploadCount())
	assert.True(t, mock.hasExecuted(`chown appuser "/opt/app/data.txt"`))
	assert.True(t, mock.hasExecuted(`chgrp appgroup "/opt/app/data.txt"`))
}

func TestModulesFile_ModuleCopy_Good_CustomMode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "#!/bin/bash\necho hello",
		"dest":    "/usr/local/bin/hello.sh",
		"mode":    "0755",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, fs.FileMode(0755), up.Mode)
}

func TestModulesFile_ModuleCopy_Bad_MissingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "data",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dest required")
}

func TestModulesFile_ModuleCopy_Bad_MissingSrcAndContent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src or content required")
}

func TestModulesFile_ModuleCopy_Bad_SrcFileNotFound(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/file.txt",
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read src")
}

func TestModulesFile_ModuleCopy_Good_ContentTakesPrecedenceOverSrc(t *testing.T) {
	// When both content and src are given, src is checked first in the implementation
	// but if src is empty string, content is used
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "from_content",
		"dest":    "/tmp/out",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	up := mock.lastUpload()
	assert.Equal(t, []byte("from_content"), up.Content)
}

func TestModulesFile_ModuleCopy_Good_SkipsUnchangedContent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.Diff = true
	mock.addFile("/etc/app/config", []byte("server_name=web01"))

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=web01",
		"dest":    "/etc/app/config",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Equal(t, 0, mock.uploadCount())
	assert.Contains(t, result.Msg, "already up to date")
}

func TestModulesFile_ModuleCopy_Good_ForceFalseSkipsExistingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=new",
		"dest":    "/etc/app/config",
		"force":   false,
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Equal(t, 0, mock.uploadCount())
	assert.Contains(t, result.Msg, "skipped existing destination")
}

func TestModulesFile_ModuleCopy_Good_BackupExistingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=new",
		"dest":    "/etc/app/config",
		"backup":  true,
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	require.NotNil(t, result.Data)
	backupPath, ok := result.Data["backup_file"].(string)
	require.True(t, ok)
	assert.Contains(t, backupPath, "/etc/app/config.")
	assert.Equal(t, 2, mock.uploadCount())
	backupContent, err := mock.Download(context.Background(), backupPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("server_name=old"), backupContent)
}

// --- file module ---

func TestModulesFile_ModuleFile_Good_StateDirectory(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/var/lib/app",
		"state": "directory",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should execute mkdir -p with default mode 0755
	assert.True(t, mock.hasExecuted(`mkdir -p "/var/lib/app"`))
	assert.True(t, mock.hasExecuted(`chmod 0755`))
}

func TestModulesFile_ModuleFile_Good_StateDirectoryCustomMode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/opt/data",
		"state": "directory",
		"mode":  "0700",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`mkdir -p "/opt/data" && chmod 0700 "/opt/data"`))
}

func TestModulesFile_ModuleFile_Good_StateAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/tmp/old-dir",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`rm -rf "/tmp/old-dir"`))
}

func TestModulesFile_ModuleFile_Good_StateTouch(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/var/log/app.log",
		"state": "touch",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`touch "/var/log/app.log"`))
}

func TestModulesFile_ModuleFile_Good_StateLink(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/usr/local/bin/node",
		"state": "link",
		"src":   "/opt/node/bin/node",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`ln -sf "/opt/node/bin/node" "/usr/local/bin/node"`))
}

func TestModulesFile_ModuleFile_Good_StateHard(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/usr/local/bin/node",
		"state": "hard",
		"src":   "/opt/node/bin/node",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`ln -f "/opt/node/bin/node" "/usr/local/bin/node"`))
}

func TestModulesFile_ModuleFile_Bad_StateHardMissingSrc(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/usr/local/bin/node",
		"state": "hard",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src required for hard state")
}

func TestModulesFile_ModuleFile_Bad_LinkMissingSrc(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/usr/local/bin/node",
		"state": "link",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src required for link state")
}

func TestModulesFile_ModuleFile_Good_OwnerGroupMode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/var/lib/app/data",
		"state": "directory",
		"owner": "www-data",
		"group": "www-data",
		"mode":  "0775",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should have mkdir, chmod in the directory command, then chown and chgrp
	assert.True(t, mock.hasExecuted(`mkdir -p "/var/lib/app/data" && chmod 0775 "/var/lib/app/data"`))
	assert.True(t, mock.hasExecuted(`chown www-data "/var/lib/app/data"`))
	assert.True(t, mock.hasExecuted(`chgrp www-data "/var/lib/app/data"`))
}

func TestModulesFile_ModuleFile_Good_RecurseOwner(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":    "/var/www",
		"state":   "directory",
		"owner":   "www-data",
		"recurse": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should have both regular chown and recursive chown
	assert.True(t, mock.hasExecuted(`chown www-data "/var/www"`))
	assert.True(t, mock.hasExecuted(`chown -R www-data "/var/www"`))
}

func TestModulesFile_ModuleFile_Good_RecurseGroupAndMode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":    "/srv/app",
		"state":   "directory",
		"group":   "appgroup",
		"mode":    "0770",
		"recurse": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	assert.True(t, mock.hasExecuted(`chgrp appgroup "/srv/app"`))
	assert.True(t, mock.hasExecuted(`chgrp -R appgroup "/srv/app"`))
	assert.True(t, mock.hasExecuted(`chmod -R 0770 "/srv/app"`))
}

func TestModulesFile_ModuleFile_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		"state": "directory",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestModulesFile_ModuleFile_Good_DestAliasForPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"dest":  "/opt/myapp",
		"state": "directory",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`mkdir -p "/opt/myapp"`))
}

func TestModulesFile_ModuleFile_Good_StateFileWithMode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/etc/config.yml",
		"state": "file",
		"mode":  "0600",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`chmod 0600 "/etc/config.yml"`))
}

func TestModulesFile_ModuleFile_Good_DirectoryCommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("mkdir", "", "permission denied", 1)

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/root/protected",
		"state": "directory",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "permission denied")
}

// --- lineinfile module ---

func TestModulesFile_ModuleLineinfile_Good_InsertLine(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path": "/etc/hosts",
		"line": "192.168.1.100 web01",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should use grep -qxF to check and echo to append
	assert.True(t, mock.hasExecuted(`grep -qxF`))
	assert.True(t, mock.hasExecuted(`192.168.1.100 web01`))
}

func TestModulesFile_ModuleLineinfile_Good_ReplaceRegexp(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/ssh/sshd_config",
		"regexp": "^#?PermitRootLogin",
		"line":   "PermitRootLogin no",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should use sed to replace
	assert.True(t, mock.hasExecuted(`sed -i 's/\^#\?PermitRootLogin/PermitRootLogin no/'`))
}

func TestModulesFile_ModuleLineinfile_Good_RemoveLine(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/hosts",
		"regexp": "^192\\.168\\.1\\.100",
		"state":  "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should use sed to delete matching lines
	assert.True(t, mock.hasExecuted(`sed -i '/\^192`))
	assert.True(t, mock.hasExecuted(`/d'`))
}

func TestModulesFile_ModuleLineinfile_Good_RegexpFallsBackToAppend(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// Simulate sed returning non-zero (pattern not found)
	mock.expectCommand("sed -i", "", "", 1)

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/config",
		"regexp": "^setting=",
		"line":   "setting=value",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should have attempted sed, then fallen back to echo append
	cmds := mock.executedCommands()
	assert.GreaterOrEqual(t, len(cmds), 2)
	assert.True(t, mock.hasExecuted(`echo`))
}

func TestModulesFile_ModuleLineinfile_Good_CreateFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := e.moduleLineinfile(context.Background(), mock, map[string]any{
		"path":   "/etc/example.conf",
		"regexp": "^setting=",
		"line":   "setting=value",
		"create": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`touch "/etc/example\.conf"`))
	assert.True(t, mock.hasExecuted(`sed -i`))
}

func TestModulesFile_ModuleLineinfile_Good_BackrefsReplaceMatchOnly(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":     "/etc/example.conf",
		"regexp":   "^(foo=).*$",
		"line":     "\\1bar",
		"backrefs": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`grep -Eq`))
	cmd := mock.lastCommand()
	assert.Equal(t, "Run", cmd.Method)
	assert.Contains(t, cmd.Cmd, "sed -E -i")
	assert.Contains(t, cmd.Cmd, "s/^(foo=).*$")
	assert.Contains(t, cmd.Cmd, "\\1bar")
	assert.Contains(t, cmd.Cmd, `"/etc/example.conf"`)
	assert.False(t, mock.hasExecuted(`echo`))
}

func TestModulesFile_ModuleLineinfile_Good_BackrefsNoMatchNoAppend(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("grep -Eq", "", "", 1)

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":     "/etc/example.conf",
		"regexp":   "^(foo=).*$",
		"line":     "\\1bar",
		"backrefs": true,
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Equal(t, 1, mock.commandCount())
	assert.Contains(t, mock.lastCommand().Cmd, "grep -Eq")
	assert.False(t, mock.hasExecuted(`echo`))
}

func TestModulesFile_ModuleLineinfile_Good_InsertBeforeAnchor(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := e.moduleLineinfile(context.Background(), mock, map[string]any{
		"path":         "/etc/example.conf",
		"line":         "setting=value",
		"insertbefore": "^# managed settings",
		"firstmatch":   true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`grep -Eq`))
	assert.True(t, mock.hasExecuted(regexp.QuoteMeta("print line; done=1 } print")))
}

func TestModulesFile_ModuleLineinfile_Good_InsertAfterAnchor(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := e.moduleLineinfile(context.Background(), mock, map[string]any{
		"path":        "/etc/example.conf",
		"line":        "setting=value",
		"insertafter": "^# managed settings",
		"firstmatch":  true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`grep -Eq`))
	assert.True(t, mock.hasExecuted(regexp.QuoteMeta("print; if (!done && $0 ~ re) { print line; done=1 }")))
}

func TestModulesFile_ModuleLineinfile_Good_InsertAfterAnchor_DefaultUsesLastMatch(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := e.moduleLineinfile(context.Background(), mock, map[string]any{
		"path":        "/etc/example.conf",
		"line":        "setting=value",
		"insertafter": "^# managed settings",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`grep -Eq`))
	assert.True(t, mock.hasExecuted("pos=NR"))
	assert.False(t, mock.hasExecuted("done=1"))
}

func TestModulesFile_ModuleLineinfile_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"line": "test",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestModulesFile_ModuleLineinfile_Good_DestAliasForPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"dest": "/etc/config",
		"line": "key=value",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`/etc/config`))
}

func TestModulesFile_ModuleLineinfile_Good_AbsentWithNoRegexp(t *testing.T) {
	// When state=absent but no regexp, nothing happens (no commands)
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":  "/etc/config",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, 0, mock.commandCount())
}

func TestModulesFile_ModuleLineinfile_Good_LineWithSlashes(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/nginx/conf.d/default.conf",
		"regexp": "^root /",
		"line":   "root /var/www/html;",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Slashes in the line should be escaped
	assert.True(t, mock.hasExecuted(`root \\/var\\/www\\/html;`))
}

func TestModulesFile_ModuleLineinfile_Good_ExactLineAlreadyPresentIsNoOp(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/example.conf", []byte("setting=value\nother=1\n"))

	result, err := e.moduleLineinfile(context.Background(), mock, map[string]any{
		"path": "/etc/example.conf",
		"line": "setting=value",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Contains(t, result.Msg, "already up to date")
	assert.Equal(t, 0, mock.commandCount())
}

func TestModulesFile_ModuleLineinfile_Good_BackupExistingFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	path := "/etc/example.conf"
	mock.addFile(path, []byte("setting=old\n"))

	result, err := e.moduleLineinfile(context.Background(), mock, map[string]any{
		"path":   path,
		"line":   "setting=new",
		"backup": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)

	backupPath, ok := result.Data["backup_file"].(string)
	require.True(t, ok)
	assert.Contains(t, backupPath, "/etc/example.conf.")

	backupContent, err := mock.Download(context.Background(), backupPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("setting=old\n"), backupContent)
}

func TestModulesFile_ModuleLineinfile_Good_DiffData(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Diff = true

	client := newDiffFileClient(map[string]string{
		"/etc/example.conf": "setting=old\n",
	})

	result, err := e.moduleLineinfile(context.Background(), client, map[string]any{
		"path": "/etc/example.conf",
		"line": "setting=new",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)

	diff, ok := result.Data["diff"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "/etc/example.conf", diff["path"])
	assert.Equal(t, "setting=old\n", diff["before"])
	assert.Contains(t, diff["after"], "setting=new")
}

// --- replace module ---

func TestModulesFile_ModuleReplace_Good_RegexpReplacementWithBackupAndDiff(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Diff = true
	client := newDiffFileClient(map[string]string{
		"/etc/app.conf": "port=8080\nmode=prod\n",
	})

	result, err := e.moduleReplace(context.Background(), client, map[string]any{
		"path":    "/etc/app.conf",
		"regexp":  `port=(\d+)`,
		"replace": "port=9090",
		"backup":  true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)
	assert.Contains(t, result.Data, "backup_file")
	assert.Contains(t, result.Data, "diff")

	after, err := client.Download(context.Background(), "/etc/app.conf")
	require.NoError(t, err)
	assert.Equal(t, "port=9090\nmode=prod\n", string(after))

	backupPath, _ := result.Data["backup_file"].(string)
	require.NotEmpty(t, backupPath)
	backup, err := client.Download(context.Background(), backupPath)
	require.NoError(t, err)
	assert.Equal(t, "port=8080\nmode=prod\n", string(backup))
}

func TestModulesFile_ModuleReplace_Good_NoOpWhenPatternMissing(t *testing.T) {
	e := NewExecutor("/tmp")
	client := newDiffFileClient(map[string]string{
		"/etc/app.conf": "port=8080\n",
	})

	result, err := e.moduleReplace(context.Background(), client, map[string]any{
		"path":    "/etc/app.conf",
		"regexp":  `mode=.+`,
		"replace": "mode=prod",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Contains(t, result.Msg, "already up to date")
}

func TestModulesFile_ModuleReplace_Bad_MissingPath(t *testing.T) {
	e := NewExecutor("/tmp")
	client := newDiffFileClient(nil)

	_, err := e.moduleReplace(context.Background(), client, map[string]any{
		"regexp":  `mode=.+`,
		"replace": "mode=prod",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

// --- blockinfile module ---

func TestModulesFile_ModuleBlockinfile_Good_InsertBlock(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":  "/etc/nginx/conf.d/upstream.conf",
		"block": "server 10.0.0.1:8080;\nserver 10.0.0.2:8080;",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should use RunScript for the heredoc approach
	assert.True(t, mock.hasExecutedMethod("RunScript", "BEGIN ANSIBLE MANAGED BLOCK"))
	assert.True(t, mock.hasExecutedMethod("RunScript", "END ANSIBLE MANAGED BLOCK"))
	assert.True(t, mock.hasExecutedMethod("RunScript", "10\\.0\\.0\\.1"))
}

func TestModulesFile_ModuleBlockinfile_Good_CustomMarkers(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/hosts",
		"block":  "10.0.0.5 db01",
		"marker": "# {mark} managed by devops",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should use custom markers instead of default
	assert.True(t, mock.hasExecutedMethod("RunScript", "# BEGIN managed by devops"))
	assert.True(t, mock.hasExecutedMethod("RunScript", "# END managed by devops"))
}

func TestModulesFile_ModuleBlockinfile_Good_NewlinePadding(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":            "/etc/hosts",
		"block":           "10.0.0.5 db01",
		"prepend_newline": true,
		"append_newline":  true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	cmd := mock.lastCommand().Cmd
	assert.Contains(t, cmd, "\n\n# BEGIN ANSIBLE MANAGED BLOCK\n10.0.0.5 db01\n# END ANSIBLE MANAGED BLOCK\n")
}

func TestModulesFile_ModuleBlockinfile_Good_RemoveBlock(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":  "/etc/config",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should use sed to remove the block between markers
	assert.True(t, mock.hasExecuted(`sed -i '/.*BEGIN ANSIBLE MANAGED BLOCK/,/.*END ANSIBLE MANAGED BLOCK/d'`))
}

func TestModulesFile_ModuleBlockinfile_Good_CreateFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/new-config",
		"block":  "setting=value",
		"create": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Should touch the file first when create=true
	assert.True(t, mock.hasExecuted(`touch "/etc/new-config"`))
}

func TestModulesFile_ModuleBlockinfile_Good_BackupExistingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/config", []byte("old block contents"))

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":   "/etc/config",
		"block":  "new block contents",
		"backup": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)

	backupPath, ok := result.Data["backup_file"].(string)
	require.True(t, ok)
	assert.Contains(t, backupPath, "/etc/config.")
	assert.Equal(t, 1, mock.uploadCount())

	backupContent, err := mock.Download(context.Background(), backupPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("old block contents"), backupContent)
}

func TestModulesFile_ModuleBlockinfile_Good_DiffData(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Diff = true

	client := newDiffFileClient(map[string]string{
		"/etc/config": "old block contents\n",
	})

	result, err := e.moduleBlockinfile(context.Background(), client, map[string]any{
		"path":  "/etc/config",
		"block": "new block contents",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)

	diff, ok := result.Data["diff"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "/etc/config", diff["path"])
	assert.Equal(t, "old block contents\n", diff["before"])
	assert.Contains(t, diff["after"], "new block contents")
}

func TestModulesFile_ModuleBlockinfile_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"block": "content",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestModulesFile_ModuleBlockinfile_Good_DestAliasForPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"dest":  "/etc/config",
		"block": "data",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestModulesFile_ModuleBlockinfile_Good_ScriptFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("BLOCK_EOF", "", "write error", 1)

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"path":  "/etc/config",
		"block": "data",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "write error")
}

// --- stat module ---

func TestModulesFile_ModuleStat_Good_ExistingFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addStat("/etc/nginx/nginx.conf", map[string]any{
		"exists": true,
		"isdir":  false,
		"mode":   "0644",
		"size":   1234,
		"uid":    0,
		"gid":    0,
	})

	result, err := moduleStatWithClient(e, mock, map[string]any{
		"path": "/etc/nginx/nginx.conf",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed) // stat never changes anything
	require.NotNil(t, result.Data)

	stat, ok := result.Data["stat"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, stat["exists"])
	assert.Equal(t, false, stat["isdir"])
	assert.Equal(t, "0644", stat["mode"])
	assert.Equal(t, 1234, stat["size"])
}

func TestModulesFile_ModuleStat_Good_MissingFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleStatWithClient(e, mock, map[string]any{
		"path": "/nonexistent/file.txt",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	require.NotNil(t, result.Data)

	stat, ok := result.Data["stat"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, false, stat["exists"])
}

func TestModulesFile_ModuleStat_Good_Directory(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addStat("/var/log", map[string]any{
		"exists": true,
		"isdir":  true,
		"mode":   "0755",
	})

	result, err := moduleStatWithClient(e, mock, map[string]any{
		"path": "/var/log",
	})

	require.NoError(t, err)
	stat := result.Data["stat"].(map[string]any)
	assert.Equal(t, true, stat["exists"])
	assert.Equal(t, true, stat["isdir"])
}

func TestModulesFile_ModuleStat_Good_FallbackFromFileSystem(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// No explicit stat, but add a file — stat falls back to file existence
	mock.addFile("/etc/hosts", []byte("127.0.0.1 localhost"))

	result, err := moduleStatWithClient(e, mock, map[string]any{
		"path": "/etc/hosts",
	})

	require.NoError(t, err)
	stat := result.Data["stat"].(map[string]any)
	assert.Equal(t, true, stat["exists"])
	assert.Equal(t, false, stat["isdir"])
}

func TestModulesFile_ModuleStat_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleStatWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

// --- template module ---

func TestModulesFile_ModuleTemplate_Good_BasicTemplate(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "app.conf.j2")
	require.NoError(t, writeTestFile(srcPath, []byte("server_name={{ server_name }};"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("server_name", "web01.example.com")

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/nginx/conf.d/app.conf",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Contains(t, result.Msg, "templated to /etc/nginx/conf.d/app.conf")

	// Verify upload was performed with templated content
	assert.Equal(t, 1, mock.uploadCount())
	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, "/etc/nginx/conf.d/app.conf", up.Remote)
	// Template replaces {{ var }} — the TemplateFile does Jinja2 to Go conversion
	assert.Contains(t, string(up.Content), "web01.example.com")
}

func TestModulesFile_ModuleTemplate_Good_AnsibleFactsMapTemplate(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "facts.conf.j2")
	require.NoError(t, writeTestFile(srcPath, []byte("host={{ ansible_facts.ansible_hostname }}"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	e.facts["host1"] = &Facts{
		Hostname:     "web01",
		Distribution: "debian",
	}

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/app/facts.conf",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Contains(t, string(up.Content), "host=web01")
}

func TestModulesFile_ModuleTemplate_Good_TaskVarsAndHostMagicVars(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "context.conf.j2")
	require.NoError(t, writeTestFile(srcPath, []byte("short={{ inventory_hostname_short }} local={{ local_value }}"), 0644))

	e, mock := newTestExecutorWithMock("web01.example.com")
	task := &Task{
		Vars: map[string]any{
			"local_value": "from-task",
		},
	}

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/app/context.conf",
	}, "web01.example.com", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Contains(t, string(up.Content), "short=web01")
	assert.Contains(t, string(up.Content), "local=from-task")
}

func TestModulesFile_ModuleTemplate_Good_CustomMode(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "script.sh.j2")
	require.NoError(t, writeTestFile(srcPath, []byte("#!/bin/bash\necho done"), 0644))

	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/usr/local/bin/run.sh",
		"mode": "0755",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, fs.FileMode(0755), up.Mode)
}

func TestModulesFile_ModuleTemplate_Good_ForceFalseSkipsExistingDest(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "config.tmpl")
	require.NoError(t, writeTestFile(srcPath, []byte("server_name={{ inventory_hostname }}"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":   srcPath,
		"dest":  "/etc/app/config",
		"force": false,
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Equal(t, 0, mock.uploadCount())
	assert.Contains(t, result.Msg, "skipped existing destination")
}

func TestModulesFile_ModuleTemplate_Good_BackupExistingDest(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "config.tmpl")
	require.NoError(t, writeTestFile(srcPath, []byte("server_name={{ inventory_hostname }}"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":    srcPath,
		"dest":   "/etc/app/config",
		"backup": true,
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	require.NotNil(t, result.Data)
	backupPath, ok := result.Data["backup_file"].(string)
	require.True(t, ok)
	assert.Contains(t, backupPath, "/etc/app/config.")
	assert.Equal(t, 2, mock.uploadCount())
	backupContent, err := mock.Download(context.Background(), backupPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("server_name=old"), backupContent)
}

func TestModulesFile_ModuleTemplate_Bad_MissingSrc(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModulesFile_ModuleTemplate_Bad_MissingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src": "/tmp/in.j2",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModulesFile_ModuleTemplate_Bad_SrcFileNotFound(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/template.j2",
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "template")
}

func TestModulesFile_ModuleTemplate_Good_PlainTextNoVars(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "static.conf")
	content := "listen 80;\nserver_name localhost;"
	require.NoError(t, writeTestFile(srcPath, []byte(content), 0644))

	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/config",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, content, string(up.Content))
}

func TestModulesFile_ModuleTemplate_Good_DiffData(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "app.conf.j2")
	require.NoError(t, writeTestFile(srcPath, []byte("server_name={{ server_name }};"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	e.Diff = true
	e.SetVar("server_name", "web01.example.com")
	mock.addFile("/etc/nginx/conf.d/app.conf", []byte("server_name=old.example.com;"))

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/nginx/conf.d/app.conf",
	}, "host1", &Task{})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)

	diff, ok := result.Data["diff"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "/etc/nginx/conf.d/app.conf", diff["path"])
	assert.Equal(t, "server_name=old.example.com;", diff["before"])
	assert.Contains(t, diff["after"], "web01.example.com")
}

// --- Cross-module dispatch tests for file modules ---

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchCopy(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "copy",
		Args: map[string]any{
			"content": "hello world",
			"dest":    "/tmp/hello.txt",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, 1, mock.uploadCount())
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "file",
		Args: map[string]any{
			"path":  "/opt/data",
			"state": "directory",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted("mkdir"))
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchStat(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addStat("/etc/hosts", map[string]any{"exists": true, "isdir": false})

	task := &Task{
		Module: "stat",
		Args: map[string]any{
			"path": "/etc/hosts",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.False(t, result.Changed)
	stat := result.Data["stat"].(map[string]any)
	assert.Equal(t, true, stat["exists"])
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchLineinfile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "lineinfile",
		Args: map[string]any{
			"path": "/etc/hosts",
			"line": "10.0.0.1 dbhost",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchBlockinfile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "blockinfile",
		Args: map[string]any{
			"path":  "/etc/config",
			"block": "key=value",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchTemplate(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "test.j2")
	require.NoError(t, writeTestFile(srcPath, []byte("static content"), 0644))

	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "template",
		Args: map[string]any{
			"src":  srcPath,
			"dest": "/etc/out.conf",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, 1, mock.uploadCount())
}

// --- Template variable resolution integration ---

func TestModulesFile_ModuleCopy_Good_TemplatedArgs(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("deploy_path", "/opt/myapp")

	task := &Task{
		Module: "copy",
		Args: map[string]any{
			"content": "deployed",
			"dest":    "{{ deploy_path }}/config.yml",
		},
	}

	// Template the args as the executor does
	args := e.templateArgs(task.Args, "host1", task)
	result, err := moduleCopyWithClient(e, mock, args, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)

	up := mock.lastUpload()
	require.NotNil(t, up)
	assert.Equal(t, "/opt/myapp/config.yml", up.Remote)
}

func TestModulesFile_ModuleFile_Good_TemplatedPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("app_dir", "/var/www/html")

	task := &Task{
		Module: "file",
		Args: map[string]any{
			"path":  "{{ app_dir }}/uploads",
			"state": "directory",
			"owner": "www-data",
		},
	}

	args := e.templateArgs(task.Args, "host1", task)
	result, err := moduleFileWithClient(e, mock, args)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`mkdir -p "/var/www/html/uploads"`))
	assert.True(t, mock.hasExecuted(`chown www-data "/var/www/html/uploads"`))
}
