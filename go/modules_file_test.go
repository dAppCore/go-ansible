package ansible

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	core "dappco.re/go"
	"encoding/hex"
	"io"
	"io/fs"
	"regexp"
	"sync"
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

func (c *diffFileClient) Run(_ context.Context, cmd string) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	if contains(cmd, `|| echo `) && contains(cmd, `grep -qxF `) {
		re := regexp.MustCompile(`grep -qxF "([^"]*)" "([^"]*)" \|\| echo "([^"]*)" >> "([^"]*)"`)
		match := re.FindStringSubmatch(cmd)
		if len(match) == 5 {
			line := match[3]
			path := match[4]
			if c.files[path] == "" {
				c.files[path] = line + "\n"
			} else if !contains(c.files[path], line+"\n") && c.files[path] != line {
				if !hasSuffix(c.files[path], "\n") {
					c.files[path] += "\n"
				}
				c.files[path] += line + "\n"
			}
		}
	}

	if contains(cmd, `sed -i '/`) && contains(cmd, `/d' `) {
		re := regexp.MustCompile(`sed -i '/([^']*)/d' "([^"]*)"`)
		match := re.FindStringSubmatch(cmd)
		if len(match) == 3 {
			pattern := match[1]
			path := match[2]
			lines := split(c.files[path], "\n")
			out := make([]string, 0, len(lines))
			for _, line := range lines {
				if line == "" {
					continue
				}
				if contains(line, pattern) {
					continue
				}
				out = append(out, line)
			}
			if len(out) > 0 {
				c.files[path] = join("\n", out) + "\n"
			} else {
				c.files[path] = ""
			}
		}
	}

	return commandRunOK("", "", 0)
}

func (c *diffFileClient) RunScript(_ context.Context, script string) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	re := regexp.MustCompile("(?s)cat >> \"([^\"]+)\" << 'BLOCK_EOF'\\n(.*)\\nBLOCK_EOF")
	match := re.FindStringSubmatch(script)
	if len(match) == 3 {
		path := match[1]
		block := match[2]
		c.files[path] = block + "\n"
	}

	return commandRunOK("", "", 0)
}

func (c *diffFileClient) Upload(_ context.Context, local io.Reader, remote string, _ fs.FileMode) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	content, err := io.ReadAll(local)
	if err != nil {
		return core.Fail(err)
	}
	c.files[remote] = string(content)
	return core.Ok(nil)
}

func (c *diffFileClient) Download(_ context.Context, remote string) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	content, ok := c.files[remote]
	if !ok {
		return core.Fail(mockError("diffFileClient.Download", "file not found: "+remote))
	}
	return core.Ok([]byte(content))
}

func (c *diffFileClient) Stat(_ context.Context, path string) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.files[path]; ok {
		return core.Ok(map[string]any{"exists": true})
	}
	return core.Ok(map[string]any{"exists": false})
}

func (c *diffFileClient) FileExists(_ context.Context, path string) core.Result {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.files[path]
	return core.Ok(ok)
}

func (c *diffFileClient) BecomeState() (bool, string, string) {
	return false, "", ""
}

func (c *diffFileClient) SetBecome(bool, string, string) {}

func (c *diffFileClient) Close() core.Result { return core.Ok(nil) }

func requireDownloadBytes(t *core.T, result core.Result) []byte {
	core.RequireTrue(t, result.OK)
	content, ok := result.Value.([]byte)
	core.RequireTrue(t, ok)
	return content
}

// ============================================================
// Step 1.2: copy / template / file / lineinfile / blockinfile / stat module tests
// ============================================================

// --- copy module ---

func TestModulesFile_ModuleCopy_Good_ContentUpload(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=web01",
		"dest":    "/etc/app/config",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertContains(t, result.Msg, "copied to /etc/app/config")

	// Verify upload was performed
	core.AssertEqual(t, 1, mock.uploadCount())
	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/etc/app/config", up.Remote)
	core.AssertEqual(t, []byte("server_name=web01"), up.Content)
	core.AssertEqual(t, fs.FileMode(0644), up.Mode)
}

func TestModulesFile_ModuleCopy_Good_SrcFile(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "nginx.conf")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("worker_processes auto;"), 0644))

	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/nginx/nginx.conf",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/etc/nginx/nginx.conf", up.Remote)
	core.AssertEqual(t, []byte("worker_processes auto;"), up.Content)
}

func TestModulesFile_ModuleCopy_Good_RemoteSrc(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/remote-source.txt", []byte("remote payload"))

	result := requireTaskResult(t, e.moduleCopy(context.Background(), mock, map[string]any{
		"src":        "/tmp/remote-source.txt",
		"dest":       "/etc/app/remote.txt",
		"remote_src": true,
	}, "host1", &Task{}))

	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/etc/app/remote.txt", up.Remote)
	core.AssertEqual(t, []byte("remote payload"), up.Content)
}

func TestModulesFile_ModuleCopy_Good_OwnerGroup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "data",
		"dest":    "/opt/app/data.txt",
		"owner":   "appuser",
		"group":   "appgroup",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Upload + chown + chgrp = 1 upload + 2 Run calls
	core.AssertEqual(t, 1, mock.uploadCount())
	core.AssertTrue(t, mock.hasExecuted(`chown appuser "/opt/app/data.txt"`))
	core.AssertTrue(t, mock.hasExecuted(`chgrp appgroup "/opt/app/data.txt"`))
}

func TestModulesFile_ModuleCopy_Good_CustomMode(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "#!/bin/bash\necho hello",
		"dest":    "/usr/local/bin/hello.sh",
		"mode":    "0755",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, fs.FileMode(0755), up.Mode)
}

func TestModulesFile_ModuleCopy_Bad_MissingDest(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "data",
	}, "host1", &Task{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "dest required")
}

func TestModulesFile_ModuleCopy_Bad_MissingSrcAndContent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"dest": "/tmp/out",
	}, "host1", &Task{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src or content required")
}

func TestModulesFile_ModuleCopy_Bad_SrcFileNotFound(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/file.txt",
		"dest": "/tmp/out",
	}, "host1", &Task{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "read src")
}

func TestModulesFile_ModuleCopy_Good_ContentTakesPrecedenceOverSrc(t *core.T) {
	// When both content and src are given, src is checked first in the implementation
	// but if src is empty string, content is used
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "from_content",
		"dest":    "/tmp/out",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	up := mock.lastUpload()
	core.AssertEqual(t, []byte("from_content"), up.Content)
}

func TestModulesFile_ModuleCopy_Good_SkipsUnchangedContent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.Diff = true
	mock.addFile("/etc/app/config", []byte("server_name=web01"))

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=web01",
		"dest":    "/etc/app/config",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.uploadCount())
	core.AssertContains(t, result.Msg, "already up to date")
}

func TestModulesFile_ModuleCopy_Good_ForceFalseSkipsExistingDest(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=new",
		"dest":    "/etc/app/config",
		"force":   false,
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.uploadCount())
	core.AssertContains(t, result.Msg, "skipped existing destination")
}

func TestModulesFile_ModuleCopy_Good_BackupExistingDest(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "server_name=new",
		"dest":    "/etc/app/config",
		"backup":  true,
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	core.AssertNotNil(t, result.Data)
	backupPath, ok := result.Data["backup_file"].(string)
	core.RequireTrue(t, ok)
	core.AssertContains(t, backupPath, "/etc/app/config.")
	core.AssertEqual(t, 2, mock.uploadCount())
	backupContent := requireDownloadBytes(t, mock.Download(context.Background(), backupPath))
	core.AssertEqual(t, []byte("server_name=old"), backupContent)
}

// --- file module ---

func TestModulesFile_ModuleFile_Good_StateDirectory(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/var/lib/app",
		"state":    "directory",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should execute mkdir -p with default mode 0755
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/var/lib/app"`))
	core.AssertTrue(t, mock.hasExecuted(`chmod 0755`))
}

func TestModulesFile_ModuleFile_Good_StateDirectoryCustomMode(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/opt/data",
		"state":    "directory",
		"mode":     "0700",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/opt/data" && chmod 0700 "/opt/data"`))
}

func TestModulesFile_ModuleFile_Good_StateAbsent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/tmp/old-dir",
		"state":    "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`rm -rf "/tmp/old-dir"`))
}

func TestModulesFile_ModuleFile_Good_StateTouch(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/var/log/app.log",
		"state":    "touch",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`touch "/var/log/app.log"`))
}

func TestModulesFile_ModuleFile_Good_StateLink(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/usr/local/bin/node",
		"state":    "link",
		"src":      "/opt/node/bin/node",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`ln -sf "/opt/node/bin/node" "/usr/local/bin/node"`))
}

func TestModulesFile_ModuleFile_Good_StateHard(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/usr/local/bin/node",
		"state":    "hard",
		"src":      "/opt/node/bin/node",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`ln -f "/opt/node/bin/node" "/usr/local/bin/node"`))
}

func TestModulesFile_ModuleFile_Bad_StateHardMissingSrc(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/usr/local/bin/node",
		"state":    "hard",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src required for hard state")
}

func TestModulesFile_ModuleFile_Bad_LinkMissingSrc(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/usr/local/bin/node",
		"state":    "link",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src required for link state")
}

func TestModulesFile_ModuleFile_Good_OwnerGroupMode(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/var/lib/app/data",
		"state":    "directory",
		"owner":    "www-data",
		"group":    "www-data",
		"mode":     "0775",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should have mkdir, chmod in the directory command, then chown and chgrp
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/var/lib/app/data" && chmod 0775 "/var/lib/app/data"`))
	core.AssertTrue(t, mock.hasExecuted(`chown www-data "/var/lib/app/data"`))
	core.AssertTrue(t, mock.hasExecuted(`chgrp www-data "/var/lib/app/data"`))
}

func TestModulesFile_ModuleFile_Good_RecurseOwner(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/var/www",
		"state":    "directory",
		"owner":    "www-data",
		"recurse":  true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should have both regular chown and recursive chown
	core.AssertTrue(t, mock.hasExecuted(`chown www-data "/var/www"`))
	core.AssertTrue(t, mock.hasExecuted(`chown -R www-data "/var/www"`))
}

func TestModulesFile_ModuleFile_Good_RecurseGroupAndMode(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/srv/app",
		"state":    "directory",
		"group":    "appgroup",
		"mode":     "0770",
		"recurse":  true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	core.AssertTrue(t, mock.hasExecuted(`chgrp appgroup "/srv/app"`))
	core.AssertTrue(t, mock.hasExecuted(`chgrp -R appgroup "/srv/app"`))
	core.AssertTrue(t, mock.hasExecuted(`chmod -R 0770 "/srv/app"`))
}

func TestModulesFile_ModuleFile_Bad_MissingPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		"state": "directory",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "path required")
}

func TestModulesFile_ModuleFile_Good_DestAliasForPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"dest":  "/opt/myapp",
		"state": "directory",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/opt/myapp"`))
}

func TestModulesFile_ModuleFile_Good_StateFileWithMode(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config.yml",
		"state":    "file",
		"mode":     "0600",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`chmod 0600 "/etc/config.yml"`))
}

func TestModulesFile_ModuleFile_Good_DirectoryCommandFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("mkdir", "", "permission denied", 1)

	result, err := moduleFileWithClient(e, mock, map[string]any{
		pathArgKey: "/root/protected",
		"state":    "directory",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "permission denied")
}

// --- lineinfile module ---

func TestModulesFile_ModuleLineinfile_Good_InsertLine(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/hosts",
		"line":     "192.168.1.100 web01",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should use grep -qxF to check and echo to append
	core.AssertTrue(t, mock.hasExecuted(`grep -qxF`))
	core.AssertTrue(t, mock.hasExecuted(`192.168.1.100 web01`))
}

func TestModulesFile_ModuleLineinfile_Good_ReplaceRegexp(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/ssh/sshd_config",
		"regexp":   "^#?PermitRootLogin",
		"line":     "PermitRootLogin no",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should use sed to replace
	core.AssertTrue(t, mock.hasExecuted(`sed -i 's/\^#\?PermitRootLogin/PermitRootLogin no/'`))
}

func TestModulesFile_ModuleLineinfile_Good_RemoveLine(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/hosts", []byte("192.168.1.100 oldhost\n127.0.0.1 localhost\n"))

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/hosts",
		"regexp":   "^192\\.168\\.1\\.100",
		"state":    "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should use sed to delete matching lines
	core.AssertTrue(t, mock.hasExecuted(`sed -i '/\^192`))
	core.AssertTrue(t, mock.hasExecuted(`/d'`))
}

func TestModulesFile_ModuleLineinfile_Good_RegexpFallsBackToAppend(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// Simulate sed returning non-zero (pattern not found)
	mock.expectCommand("sed -i", "", "", 1)

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config",
		"regexp":   "^setting=",
		"line":     "setting=value",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should have attempted sed, then fallen back to echo append
	cmds := mock.executedCommands()
	core.AssertGreaterOrEqual(t, len(cmds), 2)
	core.AssertTrue(t, mock.hasExecuted(`echo`))
}

func TestModulesFile_ModuleLineinfile_Good_CreateFile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), mock, map[string]any{
		pathArgKey: "/etc/example.conf",
		"regexp":   "^setting=",
		"line":     "setting=value",
		"create":   true,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`touch "/etc/example\.conf"`))
	core.AssertTrue(t, mock.hasExecuted(`sed -i`))
}

func TestModulesFile_ModuleLineinfile_Good_BackrefsReplaceMatchOnly(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/example.conf",
		"regexp":   "^(foo=).*$",
		"line":     "\\1bar",
		"backrefs": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`grep -Eq`))
	cmd := mock.lastCommand()
	core.AssertEqual(t, "Run", cmd.Method)
	core.AssertContains(t, cmd.Cmd, "sed -E -i")
	core.AssertContains(t, cmd.Cmd, "s/^(foo=).*$")
	core.AssertContains(t, cmd.Cmd, "\\1bar")
	core.AssertContains(t, cmd.Cmd, `"/etc/example.conf"`)
	core.AssertFalse(t, mock.hasExecuted(`echo`))
}

func TestModulesFile_ModuleLineinfile_Good_BackrefsNoMatchNoAppend(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("grep -Eq", "", "", 1)

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/example.conf",
		"regexp":   "^(foo=).*$",
		"line":     "\\1bar",
		"backrefs": true,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 1, mock.commandCount())
	core.AssertContains(t, mock.lastCommand().Cmd, "grep -Eq")
	core.AssertFalse(t, mock.hasExecuted(`echo`))
}

func TestModulesFile_ModuleLineinfile_Good_InsertBeforeAnchor(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), mock, map[string]any{
		pathArgKey:     "/etc/example.conf",
		"line":         "setting=value",
		"insertbefore": "^# managed settings",
		"firstmatch":   true,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`grep -Eq`))
	core.AssertTrue(t, mock.hasExecuted(regexp.QuoteMeta("print line; done=1 } print")))
}

func TestModulesFile_ModuleLineinfile_Good_InsertAfterAnchor(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), mock, map[string]any{
		pathArgKey:    "/etc/example.conf",
		"line":        "setting=value",
		"insertafter": "^# managed settings",
		"firstmatch":  true,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`grep -Eq`))
	core.AssertTrue(t, mock.hasExecuted(regexp.QuoteMeta("print; if (!done && $0 ~ re) { print line; done=1 }")))
}

func TestModulesFile_ModuleLineinfile_Good_InsertAfterAnchor_DefaultUsesLastMatch(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), mock, map[string]any{
		pathArgKey:    "/etc/example.conf",
		"line":        "setting=value",
		"insertafter": "^# managed settings",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`grep -Eq`))
	core.AssertTrue(t, mock.hasExecuted("pos=NR"))
	core.AssertFalse(t, mock.hasExecuted("done=1"))
}

func TestModulesFile_ModuleLineinfile_Bad_MissingPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"line": "test",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "path required")
}

func TestModulesFile_ModuleLineinfile_Good_DestAliasForPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"dest": "/etc/config",
		"line": "key=value",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`/etc/config`))
}

func TestModulesFile_ModuleLineinfile_Good_AbsentWithNoRegexp(t *core.T) {
	// When state=absent but no regexp, nothing happens (no commands)
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config",
		"state":    "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestModulesFile_ModuleLineinfile_Good_LineWithSlashes(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/nginx/conf.d/default.conf",
		"regexp":   "^root /",
		"line":     "root /var/www/html;",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Slashes in the line should be escaped
	core.AssertTrue(t, mock.hasExecuted(`root \\/var\\/www\\/html;`))
}

func TestModulesFile_ModuleLineinfile_Good_ExactLineAlreadyPresentIsNoOp(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/example.conf", []byte("setting=value\nother=1\n"))

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), mock, map[string]any{
		pathArgKey: "/etc/example.conf",
		"line":     "setting=value",
	}))

	core.AssertFalse(t, result.Changed)
	core.AssertContains(t, result.Msg, "already up to date")
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestModulesFile_ModuleLineinfile_Good_SearchStringReplacesMatchingLine(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/ssh/sshd_config", []byte("PermitRootLogin yes\nPasswordAuthentication yes\n"))

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey:      "/etc/ssh/sshd_config",
		"search_string": "PermitRootLogin",
		"line":          "PermitRootLogin no",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	after := requireDownloadBytes(t, mock.Download(context.Background(), "/etc/ssh/sshd_config"))
	core.AssertEqual(t, "PermitRootLogin no\nPasswordAuthentication yes\n", string(after))
}

func TestModulesFile_ModuleLineinfile_Good_SearchStringRemovesMatchingLine(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/ssh/sshd_config", []byte("PermitRootLogin yes\nPasswordAuthentication yes\n"))

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		pathArgKey:      "/etc/ssh/sshd_config",
		"search_string": "PermitRootLogin",
		"state":         "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	after := requireDownloadBytes(t, mock.Download(context.Background(), "/etc/ssh/sshd_config"))
	core.AssertEqual(t, "PasswordAuthentication yes\n", string(after))
}

func TestModulesFile_ModuleLineinfile_Good_BackupExistingFile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	path := "/etc/example.conf"
	mock.addFile(path, []byte("setting=old\n"))

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), mock, map[string]any{
		pathArgKey: path,
		"line":     "setting=new",
		"backup":   true,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	backupPath, ok := result.Data["backup_file"].(string)
	core.RequireTrue(t, ok)
	core.AssertContains(t, backupPath, "/etc/example.conf.")

	backupContent := requireDownloadBytes(t, mock.Download(context.Background(), backupPath))
	core.AssertEqual(t, []byte("setting=old\n"), backupContent)
}

func TestModulesFile_ModuleLineinfile_Good_DiffData(t *core.T) {
	e := NewExecutor("/tmp")
	e.Diff = true

	client := newDiffFileClient(map[string]string{
		"/etc/example.conf": "setting=old\n",
	})

	result := requireTaskResult(t, e.moduleLineinfile(context.Background(), client, map[string]any{
		pathArgKey: "/etc/example.conf",
		"line":     "setting=new",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	diff, ok := result.Data["diff"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "/etc/example.conf", diff[pathArgKey])
	core.AssertEqual(t, "setting=old\n", diff["before"])
	core.AssertContains(t, diff["after"], "setting=new")
}

// --- replace module ---

func TestModulesFile_ModuleReplace_Good_RegexpReplacementWithBackupAndDiff(t *core.T) {
	e := NewExecutor("/tmp")
	e.Diff = true
	client := newDiffFileClient(map[string]string{
		"/etc/app.conf": "port=8080\nmode=prod\n",
	})

	result := requireTaskResult(t, e.moduleReplace(context.Background(), client, map[string]any{
		pathArgKey: "/etc/app.conf",
		"regexp":   `port=(\d+)`,
		"replace":  "port=9090",
		"backup":   true,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertContains(t, result.Data, "backup_file")
	core.AssertContains(t, result.Data, "diff")

	after := requireDownloadBytes(t, client.Download(context.Background(), "/etc/app.conf"))
	core.AssertEqual(t, "port=9090\nmode=prod\n", string(after))

	backupPath, _ := result.Data["backup_file"].(string)
	core.RequireNotEmpty(t, backupPath)
	backup := requireDownloadBytes(t, client.Download(context.Background(), backupPath))
	core.AssertEqual(t, "port=8080\nmode=prod\n", string(backup))
}

func TestModulesFile_ModuleReplace_Good_NoOpWhenPatternMissing(t *core.T) {
	e := NewExecutor("/tmp")
	client := newDiffFileClient(map[string]string{
		"/etc/app.conf": "port=8080\n",
	})

	result := requireTaskResult(t, e.moduleReplace(context.Background(), client, map[string]any{
		pathArgKey: "/etc/app.conf",
		"regexp":   `mode=.+`,
		"replace":  "mode=prod",
	}))

	core.AssertFalse(t, result.Changed)
	core.AssertContains(t, result.Msg, "already up to date")
}

func TestModulesFile_ModuleReplace_Bad_MissingPath(t *core.T) {
	e := NewExecutor("/tmp")
	client := newDiffFileClient(nil)

	result := e.moduleReplace(context.Background(), client, map[string]any{
		"regexp":  `mode=.+`,
		"replace": "mode=prod",
	})

	core.AssertFalse(t, result.OK)
	core.AssertContains(t, resultErrorMessage(result), "path required")
}

// --- blockinfile module ---

func TestModulesFile_ModuleBlockinfile_Good_InsertBlock(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/nginx/conf.d/upstream.conf",
		"block":    "server 10.0.0.1:8080;\nserver 10.0.0.2:8080;",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should use RunScript for the heredoc approach
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "BEGIN ANSIBLE MANAGED BLOCK"))
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "END ANSIBLE MANAGED BLOCK"))
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "10\\.0\\.0\\.1"))
}

func TestModulesFile_ModuleBlockinfile_Good_CustomMarkers(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/hosts",
		"block":    "10.0.0.5 db01",
		"marker":   "# {mark} managed by devops",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should use custom markers instead of default
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "# BEGIN managed by devops"))
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "# END managed by devops"))
}

func TestModulesFile_ModuleBlockinfile_Good_CustomMarkerBeginAndEnd(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleBlockinfile(context.Background(), mock, map[string]any{
		pathArgKey:     "/etc/app.conf",
		"block":        "setting=value",
		"marker":       "# {mark} managed by app",
		"marker_begin": "START",
		"marker_end":   "STOP",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "# START managed by app"))
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "# STOP managed by app"))
}

func TestModulesFile_ModuleBlockinfile_Good_NewlinePadding(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey:        "/etc/hosts",
		"block":           "10.0.0.5 db01",
		"prepend_newline": true,
		"append_newline":  true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	cmd := mock.lastCommand().Cmd
	core.AssertContains(t, cmd, "\n\n# BEGIN ANSIBLE MANAGED BLOCK\n10.0.0.5 db01\n# END ANSIBLE MANAGED BLOCK\n")
}

func TestModulesFile_ModuleBlockinfile_Good_RemoveBlock(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config",
		"state":    "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should use sed to remove the block between markers
	core.AssertTrue(t, mock.hasExecuted(`sed -i '/.*BEGIN ANSIBLE MANAGED BLOCK/,/.*END ANSIBLE MANAGED BLOCK/d'`))
}

func TestModulesFile_ModuleBlockinfile_Good_RemoveBlockWithBackup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/config", []byte("before\n# BEGIN ANSIBLE MANAGED BLOCK\nmanaged\n# END ANSIBLE MANAGED BLOCK\nafter\n"))

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config",
		"state":    "absent",
		"backup":   true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	backupPath, ok := result.Data["backup_file"].(string)
	core.RequireTrue(t, ok)
	core.AssertContains(t, backupPath, "/etc/config.")

	backupContent := requireDownloadBytes(t, mock.Download(context.Background(), backupPath))
	core.AssertEqual(t, []byte("before\n# BEGIN ANSIBLE MANAGED BLOCK\nmanaged\n# END ANSIBLE MANAGED BLOCK\nafter\n"), backupContent)
}

func TestModulesFile_ModuleBlockinfile_Good_RemoveBlockWithCustomMarkerBeginAndEnd(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleBlockinfile(context.Background(), mock, map[string]any{
		pathArgKey:     "/etc/app.conf",
		"state":        "absent",
		"marker":       "# {mark} managed by app",
		"marker_begin": "START",
		"marker_end":   "STOP",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`sed -i '/.*START managed by app/,/.*STOP managed by app/d'`))
}

func TestModulesFile_ModuleBlockinfile_Good_CreateFile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/new-config",
		"block":    "setting=value",
		"create":   true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Should touch the file first when create=true
	core.AssertTrue(t, mock.hasExecuted(`touch "/etc/new-config"`))
}

func TestModulesFile_ModuleBlockinfile_Good_BackupExistingDest(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/config", []byte("old block contents"))

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config",
		"block":    "new block contents",
		"backup":   true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	backupPath, ok := result.Data["backup_file"].(string)
	core.RequireTrue(t, ok)
	core.AssertContains(t, backupPath, "/etc/config.")
	core.AssertEqual(t, 1, mock.uploadCount())

	backupContent := requireDownloadBytes(t, mock.Download(context.Background(), backupPath))
	core.AssertEqual(t, []byte("old block contents"), backupContent)
}

func TestModulesFile_ModuleBlockinfile_Good_DiffData(t *core.T) {
	e := NewExecutor("/tmp")
	e.Diff = true

	client := newDiffFileClient(map[string]string{
		"/etc/config": "old block contents\n",
	})

	result := requireTaskResult(t, e.moduleBlockinfile(context.Background(), client, map[string]any{
		pathArgKey: "/etc/config",
		"block":    "new block contents",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	diff, ok := result.Data["diff"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "/etc/config", diff[pathArgKey])
	core.AssertEqual(t, "old block contents\n", diff["before"])
	core.AssertContains(t, diff["after"], "new block contents")
}

func TestModulesFile_ModuleBlockinfile_Bad_MissingPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"block": "content",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "path required")
}

func TestModulesFile_ModuleBlockinfile_Good_DestAliasForPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"dest":  "/etc/config",
		"block": "data",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesFile_ModuleBlockinfile_Good_ScriptFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("BLOCK_EOF", "", "write error", 1)

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/config",
		"block":    "data",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "write error")
}

// --- stat module ---

func TestModulesFile_ModuleStat_Good_ExistingFile(t *core.T) {
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
		pathArgKey: "/etc/nginx/nginx.conf",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed) // stat never changes anything
	core.AssertNotNil(t, result.Data)

	stat, ok := result.Data["stat"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, true, stat["exists"])
	core.AssertEqual(t, false, stat["isdir"])
	core.AssertEqual(t, "0644", stat["mode"])
	core.AssertEqual(t, 1234, stat["size"])
}

func TestModulesFile_ModuleStat_Good_MissingFile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleStatWithClient(e, mock, map[string]any{
		pathArgKey: "/nonexistent/file.txt",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	stat, ok := result.Data["stat"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, false, stat["exists"])
}

func TestModulesFile_ModuleStat_Good_Directory(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addStat("/var/log", map[string]any{
		"exists": true,
		"isdir":  true,
		"mode":   "0755",
	})

	result, err := moduleStatWithClient(e, mock, map[string]any{
		pathArgKey: "/var/log",
	})

	core.RequireNoError(t, err)
	stat := result.Data["stat"].(map[string]any)
	core.AssertEqual(t, true, stat["exists"])
	core.AssertEqual(t, true, stat["isdir"])
}

func TestModulesFile_ModuleStat_Good_FallbackFromFileSystem(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// No explicit stat, but add a file — stat falls back to file existence
	mock.addFile("/etc/hosts", []byte("127.0.0.1 localhost"))

	result, err := moduleStatWithClient(e, mock, map[string]any{
		pathArgKey: "/etc/hosts",
	})

	core.RequireNoError(t, err)
	stat := result.Data["stat"].(map[string]any)
	core.AssertEqual(t, true, stat["exists"])
	core.AssertEqual(t, false, stat["isdir"])
}

func TestModulesFile_ModuleStat_Bad_MissingPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleStatWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "path required")
}

// --- template module ---

func TestModulesFile_ModuleTemplate_Good_BasicTemplate(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "app.conf.j2")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("server_name={{ server_name }};"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("server_name", "web01.example.com")

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/nginx/conf.d/app.conf",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertContains(t, result.Msg, "templated to /etc/nginx/conf.d/app.conf")

	// Verify upload was performed with templated content
	core.AssertEqual(t, 1, mock.uploadCount())
	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/etc/nginx/conf.d/app.conf", up.Remote)
	// Template replaces {{ var }} — the TemplateFile does Jinja2 to Go conversion
	core.AssertContains(t, string(up.Content), "web01.example.com")
}

func TestModulesFile_ModuleTemplate_Good_AnsibleFactsMapTemplate(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "facts.conf.j2")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("host={{ ansible_facts.ansible_hostname }}"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	e.facts["host1"] = &Facts{
		Hostname:     "web01",
		Distribution: "debian",
	}

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/app/facts.conf",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertContains(t, string(up.Content), "host=web01")
}

func TestModulesFile_ModuleTemplate_Good_TaskVarsAndHostMagicVars(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "context.conf.j2")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("short={{ inventory_hostname_short }} local={{ local_value }}"), 0644))

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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertContains(t, string(up.Content), "short=web01")
	core.AssertContains(t, string(up.Content), "local=from-task")
}

func TestModulesFile_ModuleTemplate_Good_CustomMode(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "script.sh.j2")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("#!/bin/bash\necho done"), 0644))

	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/usr/local/bin/run.sh",
		"mode": "0755",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, fs.FileMode(0755), up.Mode)
}

func TestModulesFile_ModuleTemplate_Good_ForceFalseSkipsExistingDest(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "config.tmpl")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("server_name={{ inventory_hostname }}"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":   srcPath,
		"dest":  "/etc/app/config",
		"force": false,
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.uploadCount())
	core.AssertContains(t, result.Msg, "skipped existing destination")
}

func TestModulesFile_ModuleTemplate_Good_BackupExistingDest(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "config.tmpl")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("server_name={{ inventory_hostname }}"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/etc/app/config", []byte("server_name=old"))

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":    srcPath,
		"dest":   "/etc/app/config",
		"backup": true,
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	core.AssertNotNil(t, result.Data)
	backupPath, ok := result.Data["backup_file"].(string)
	core.RequireTrue(t, ok)
	core.AssertContains(t, backupPath, "/etc/app/config.")
	core.AssertEqual(t, 2, mock.uploadCount())
	backupContent := requireDownloadBytes(t, mock.Download(context.Background(), backupPath))
	core.AssertEqual(t, []byte("server_name=old"), backupContent)
}

func TestModulesFile_ModuleTemplate_Bad_MissingSrc(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"dest": "/tmp/out",
	}, "host1", &Task{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src and dest required")
}

func TestModulesFile_ModuleTemplate_Bad_MissingDest(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src": "/tmp/in.j2",
	}, "host1", &Task{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src and dest required")
}

func TestModulesFile_ModuleTemplate_Bad_SrcFileNotFound(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/template.j2",
		"dest": "/tmp/out",
	}, "host1", &Task{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "template")
}

func TestModulesFile_ModuleTemplate_Good_PlainTextNoVars(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "static.conf")
	content := "listen 80;\nserver_name localhost;"
	core.RequireNoError(t, writeTestFile(srcPath, []byte(content), 0644))

	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/config",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, content, string(up.Content))
}

func TestModulesFile_ModuleTemplate_Good_DiffData(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "app.conf.j2")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("server_name={{ server_name }};"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	e.Diff = true
	e.SetVar("server_name", "web01.example.com")
	mock.addFile("/etc/nginx/conf.d/app.conf", []byte("server_name=old.example.com;"))

	result, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  srcPath,
		"dest": "/etc/nginx/conf.d/app.conf",
	}, "host1", &Task{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)

	diff, ok := result.Data["diff"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "/etc/nginx/conf.d/app.conf", diff[pathArgKey])
	core.AssertEqual(t, "server_name=old.example.com;", diff["before"])
	core.AssertContains(t, diff["after"], "web01.example.com")
}

// --- Cross-module dispatch tests for file modules ---

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchCopy(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "copy",
		Args: map[string]any{
			"content": "hello world",
			"dest":    "/tmp/hello.txt",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, 1, mock.uploadCount())
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchFile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "file",
		Args: map[string]any{
			pathArgKey: "/opt/data",
			"state":    "directory",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted("mkdir"))
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchStat(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addStat("/etc/hosts", map[string]any{"exists": true, "isdir": false})

	task := &Task{
		Module: "stat",
		Args: map[string]any{
			pathArgKey: "/etc/hosts",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	stat := result.Data["stat"].(map[string]any)
	core.AssertEqual(t, true, stat["exists"])
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchLineinfile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "lineinfile",
		Args: map[string]any{
			pathArgKey: "/etc/hosts",
			"line":     "10.0.0.1 dbhost",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchBlockinfile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "blockinfile",
		Args: map[string]any{
			pathArgKey: "/etc/config",
			"block":    "key=value",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesFile_ExecuteModuleWithMock_Good_DispatchTemplate(t *core.T) {
	tmpDir := t.TempDir()
	srcPath := joinPath(tmpDir, "test.j2")
	core.RequireNoError(t, writeTestFile(srcPath, []byte("static content"), 0644))

	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "template",
		Args: map[string]any{
			"src":  srcPath,
			"dest": "/etc/out.conf",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, 1, mock.uploadCount())
}

// --- Template variable resolution integration ---

func TestModulesFile_ModuleCopy_Good_TemplatedArgs(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/opt/myapp/config.yml", up.Remote)
}

func TestModulesFile_ModuleFile_Good_TemplatedPath(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("app_dir", "/var/www/html")

	task := &Task{
		Module: "file",
		Args: map[string]any{
			pathArgKey: "{{ app_dir }}/uploads",
			"state":    "directory",
			"owner":    "www-data",
		},
	}

	args := e.templateArgs(task.Args, "host1", task)
	result, err := moduleFileWithClient(e, mock, args)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/var/www/html/uploads"`))
	core.AssertTrue(t, mock.hasExecuted(`chown www-data "/var/www/html/uploads"`))
}

func TestModulesFile_ModuleGetURL_Good_Checksum(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	payload := "downloaded artifact"
	mock.expectCommand(`curl.*https://downloads\.example\.com/app\.tgz`, payload, "", 0)

	sum := sha256.Sum256([]byte(payload))
	result := requireTaskResult(t, e.moduleGetURL(context.Background(), mock, map[string]any{
		"url":      "https://downloads.example.com/app.tgz",
		"dest":     "/tmp/app.tgz",
		"checksum": "sha256:" + hex.EncodeToString(sum[:]),
		"mode":     "0600",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, 1, mock.uploadCount())

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/tmp/app.tgz", up.Remote)
	core.AssertEqual(t, []byte(payload), up.Content)
	core.AssertEqual(t, fs.FileMode(0600), up.Mode)
}

func TestModulesFile_ModuleGetURL_Good_Sha512Checksum(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	payload := "downloaded artifact"
	mock.expectCommand(`curl.*https://downloads\.example\.com/app\.tgz`, payload, "", 0)

	sum := sha512.Sum512([]byte(payload))
	result := requireTaskResult(t, e.moduleGetURL(context.Background(), mock, map[string]any{
		"url":      "https://downloads.example.com/app.tgz",
		"dest":     "/tmp/app.tgz",
		"checksum": "sha512:" + hex.EncodeToString(sum[:]),
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, 1, mock.uploadCount())

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/tmp/app.tgz", up.Remote)
	core.AssertEqual(t, []byte(payload), up.Content)
}

func TestModulesFile_ModuleGetURL_Good_ChecksumFileURL(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	payload := "downloaded artifact"
	sum := sha256.Sum256([]byte(payload))
	checksumURL := "https://downloads.example.com/app.tgz.sha256"

	mock.expectCommand(`curl.*https://downloads\.example\.com/app\.tgz\.sha256(?:["\s]|$)`, hex.EncodeToString(sum[:])+"  app.tgz\n", "", 0)
	mock.expectCommand(`curl.*https://downloads\.example\.com/app\.tgz(?:["\s]|$)`, payload, "", 0)

	result := requireTaskResult(t, e.moduleGetURL(context.Background(), mock, map[string]any{
		"url":      "https://downloads.example.com/app.tgz",
		"dest":     "/tmp/app.tgz",
		"checksum": "sha256:" + checksumURL,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, 1, mock.uploadCount())

	up := mock.lastUpload()
	core.AssertNotNil(t, up)
	core.AssertEqual(t, "/tmp/app.tgz", up.Remote)
	core.AssertEqual(t, []byte(payload), up.Content)
}

func TestModulesFile_ModuleGetURL_Good_ForceFalseSkipsExistingDestination(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/app.tgz", []byte("existing artifact"))

	result := requireTaskResult(t, e.moduleGetURL(context.Background(), mock, map[string]any{
		"url":   "https://downloads.example.com/app.tgz",
		"dest":  "/tmp/app.tgz",
		"force": false,
	}))

	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, "skipped existing destination: /tmp/app.tgz", result.Msg)
	core.AssertEqual(t, 0, mock.commandCount())
	core.AssertEqual(t, 0, mock.uploadCount())
}

func TestModulesFile_ModuleGetURL_Good_DisablesProxyUsage(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*https://downloads\.example\.com/app\.tgz`, "downloaded artifact", "", 0)

	result := requireTaskResult(t, e.moduleGetURL(context.Background(), mock, map[string]any{
		"url":       "https://downloads.example.com/app.tgz",
		"dest":      "/tmp/app.tgz",
		"use_proxy": false,
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`--noproxy`))
	core.AssertTrue(t, mock.hasExecuted(`wget --no-proxy`))
}

func TestModulesFile_ModuleGetURL_Bad_ChecksumMismatch(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*https://downloads\.example\.com/app\.tgz`, "downloaded artifact", "", 0)

	result := requireTaskResult(t, e.moduleGetURL(context.Background(), mock, map[string]any{
		"url":      "https://downloads.example.com/app.tgz",
		"dest":     "/tmp/app.tgz",
		"checksum": "sha256:deadbeef",
	}))

	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "checksum mismatch")
	core.AssertEqual(t, 0, mock.uploadCount())
}
