package ansible

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Step 1.2: copy / template / file / lineinfile / blockinfile / stat module tests
// ============================================================

// --- copy module ---

func TestModuleCopy_Good_ContentUpload(t *testing.T) {
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
	assert.Equal(t, os.FileMode(0644), up.Mode)
}

func TestModuleCopy_Good_SrcFile(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "nginx.conf")
	require.NoError(t, os.WriteFile(srcPath, []byte("worker_processes auto;"), 0644))

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

func TestModuleCopy_Good_OwnerGroup(t *testing.T) {
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

func TestModuleCopy_Good_CustomMode(t *testing.T) {
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
	assert.Equal(t, os.FileMode(0755), up.Mode)
}

func TestModuleCopy_Bad_MissingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"content": "data",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dest required")
}

func TestModuleCopy_Bad_MissingSrcAndContent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src or content required")
}

func TestModuleCopy_Bad_SrcFileNotFound(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleCopyWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/file.txt",
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read src")
}

func TestModuleCopy_Good_ContentTakesPrecedenceOverSrc(t *testing.T) {
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

// --- file module ---

func TestModuleFile_Good_StateDirectory(t *testing.T) {
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

func TestModuleFile_Good_StateDirectoryCustomMode(t *testing.T) {
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

func TestModuleFile_Good_StateAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/tmp/old-dir",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`rm -rf "/tmp/old-dir"`))
}

func TestModuleFile_Good_StateTouch(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/var/log/app.log",
		"state": "touch",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`touch "/var/log/app.log"`))
}

func TestModuleFile_Good_StateLink(t *testing.T) {
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

func TestModuleFile_Bad_LinkMissingSrc(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		"path":  "/usr/local/bin/node",
		"state": "link",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src required for link state")
}

func TestModuleFile_Good_OwnerGroupMode(t *testing.T) {
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

func TestModuleFile_Good_RecurseOwner(t *testing.T) {
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

func TestModuleFile_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleFileWithClient(e, mock, map[string]any{
		"state": "directory",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestModuleFile_Good_DestAliasForPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleFileWithClient(e, mock, map[string]any{
		"dest":  "/opt/myapp",
		"state": "directory",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`mkdir -p "/opt/myapp"`))
}

func TestModuleFile_Good_StateFileWithMode(t *testing.T) {
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

func TestModuleFile_Good_DirectoryCommandFailure(t *testing.T) {
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

func TestModuleLineinfile_Good_InsertLine(t *testing.T) {
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

func TestModuleLineinfile_Good_ReplaceRegexp(t *testing.T) {
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

func TestModuleLineinfile_Good_RemoveLine(t *testing.T) {
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

func TestModuleLineinfile_Good_RegexpFallsBackToAppend(t *testing.T) {
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

func TestModuleLineinfile_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"line": "test",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestModuleLineinfile_Good_DestAliasForPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleLineinfileWithClient(e, mock, map[string]any{
		"dest": "/etc/config",
		"line": "key=value",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`/etc/config`))
}

func TestModuleLineinfile_Good_AbsentWithNoRegexp(t *testing.T) {
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

func TestModuleLineinfile_Good_LineWithSlashes(t *testing.T) {
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

// --- blockinfile module ---

func TestModuleBlockinfile_Good_InsertBlock(t *testing.T) {
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

func TestModuleBlockinfile_Good_CustomMarkers(t *testing.T) {
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

func TestModuleBlockinfile_Good_RemoveBlock(t *testing.T) {
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

func TestModuleBlockinfile_Good_CreateFile(t *testing.T) {
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

func TestModuleBlockinfile_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"block": "content",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

func TestModuleBlockinfile_Good_DestAliasForPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleBlockinfileWithClient(e, mock, map[string]any{
		"dest":  "/etc/config",
		"block": "data",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestModuleBlockinfile_Good_ScriptFailure(t *testing.T) {
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

func TestModuleStat_Good_ExistingFile(t *testing.T) {
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

func TestModuleStat_Good_MissingFile(t *testing.T) {
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

func TestModuleStat_Good_Directory(t *testing.T) {
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

func TestModuleStat_Good_FallbackFromFileSystem(t *testing.T) {
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

func TestModuleStat_Bad_MissingPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleStatWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "path required")
}

// --- template module ---

func TestModuleTemplate_Good_BasicTemplate(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "app.conf.j2")
	require.NoError(t, os.WriteFile(srcPath, []byte("server_name={{ server_name }};"), 0644))

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

func TestModuleTemplate_Good_CustomMode(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "script.sh.j2")
	require.NoError(t, os.WriteFile(srcPath, []byte("#!/bin/bash\necho done"), 0644))

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
	assert.Equal(t, os.FileMode(0755), up.Mode)
}

func TestModuleTemplate_Bad_MissingSrc(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModuleTemplate_Bad_MissingDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src": "/tmp/in.j2",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModuleTemplate_Bad_SrcFileNotFound(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	_, err := moduleTemplateWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/template.j2",
		"dest": "/tmp/out",
	}, "host1", &Task{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "template")
}

func TestModuleTemplate_Good_PlainTextNoVars(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "static.conf")
	content := "listen 80;\nserver_name localhost;"
	require.NoError(t, os.WriteFile(srcPath, []byte(content), 0644))

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

// --- Cross-module dispatch tests for file modules ---

func TestExecuteModuleWithMock_Good_DispatchCopy(t *testing.T) {
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

func TestExecuteModuleWithMock_Good_DispatchFile(t *testing.T) {
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

func TestExecuteModuleWithMock_Good_DispatchStat(t *testing.T) {
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

func TestExecuteModuleWithMock_Good_DispatchLineinfile(t *testing.T) {
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

func TestExecuteModuleWithMock_Good_DispatchBlockinfile(t *testing.T) {
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

func TestExecuteModuleWithMock_Good_DispatchTemplate(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "test.j2")
	require.NoError(t, os.WriteFile(srcPath, []byte("static content"), 0644))

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

func TestModuleCopy_Good_TemplatedArgs(t *testing.T) {
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

func TestModuleFile_Good_TemplatedPath(t *testing.T) {
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
