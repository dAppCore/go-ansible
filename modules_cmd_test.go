package ansible

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Step 1.1: command / shell / raw / script module tests
// ============================================================

// --- MockSSHClient basic tests ---

func TestMockSSHClient_Good_RunRecordsExecution(t *testing.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("echo hello", "hello\n", "", 0)

	stdout, stderr, rc, err := mock.Run(nil, "echo hello")

	assert.NoError(t, err)
	assert.Equal(t, "hello\n", stdout)
	assert.Equal(t, "", stderr)
	assert.Equal(t, 0, rc)
	assert.Equal(t, 1, mock.commandCount())
	assert.Equal(t, "Run", mock.lastCommand().Method)
	assert.Equal(t, "echo hello", mock.lastCommand().Cmd)
}

func TestMockSSHClient_Good_RunScriptRecordsExecution(t *testing.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("set -e", "ok", "", 0)

	stdout, _, rc, err := mock.RunScript(nil, "set -e\necho done")

	assert.NoError(t, err)
	assert.Equal(t, "ok", stdout)
	assert.Equal(t, 0, rc)
	assert.Equal(t, 1, mock.commandCount())
	assert.Equal(t, "RunScript", mock.lastCommand().Method)
}

func TestMockSSHClient_Good_DefaultSuccessResponse(t *testing.T) {
	mock := NewMockSSHClient()

	// No expectations registered — should return empty success
	stdout, stderr, rc, err := mock.Run(nil, "anything")

	assert.NoError(t, err)
	assert.Equal(t, "", stdout)
	assert.Equal(t, "", stderr)
	assert.Equal(t, 0, rc)
}

func TestMockSSHClient_Good_LastMatchWins(t *testing.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("echo", "first", "", 0)
	mock.expectCommand("echo", "second", "", 0)

	stdout, _, _, _ := mock.Run(nil, "echo hello")

	assert.Equal(t, "second", stdout)
}

func TestMockSSHClient_Good_FileOperations(t *testing.T) {
	mock := NewMockSSHClient()

	// File does not exist initially
	exists, err := mock.FileExists(nil, "/etc/config")
	assert.NoError(t, err)
	assert.False(t, exists)

	// Add file
	mock.addFile("/etc/config", []byte("key=value"))

	// Now it exists
	exists, err = mock.FileExists(nil, "/etc/config")
	assert.NoError(t, err)
	assert.True(t, exists)

	// Download it
	content, err := mock.Download(nil, "/etc/config")
	assert.NoError(t, err)
	assert.Equal(t, []byte("key=value"), content)

	// Download non-existent file
	_, err = mock.Download(nil, "/nonexistent")
	assert.Error(t, err)
}

func TestMockSSHClient_Good_StatWithExplicit(t *testing.T) {
	mock := NewMockSSHClient()
	mock.addStat("/var/log", map[string]any{"exists": true, "isdir": true})

	info, err := mock.Stat(nil, "/var/log")
	assert.NoError(t, err)
	assert.Equal(t, true, info["exists"])
	assert.Equal(t, true, info["isdir"])
}

func TestMockSSHClient_Good_StatFallback(t *testing.T) {
	mock := NewMockSSHClient()
	mock.addFile("/etc/hosts", []byte("127.0.0.1 localhost"))

	info, err := mock.Stat(nil, "/etc/hosts")
	assert.NoError(t, err)
	assert.Equal(t, true, info["exists"])
	assert.Equal(t, false, info["isdir"])

	info, err = mock.Stat(nil, "/nonexistent")
	assert.NoError(t, err)
	assert.Equal(t, false, info["exists"])
}

func TestMockSSHClient_Good_BecomeTracking(t *testing.T) {
	mock := NewMockSSHClient()

	assert.False(t, mock.become)
	assert.Equal(t, "", mock.becomeUser)

	mock.SetBecome(true, "root", "secret")

	assert.True(t, mock.become)
	assert.Equal(t, "root", mock.becomeUser)
	assert.Equal(t, "secret", mock.becomePass)
}

func TestMockSSHClient_Good_HasExecuted(t *testing.T) {
	mock := NewMockSSHClient()
	_, _, _, _ = mock.Run(nil, "systemctl restart nginx")
	_, _, _, _ = mock.Run(nil, "apt-get update")

	assert.True(t, mock.hasExecuted("systemctl.*nginx"))
	assert.True(t, mock.hasExecuted("apt-get"))
	assert.False(t, mock.hasExecuted("yum"))
}

func TestMockSSHClient_Good_HasExecutedMethod(t *testing.T) {
	mock := NewMockSSHClient()
	_, _, _, _ = mock.Run(nil, "echo run")
	_, _, _, _ = mock.RunScript(nil, "echo script")

	assert.True(t, mock.hasExecutedMethod("Run", "echo run"))
	assert.True(t, mock.hasExecutedMethod("RunScript", "echo script"))
	assert.False(t, mock.hasExecutedMethod("Run", "echo script"))
	assert.False(t, mock.hasExecutedMethod("RunScript", "echo run"))
}

func TestMockSSHClient_Good_Reset(t *testing.T) {
	mock := NewMockSSHClient()
	_, _, _, _ = mock.Run(nil, "echo hello")
	assert.Equal(t, 1, mock.commandCount())

	mock.reset()
	assert.Equal(t, 0, mock.commandCount())
}

func TestMockSSHClient_Good_ErrorExpectation(t *testing.T) {
	mock := NewMockSSHClient()
	mock.expectCommandError("bad cmd", assert.AnError)

	_, _, _, err := mock.Run(nil, "bad cmd")
	assert.Error(t, err)
}

// --- command module ---

func TestModuleCommand_Good_BasicCommand(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("ls -la /tmp", "total 0\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "ls -la /tmp",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, "total 0\n", result.Stdout)
	assert.Equal(t, 0, result.RC)

	// Verify it used Run (not RunScript)
	assert.True(t, mock.hasExecutedMethod("Run", "ls -la /tmp"))
	assert.False(t, mock.hasExecutedMethod("RunScript", ".*"))
}

func TestModuleCommand_Good_CmdArg(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("whoami", "root\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"cmd": "whoami",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "root\n", result.Stdout)
	assert.True(t, mock.hasExecutedMethod("Run", "whoami"))
}

func TestModuleCommand_Good_WithChdir(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`cd "/var/log" && ls`, "syslog\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "ls",
		"chdir":       "/var/log",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	// The command should have been wrapped with cd
	last := mock.lastCommand()
	assert.Equal(t, "Run", last.Method)
	assert.Contains(t, last.Cmd, `cd "/var/log"`)
	assert.Contains(t, last.Cmd, "ls")
}

func TestModuleCommand_Bad_NoCommand(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleCommandWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no command specified")
}

func TestModuleCommand_Good_NonZeroRC(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("false", "", "error occurred", 1)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "false",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Equal(t, 1, result.RC)
	assert.Equal(t, "error occurred", result.Stderr)
}

func TestModuleCommand_Good_SSHError(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", assert.AnError)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "any command",
	})

	require.NoError(t, err) // Module wraps SSH errors into result.Failed
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, assert.AnError.Error())
}

func TestModuleCommand_Good_RawParamsTakesPrecedence(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("from_raw", "raw\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "from_raw",
		"cmd":         "from_cmd",
	})

	require.NoError(t, err)
	assert.Equal(t, "raw\n", result.Stdout)
	assert.True(t, mock.hasExecuted("from_raw"))
}

// --- shell module ---

func TestModuleShell_Good_BasicShell(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo hello", "hello\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "echo hello",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, "hello\n", result.Stdout)

	// Shell must use RunScript (not Run)
	assert.True(t, mock.hasExecutedMethod("RunScript", "echo hello"))
	assert.False(t, mock.hasExecutedMethod("Run", ".*"))
}

func TestModuleShell_Good_CmdArg(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("date", "Thu Feb 20\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"cmd": "date",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecutedMethod("RunScript", "date"))
}

func TestModuleShell_Good_WithChdir(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`cd "/app" && npm install`, "done\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "npm install",
		"chdir":       "/app",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	last := mock.lastCommand()
	assert.Equal(t, "RunScript", last.Method)
	assert.Contains(t, last.Cmd, `cd "/app"`)
	assert.Contains(t, last.Cmd, "npm install")
}

func TestModuleShell_Bad_NoCommand(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleShellWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no command specified")
}

func TestModuleShell_Good_NonZeroRC(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("exit 2", "", "failed", 2)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "exit 2",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Equal(t, 2, result.RC)
}

func TestModuleShell_Good_SSHError(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", assert.AnError)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "some command",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

func TestModuleShell_Good_PipelineCommand(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`cat /etc/passwd \| grep root`, "root:x:0:0\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "cat /etc/passwd | grep root",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	// Shell uses RunScript, so pipes work
	assert.True(t, mock.hasExecutedMethod("RunScript", "cat /etc/passwd"))
}

// --- raw module ---

func TestModuleRaw_Good_BasicRaw(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("uname -a", "Linux host1 5.15\n", "", 0)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "uname -a",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "Linux host1 5.15\n", result.Stdout)

	// Raw must use Run (not RunScript) — no shell wrapping
	assert.True(t, mock.hasExecutedMethod("Run", "uname -a"))
	assert.False(t, mock.hasExecutedMethod("RunScript", ".*"))
}

func TestModuleRaw_Bad_NoCommand(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleRawWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no command specified")
}

func TestModuleRaw_Good_NoChdir(t *testing.T) {
	// Raw module does NOT support chdir — it should ignore it
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "echo test",
		"chdir":       "/should/be/ignored",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	// The chdir should NOT appear in the command
	last := mock.lastCommand()
	assert.Equal(t, "echo test", last.Cmd)
	assert.NotContains(t, last.Cmd, "cd")
}

func TestModuleRaw_Good_NonZeroRC(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("invalid", "", "not found", 127)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "invalid",
	})

	require.NoError(t, err)
	// Note: raw module does NOT set Failed based on RC
	assert.Equal(t, 127, result.RC)
	assert.Equal(t, "not found", result.Stderr)
}

func TestModuleRaw_Good_SSHError(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", assert.AnError)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "any",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

func TestModuleRaw_Good_ExactCommandPassthrough(t *testing.T) {
	// Raw should pass the command exactly as given — no wrapping
	e, mock := newTestExecutorWithMock("host1")
	complexCmd := `/usr/bin/python3 -c 'import sys; print(sys.version)'`
	mock.expectCommand(".*python3.*", "3.10.0\n", "", 0)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": complexCmd,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	last := mock.lastCommand()
	assert.Equal(t, complexCmd, last.Cmd)
}

// --- script module ---

func TestModuleScript_Good_BasicScript(t *testing.T) {
	// Create a temporary script file
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "setup.sh")
	scriptContent := "#!/bin/bash\necho 'setup complete'\nexit 0"
	require.NoError(t, os.WriteFile(scriptPath, []byte(scriptContent), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("setup complete", "setup complete\n", "", 0)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)

	// Script must use RunScript (not Run) — it sends the file content
	assert.True(t, mock.hasExecutedMethod("RunScript", "setup complete"))
	assert.False(t, mock.hasExecutedMethod("Run", ".*"))

	// Verify the full script content was sent
	last := mock.lastCommand()
	assert.Equal(t, scriptContent, last.Cmd)
}

func TestModuleScript_Bad_NoScript(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleScriptWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no script specified")
}

func TestModuleScript_Bad_FileNotFound(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": "/nonexistent/script.sh",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read script")
}

func TestModuleScript_Good_NonZeroRC(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "fail.sh")
	require.NoError(t, os.WriteFile(scriptPath, []byte("exit 1"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("exit 1", "", "script failed", 1)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Equal(t, 1, result.RC)
}

func TestModuleScript_Good_MultiLineScript(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "multi.sh")
	scriptContent := "#!/bin/bash\nset -e\napt-get update\napt-get install -y nginx\nsystemctl start nginx"
	require.NoError(t, os.WriteFile(scriptPath, []byte(scriptContent), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("apt-get", "done\n", "", 0)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)

	// Verify RunScript was called with the full content
	last := mock.lastCommand()
	assert.Equal(t, "RunScript", last.Method)
	assert.Equal(t, scriptContent, last.Cmd)
}

func TestModuleScript_Good_SSHError(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "ok.sh")
	require.NoError(t, os.WriteFile(scriptPath, []byte("echo ok"), 0755))

	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", assert.AnError)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

// --- Cross-module differentiation tests ---

func TestModuleDifferentiation_Good_CommandUsesRun(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleCommandWithClient(e, mock, map[string]any{"_raw_params": "echo test"})

	cmds := mock.executedCommands()
	require.Len(t, cmds, 1)
	assert.Equal(t, "Run", cmds[0].Method, "command module must use Run()")
}

func TestModuleDifferentiation_Good_ShellUsesRunScript(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleShellWithClient(e, mock, map[string]any{"_raw_params": "echo test"})

	cmds := mock.executedCommands()
	require.Len(t, cmds, 1)
	assert.Equal(t, "RunScript", cmds[0].Method, "shell module must use RunScript()")
}

func TestModuleDifferentiation_Good_RawUsesRun(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleRawWithClient(e, mock, map[string]any{"_raw_params": "echo test"})

	cmds := mock.executedCommands()
	require.Len(t, cmds, 1)
	assert.Equal(t, "Run", cmds[0].Method, "raw module must use Run()")
}

func TestModuleDifferentiation_Good_ScriptUsesRunScript(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "test.sh")
	require.NoError(t, os.WriteFile(scriptPath, []byte("echo test"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleScriptWithClient(e, mock, map[string]any{"_raw_params": scriptPath})

	cmds := mock.executedCommands()
	require.Len(t, cmds, 1)
	assert.Equal(t, "RunScript", cmds[0].Method, "script module must use RunScript()")
}

// --- executeModuleWithMock dispatch tests ---

func TestExecuteModuleWithMock_Good_DispatchCommand(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("uptime", "up 5 days\n", "", 0)

	task := &Task{
		Module: "command",
		Args:   map[string]any{"_raw_params": "uptime"},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "up 5 days\n", result.Stdout)
}

func TestExecuteModuleWithMock_Good_DispatchShell(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("ps aux", "root.*bash\n", "", 0)

	task := &Task{
		Module: "ansible.builtin.shell",
		Args:   map[string]any{"_raw_params": "ps aux | grep bash"},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchRaw(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("cat /etc/hostname", "web01\n", "", 0)

	task := &Task{
		Module: "raw",
		Args:   map[string]any{"_raw_params": "cat /etc/hostname"},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "web01\n", result.Stdout)
}

func TestExecuteModuleWithMock_Good_DispatchScript(t *testing.T) {
	tmpDir := t.TempDir()
	scriptPath := filepath.Join(tmpDir, "deploy.sh")
	require.NoError(t, os.WriteFile(scriptPath, []byte("echo deploying"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("deploying", "deploying\n", "", 0)

	task := &Task{
		Module: "script",
		Args:   map[string]any{"_raw_params": scriptPath},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Bad_UnsupportedModule(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "ansible.builtin.hostname",
		Args:   map[string]any{},
	}

	_, err := executeModuleWithMock(e, mock, "host1", task)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported module")
}

// --- Template integration tests ---

func TestModuleCommand_Good_TemplatedArgs(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("service_name", "nginx")
	mock.expectCommand("systemctl status nginx", "active\n", "", 0)

	task := &Task{
		Module: "command",
		Args:   map[string]any{"_raw_params": "systemctl status {{ service_name }}"},
	}

	// Template the args the way the executor does
	args := e.templateArgs(task.Args, "host1", task)
	result, err := moduleCommandWithClient(e, mock, args)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted("systemctl status nginx"))
}
