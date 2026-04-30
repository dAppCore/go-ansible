package ansible

import (
	"context"
	core "dappco.re/go"
)

// ============================================================
// Step 1.1: command / shell / raw / script module tests
// ============================================================

// --- MockSSHClient basic tests ---

func TestModulesCmd_MockSSHClient_Good_RunRecordsExecution(t *core.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("echo hello", "hello\n", "", 0)

	runResult := mock.Run(nil, "echo hello")
	output := commandRunValue(runResult)

	core.AssertTrue(t, runResult.OK)
	core.AssertEqual(t, "hello\n", output.Stdout)
	core.AssertEqual(t, "", output.Stderr)
	core.AssertEqual(t, 0, output.ExitCode)
	core.AssertEqual(t, 1, mock.commandCount())
	core.AssertEqual(t, "Run", mock.lastCommand().Method)
	core.AssertEqual(t, "echo hello", mock.lastCommand().Cmd)
}

func TestModulesCmd_MockSSHClient_Good_RunScriptRecordsExecution(t *core.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("set -e", "ok", "", 0)

	runResult := mock.RunScript(nil, "set -e\necho done")
	output := commandRunValue(runResult)

	core.AssertTrue(t, runResult.OK)
	core.AssertEqual(t, "ok", output.Stdout)
	core.AssertEqual(t, 0, output.ExitCode)
	core.AssertEqual(t, 1, mock.commandCount())
	core.AssertEqual(t, "RunScript", mock.lastCommand().Method)
}

func TestModulesCmd_MockSSHClient_Good_DefaultSuccessResponse(t *core.T) {
	mock := NewMockSSHClient()

	// No expectations registered — should return empty success
	runResult := mock.Run(nil, "anything")
	output := commandRunValue(runResult)

	core.AssertTrue(t, runResult.OK)
	core.AssertEqual(t, "", output.Stdout)
	core.AssertEqual(t, "", output.Stderr)
	core.AssertEqual(t, 0, output.ExitCode)
}

func TestModulesCmd_MockSSHClient_Good_LastMatchWins(t *core.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("echo", "first", "", 0)
	mock.expectCommand("echo", "second", "", 0)

	output := commandRunValue(mock.Run(nil, "echo hello"))

	core.AssertEqual(t, "second", output.Stdout)
}

func TestModulesCmd_MockSSHClient_Good_FileOperations(t *core.T) {
	mock := NewMockSSHClient()

	// File does not exist initially
	existsResult := mock.FileExists(nil, "/etc/config")
	core.AssertTrue(t, existsResult.OK)
	core.AssertFalse(t, existsResult.Value.(bool))

	// Add file
	mock.addFile("/etc/config", []byte("key=value"))

	// Now it exists
	existsResult = mock.FileExists(nil, "/etc/config")
	core.AssertTrue(t, existsResult.OK)
	core.AssertTrue(t, existsResult.Value.(bool))

	// Download it
	contentResult := mock.Download(nil, "/etc/config")
	core.AssertTrue(t, contentResult.OK)
	core.AssertEqual(t, []byte("key=value"), contentResult.Value.([]byte))

	// Download non-existent file
	missingResult := mock.Download(nil, "/nonexistent")
	core.AssertFalse(t, missingResult.OK)
}

func TestModulesCmd_MockSSHClient_Good_StatWithExplicit(t *core.T) {
	mock := NewMockSSHClient()
	mock.addStat("/var/log", map[string]any{"exists": true, "isdir": true})

	statResult := mock.Stat(nil, "/var/log")
	core.AssertTrue(t, statResult.OK)
	info := statResult.Value.(map[string]any)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, true, info["isdir"])
}

func TestModulesCmd_MockSSHClient_Good_StatFallback(t *core.T) {
	mock := NewMockSSHClient()
	mock.addFile("/etc/hosts", []byte("127.0.0.1 localhost"))

	statResult := mock.Stat(nil, "/etc/hosts")
	core.AssertTrue(t, statResult.OK)
	info := statResult.Value.(map[string]any)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])

	statResult = mock.Stat(nil, "/nonexistent")
	core.AssertTrue(t, statResult.OK)
	info = statResult.Value.(map[string]any)
	core.AssertEqual(t, false, info["exists"])
}

func TestModulesCmd_MockSSHClient_Good_BecomeTracking(t *core.T) {
	mock := NewMockSSHClient()

	core.AssertFalse(t, mock.become)
	core.AssertEqual(t, "", mock.becomeUser)

	mock.SetBecome(true, "root", "secret")

	core.AssertTrue(t, mock.become)
	core.AssertEqual(t, "root", mock.becomeUser)
	core.AssertEqual(t, "secret", mock.becomePass)
}

func TestModulesCmd_ModuleScript_Good_RelativePathResolvedAgainstBasePath(t *core.T) {
	dir := t.TempDir()
	scriptPath := joinPath(dir, "scripts", "deploy.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("echo deploy"), 0755))

	e := NewExecutor(dir)
	mock := NewMockSSHClient()
	mock.expectCommand("echo deploy", "deploy\n", "", 0)

	result := requireTaskResult(t, e.moduleScript(context.Background(), mock, map[string]any{
		"_raw_params": "scripts/deploy.sh",
	}))

	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "deploy\n", result.Stdout)
	core.AssertTrue(t, mock.hasExecuted("echo deploy"))
}

func TestModulesCmd_MockSSHClient_Good_HasExecuted(t *core.T) {
	mock := NewMockSSHClient()
	core.RequireTrue(t, mock.Run(nil, "systemctl restart nginx").OK)
	core.RequireTrue(t, mock.Run(nil, "apt-get update").OK)

	core.AssertTrue(t, mock.hasExecuted("systemctl.*nginx"))
	core.AssertTrue(t, mock.hasExecuted("apt-get"))
	core.AssertFalse(t, mock.hasExecuted("yum"))
}

func TestModulesCmd_MockSSHClient_Good_HasExecutedMethod(t *core.T) {
	mock := NewMockSSHClient()
	core.RequireTrue(t, mock.Run(nil, "echo run").OK)
	core.RequireTrue(t, mock.RunScript(nil, "echo script").OK)

	core.AssertTrue(t, mock.hasExecutedMethod("Run", "echo run"))
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "echo script"))
	core.AssertFalse(t, mock.hasExecutedMethod("Run", "echo script"))
	core.AssertFalse(t, mock.hasExecutedMethod("RunScript", "echo run"))
}

func TestModulesCmd_MockSSHClient_Good_Reset(t *core.T) {
	mock := NewMockSSHClient()
	core.RequireTrue(t, mock.Run(nil, "echo hello").OK)
	core.AssertEqual(t, 1, mock.commandCount())

	mock.reset()
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestModulesCmd_MockSSHClient_Good_ErrorExpectation(t *core.T) {
	mock := NewMockSSHClient()
	mock.expectCommandError("bad cmd", core.AnError)

	result := mock.Run(nil, "bad cmd")
	core.AssertFalse(t, result.OK)
}

// --- command module ---

func TestModulesCmd_ModuleCommand_Good_BasicCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("ls -la /tmp", "total 0\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "ls -la /tmp",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "total 0\n", result.Stdout)
	core.AssertEqual(t, 0, result.RC)

	// Verify it used Run (not RunScript)
	core.AssertTrue(t, mock.hasExecutedMethod("Run", "ls -la /tmp"))
	core.AssertFalse(t, mock.hasExecutedMethod("RunScript", ".*"))
}

func TestModulesCmd_ModuleCommand_Good_CmdArg(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("whoami", "root\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"cmd": "whoami",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "root\n", result.Stdout)
	core.AssertTrue(t, mock.hasExecutedMethod("Run", "whoami"))
}

func TestModulesCmd_ModuleCommand_Good_Argv(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`"echo".*"hello world"`, "hello world\n", "", 0)

	task := &Task{
		Module: "command",
		Args: map[string]any{
			"argv": []any{"echo", "hello world"},
		},
	}

	result := requireTaskResult(t, e.executeModule(context.Background(), "host1", mock, task, &Play{}))

	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "hello world\n", result.Stdout)
	core.AssertTrue(t, mock.hasExecuted(`hello world`))
}

func TestModulesCmd_ModuleCommand_Good_WithChdir(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`cd "/var/log" && ls`, "syslog\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "ls",
		"chdir":       "/var/log",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	// The command should have been wrapped with cd
	last := mock.lastCommand()
	core.AssertEqual(t, "Run", last.Method)
	core.AssertContains(t, last.Cmd, `cd "/var/log"`)
	core.AssertContains(t, last.Cmd, "ls")
}

func TestModulesCmd_ModuleCommand_Good_WithStdin(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("cat", "input\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "cat",
		"stdin":       "payload",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "input\n", result.Stdout)
	last := mock.lastCommand()
	core.AssertEqual(t, "Run", last.Method)
	core.AssertContains(t, last.Cmd, "printf %s")
	core.AssertContains(t, last.Cmd, "| cat")
	core.AssertContains(t, last.Cmd, "payload\n")
}

func TestModulesCmd_ModuleCommand_Bad_NoCommand(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleCommandWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no command specified")
}

func TestModulesCmd_ModuleCommand_Good_NonZeroRC(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("false", "", "error occurred", 1)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "false",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, 1, result.RC)
	core.AssertEqual(t, "error occurred", result.Stderr)
}

func TestModulesCmd_ModuleCommand_Good_SSHError(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", core.AnError)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "any command",
	})

	core.RequireNoError(t, err) // Module wraps SSH errors into result.Failed
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, core.AnError.Error())
}

func TestModulesCmd_ModuleCommand_Good_RawParamsTakesPrecedence(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("from_raw", "raw\n", "", 0)

	result, err := moduleCommandWithClient(e, mock, map[string]any{
		"_raw_params": "from_raw",
		"cmd":         "from_cmd",
	})

	core.RequireNoError(t, err)
	core.AssertEqual(t, "raw\n", result.Stdout)
	core.AssertTrue(t, mock.hasExecuted("from_raw"))
}

func TestModulesCmd_ModuleCommand_Good_SkipsWhenCreatesExists(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/output.txt", []byte("done"))

	task := &Task{
		Module: "ansible.builtin.command",
		Args: map[string]any{
			"_raw_params": "echo should-not-run",
			"creates":     "/tmp/output.txt",
		},
	}

	result := requireTaskResult(t, e.executeModule(context.Background(), "host1", mock, task, &Play{}))
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestModulesCmd_ModuleCommand_Good_SkipsWhenCreatesExistsUnderChdir(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/app/build/output.txt", []byte("done"))

	task := &Task{
		Module: "ansible.builtin.command",
		Args: map[string]any{
			"_raw_params": "echo should-not-run",
			"creates":     "build/output.txt",
			"chdir":       "/app",
		},
	}

	result := requireTaskResult(t, e.executeModule(context.Background(), "host1", mock, task, &Play{}))

	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.commandCount())
}

// --- shell module ---

func TestModulesCmd_ModuleShell_Good_BasicShell(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo hello", "hello\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "echo hello",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "hello\n", result.Stdout)

	// Shell must use RunScript (not Run)
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "echo hello"))
	core.AssertFalse(t, mock.hasExecutedMethod("Run", ".*"))
}

func TestModulesCmd_ModuleShell_Good_CmdArg(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("date", "Thu Feb 20\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"cmd": "date",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "date"))
}

func TestModulesCmd_ModuleShell_Good_WithChdir(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`cd "/app" && npm install`, "done\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "npm install",
		"chdir":       "/app",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	last := mock.lastCommand()
	core.AssertEqual(t, "RunScript", last.Method)
	core.AssertContains(t, last.Cmd, `cd "/app"`)
	core.AssertContains(t, last.Cmd, "npm install")
}

func TestModulesCmd_ModuleShell_Good_ExecutableUsesRun(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`/bin/dash.*echo test`, "test\n", "", 0)

	result := requireTaskResult(t, e.moduleShell(context.Background(), mock, map[string]any{
		"_raw_params": "echo test",
		"executable":  "/bin/dash",
	}))

	core.AssertTrue(t, result.Changed)

	last := mock.lastCommand()
	core.AssertNotNil(t, last)
	core.AssertEqual(t, "Run", last.Method)
	core.AssertContains(t, last.Cmd, "/bin/dash")
	core.AssertContains(t, last.Cmd, "-c")
	core.AssertContains(t, last.Cmd, "echo test")
}

func TestModulesCmd_ModuleShell_Bad_NoCommand(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleShellWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no command specified")
}

func TestModulesCmd_ModuleShell_Good_NonZeroRC(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("exit 2", "", "failed", 2)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "exit 2",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, 2, result.RC)
}

func TestModulesCmd_ModuleShell_Good_SkipsWhenRemovesMissing(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "ansible.builtin.shell",
		Args: map[string]any{
			"_raw_params": "echo should-not-run",
			"removes":     "/tmp/missing.txt",
		},
	}

	result := requireTaskResult(t, e.executeModule(context.Background(), "host1", mock, task, &Play{}))
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestModulesCmd_ModuleShell_Good_SSHError(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", core.AnError)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "some command",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
}

func TestModulesCmd_ModuleShell_Good_PipelineCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`cat /etc/passwd \| grep root`, "root:x:0:0\n", "", 0)

	result, err := moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "cat /etc/passwd | grep root",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	// Shell uses RunScript, so pipes work
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "cat /etc/passwd"))
}

// --- raw module ---

func TestModulesCmd_ModuleRaw_Good_BasicRaw(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("uname -a", "Linux host1 5.15\n", "", 0)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "uname -a",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "Linux host1 5.15\n", result.Stdout)

	// Raw must use Run (not RunScript) — no shell wrapping
	core.AssertTrue(t, mock.hasExecutedMethod("Run", "uname -a"))
	core.AssertFalse(t, mock.hasExecutedMethod("RunScript", ".*"))
}

func TestModulesCmd_ModuleRaw_Bad_NoCommand(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleRawWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no command specified")
}

func TestModulesCmd_ModuleRaw_Good_NoChdir(t *core.T) {
	// Raw module does NOT support chdir — it should ignore it
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "echo test",
		"chdir":       "/should/be/ignored",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	// The chdir should NOT appear in the command
	last := mock.lastCommand()
	core.AssertEqual(t, "echo test", last.Cmd)
	core.AssertNotContains(t, last.Cmd, "cd")
}

func TestModulesCmd_ModuleRaw_Good_NonZeroRC(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("invalid", "", "not found", 127)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "invalid",
	})

	core.RequireNoError(t, err)
	// Note: raw module does NOT set Failed based on RC
	core.AssertEqual(t, 127, result.RC)
	core.AssertEqual(t, "not found", result.Stderr)
}

func TestModulesCmd_ModuleRaw_Good_SSHError(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", core.AnError)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": "any",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
}

func TestModulesCmd_ModuleRaw_Good_ExactCommandPassthrough(t *core.T) {
	// Raw should pass the command exactly as given — no wrapping
	e, mock := newTestExecutorWithMock("host1")
	complexCmd := `/usr/bin/python3 -c 'import sys; print(sys.version)'`
	mock.expectCommand(".*python3.*", "3.10.0\n", "", 0)

	result, err := moduleRawWithClient(e, mock, map[string]any{
		"_raw_params": complexCmd,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	last := mock.lastCommand()
	core.AssertEqual(t, complexCmd, last.Cmd)
}

// --- script module ---

func TestModulesCmd_ModuleScript_Good_BasicScript(t *core.T) {
	// Create a temporary script file
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "setup.sh")
	scriptContent := "#!/bin/bash\necho 'setup complete'\nexit 0"
	core.RequireNoError(t, writeTestFile(scriptPath, []byte(scriptContent), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("setup complete", "setup complete\n", "", 0)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)

	// Script must use RunScript (not Run) — it sends the file content
	core.AssertTrue(t, mock.hasExecutedMethod("RunScript", "setup complete"))
	core.AssertFalse(t, mock.hasExecutedMethod("Run", ".*"))

	// Verify the full script content was sent
	last := mock.lastCommand()
	core.AssertEqual(t, scriptContent, last.Cmd)
}

func TestModulesCmd_ModuleScript_Good_CreatesSkipsExecution(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "setup.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("echo should-not-run"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/already-there", []byte("present"))

	result := requireTaskResult(t, e.moduleScript(context.Background(), mock, map[string]any{
		"_raw_params": scriptPath,
		"creates":     "/tmp/already-there",
	}))

	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestModulesCmd_ModuleScript_Good_ChdirPrefixesScript(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "work.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("pwd"), 0755))

	e, mock := newTestExecutorWithMock("host1")

	result := requireTaskResult(t, e.moduleScript(context.Background(), mock, map[string]any{
		"_raw_params": scriptPath,
		"chdir":       "/opt/app",
	}))

	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)

	last := mock.lastCommand()
	core.AssertNotNil(t, last)
	core.AssertEqual(t, "RunScript", last.Method)
	core.AssertEqual(t, `cd "/opt/app" && pwd`, last.Cmd)
}

func TestModulesCmd_ModuleScript_Good_ExecutableUsesRun(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "dash.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("echo script works"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`/bin/dash.*echo script works`, "script works\n", "", 0)

	result := requireTaskResult(t, e.moduleScript(context.Background(), mock, map[string]any{
		"_raw_params": scriptPath,
		"executable":  "/bin/dash",
	}))

	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)

	last := mock.lastCommand()
	core.AssertNotNil(t, last)
	core.AssertEqual(t, "Run", last.Method)
	core.AssertContains(t, last.Cmd, "/bin/dash")
	core.AssertContains(t, last.Cmd, "-c")
	core.AssertContains(t, last.Cmd, "echo script works")
}

func TestModulesCmd_ModuleScript_Bad_NoScript(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleScriptWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "no script specified")
}

func TestModulesCmd_ModuleScript_Bad_FileNotFound(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": "/nonexistent/script.sh",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "read script")
}

func TestModulesCmd_ModuleScript_Good_NonZeroRC(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "fail.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("exit 1"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("exit 1", "", "script failed", 1)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, 1, result.RC)
}

func TestModulesCmd_ModuleScript_Good_MultiLineScript(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "multi.sh")
	scriptContent := "#!/bin/bash\nset -e\napt-get update\napt-get install -y nginx\nsystemctl start nginx"
	core.RequireNoError(t, writeTestFile(scriptPath, []byte(scriptContent), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("apt-get", "done\n", "", 0)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)

	// Verify RunScript was called with the full content
	last := mock.lastCommand()
	core.AssertEqual(t, "RunScript", last.Method)
	core.AssertEqual(t, scriptContent, last.Cmd)
}

func TestModulesCmd_ModuleScript_Good_SSHError(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "ok.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("echo ok"), 0755))

	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommandError(".*", core.AnError)

	result, err := moduleScriptWithClient(e, mock, map[string]any{
		"_raw_params": scriptPath,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
}

// --- Cross-module differentiation tests ---

func TestModulesCmd_ModuleDifferentiation_Good_CommandUsesRun(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleCommandWithClient(e, mock, map[string]any{"_raw_params": "echo test"})

	cmds := mock.executedCommands()
	core.AssertLen(t, cmds, 1)
	core.AssertEqual(t, "Run", cmds[0].Method, "command module must use Run()")
}

func TestModulesCmd_ModuleDifferentiation_Good_ShellUsesRunScript(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleShellWithClient(e, mock, map[string]any{"_raw_params": "echo test"})

	cmds := mock.executedCommands()
	core.AssertLen(t, cmds, 1)
	core.AssertEqual(t, "RunScript", cmds[0].Method, "shell module must use RunScript()")
}

func TestModulesCmd_ModuleDifferentiation_Good_ShellWithStdinStillUsesRunScript(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleShellWithClient(e, mock, map[string]any{
		"_raw_params": "echo test",
		"stdin":       "payload",
	})

	cmds := mock.executedCommands()
	core.AssertLen(t, cmds, 1)
	core.AssertEqual(t, "RunScript", cmds[0].Method, "shell module must still use RunScript()")
	core.AssertContains(t, cmds[0].Cmd, "printf %s")
	core.AssertContains(t, cmds[0].Cmd, "| echo test")
}

func TestModulesCmd_ModuleDifferentiation_Good_RawUsesRun(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleRawWithClient(e, mock, map[string]any{"_raw_params": "echo test"})

	cmds := mock.executedCommands()
	core.AssertLen(t, cmds, 1)
	core.AssertEqual(t, "Run", cmds[0].Method, "raw module must use Run()")
}

func TestModulesCmd_ModuleDifferentiation_Good_ScriptUsesRunScript(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "test.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("echo test"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo test", "test\n", "", 0)

	_, _ = moduleScriptWithClient(e, mock, map[string]any{"_raw_params": scriptPath})

	cmds := mock.executedCommands()
	core.AssertLen(t, cmds, 1)
	core.AssertEqual(t, "RunScript", cmds[0].Method, "script module must use RunScript()")
}

// --- executeModuleWithMock dispatch tests ---

func TestModulesCmd_ExecuteModuleWithMock_Good_DispatchCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("uptime", "up 5 days\n", "", 0)

	task := &Task{
		Module: "command",
		Args:   map[string]any{"_raw_params": "uptime"},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "up 5 days\n", result.Stdout)
}

func TestModulesCmd_ExecuteModuleWithMock_Good_DispatchShell(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("ps aux", "root.*bash\n", "", 0)

	task := &Task{
		Module: "ansible.builtin.shell",
		Args:   map[string]any{"_raw_params": "ps aux | grep bash"},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesCmd_ExecuteModuleWithMock_Good_DispatchRaw(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("cat /etc/hostname", "web01\n", "", 0)

	task := &Task{
		Module: "raw",
		Args:   map[string]any{"_raw_params": "cat /etc/hostname"},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "web01\n", result.Stdout)
}

func TestModulesCmd_ExecuteModuleWithMock_Good_DispatchScript(t *core.T) {
	tmpDir := t.TempDir()
	scriptPath := joinPath(tmpDir, "deploy.sh")
	core.RequireNoError(t, writeTestFile(scriptPath, []byte("echo deploying"), 0755))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("deploying", "deploying\n", "", 0)

	task := &Task{
		Module: "script",
		Args:   map[string]any{"_raw_params": scriptPath},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesCmd_ExecuteModuleWithMock_Bad_UnsupportedModule(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		Module: "ansible.builtin.hostname",
		Args:   map[string]any{},
	}

	_, err := executeModuleWithMock(e, mock, "host1", task)

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "unsupported module")
	core.AssertContains(t, err.Error(), "ansible.builtin.hostname")
}

// --- Template integration tests ---

func TestModulesCmd_ModuleCommand_Good_TemplatedArgs(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted("systemctl status nginx"))
}
