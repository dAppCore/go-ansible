package ansible

import (
	"context"
	core "dappco.re/go"
)

func TestLocalClient_Good_RunAndRunScript(t *core.T) {
	client := newLocalClient()

	run := client.Run(context.Background(), "printf 'hello\\n'")
	out := commandRunValue(run)
	core.RequireTrue(t, run.OK)
	core.AssertEqual(t, "hello\n", out.Stdout)
	core.AssertEqual(t, "", out.Stderr)
	core.AssertEqual(t, 0, out.ExitCode)

	run = client.RunScript(context.Background(), "printf 'script\\n'")
	out = commandRunValue(run)
	core.RequireTrue(t, run.OK)
	core.AssertEqual(t, "script\n", out.Stdout)
	core.AssertEqual(t, "", out.Stderr)
	core.AssertEqual(t, 0, out.ExitCode)
}

func TestLocalClient_Good_FileOperations(t *core.T) {
	client := newLocalClient()
	dir := t.TempDir()
	path := joinPath(dir, "nested", "file.txt")

	core.RequireTrue(t, client.Upload(context.Background(), newReader("content"), path, 0o644).OK)

	existsResult := client.FileExists(context.Background(), path)
	core.RequireTrue(t, existsResult.OK)
	core.AssertTrue(t, existsResult.Value.(bool))

	infoResult := client.Stat(context.Background(), path)
	core.RequireTrue(t, infoResult.OK)
	info := infoResult.Value.(map[string]any)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])

	contentResult := client.Download(context.Background(), path)
	core.RequireTrue(t, contentResult.OK)
	core.AssertEqual(t, []byte("content"), contentResult.Value.([]byte))
}

func TestExecutor_RunTaskOnHost_Good_LocalConnection(t *core.T) {
	e := NewExecutor("/tmp")

	task := &Task{
		Name:     "Local shell",
		Module:   "shell",
		Args:     map[string]any{"_raw_params": "printf 'local ok\\n'"},
		Register: "local_result",
	}
	play := &Play{Connection: "local"}

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, task, play))

	result := e.results["host1"]["local_result"]
	core.AssertNotNil(t, result)
	core.AssertEqual(t, "local ok\n", result.Stdout)
	core.AssertFalse(t, result.Failed)

	_, ok := e.clients["host1"].(*localClient)
	core.AssertTrue(t, ok)
}

func TestExecutor_GatherFacts_Good_LocalConnection(t *core.T) {
	e := NewExecutor("/tmp")

	core.RequireNoError(t, e.gatherFacts(context.Background(), "host1", &Play{Connection: "local"}))

	facts := e.facts["host1"]
	core.AssertNotNil(t, facts)
	core.AssertNotEmpty(t, facts.Hostname)
	core.AssertNotEmpty(t, facts.Kernel)
}

func TestLocalClient_Good_SetBecomeResetsStateWhenDisabled(t *core.T) {
	client := newLocalClient()

	client.SetBecome(true, "admin", "secret")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "secret", password)

	client.SetBecome(false, "", "")
	become, user, password = client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestAsyncClone_Good_DoesNotShareLocalClientState(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "admin", "secret")

	cloned := cloneClientMap(map[string]sshExecutorClient{"host1": client})

	clonedClient, ok := cloned["host1"].(*localClient)
	core.RequireTrue(t, ok)
	core.AssertFalse(t, client == clonedClient)

	become, user, password := clonedClient.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

// --- File-aware public symbol triplets ---

func TestLocalClient_Client_BecomeState_Good(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "deploy", "secret")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "deploy", user)
	core.AssertEqual(t, "secret", password)
}

func TestLocalClient_Client_BecomeState_Bad(t *core.T) {
	client := newLocalClient()
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestLocalClient_Client_BecomeState_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "", "")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestLocalClient_Client_SetBecome_Good(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	become, user, password := client.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "root", user)
	core.AssertEqual(t, "pw", password)
}

func TestLocalClient_Client_SetBecome_Bad(t *core.T) {
	client := newLocalClient()
	client.SetBecome(false, "root", "pw")
	become, user, password := client.BecomeState()
	core.AssertFalse(t, become)
	core.AssertEmpty(t, user)
	core.AssertEmpty(t, password)
}

func TestLocalClient_Client_SetBecome_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	client.SetBecome(true, "", "")
	_, user, password := client.BecomeState()
	core.AssertEqual(t, "root", user)
	core.AssertEqual(t, "pw", password)
}

func TestLocalClient_Client_Close_Good(t *core.T) {
	client := newLocalClient()
	result := client.Close()
	core.AssertTrue(t, result.OK)
	core.AssertNotNil(t, client)
}

func TestLocalClient_Client_Close_Bad(t *core.T) {
	client := newLocalClient()
	first := client.Close()
	second := client.Close()
	core.AssertTrue(t, first.OK)
	core.AssertTrue(t, second.OK)
}

func TestLocalClient_Client_Close_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	result := client.Close()
	core.AssertTrue(t, result.OK)
	core.AssertNotNil(t, client)
}

func TestLocalClient_Client_Run_Good(t *core.T) {
	client := newLocalClient()
	result := client.Run(context.Background(), "printf local")
	out := commandRunValue(result)
	core.AssertTrue(t, result.OK)
	core.AssertEqual(t, "local", out.Stdout)
	core.AssertEmpty(t, out.Stderr)
	core.AssertEqual(t, 0, out.ExitCode)
}

func TestLocalClient_Client_Run_Bad(t *core.T) {
	client := newLocalClient()
	result := client.Run(context.Background(), "exit 7")
	out := commandRunValue(result)
	core.AssertTrue(t, result.OK)
	core.AssertEmpty(t, out.Stdout)
	core.AssertEqual(t, 7, out.ExitCode)
}

func TestLocalClient_Client_Run_Ugly(t *core.T) {
	client := newLocalClient()
	result := client.Run(context.Background(), "")
	out := commandRunValue(result)
	core.AssertTrue(t, result.OK)
	core.AssertEmpty(t, out.Stdout)
	core.AssertEmpty(t, out.Stderr)
	core.AssertEqual(t, 0, out.ExitCode)
}

func TestLocalClient_Client_RunScript_Good(t *core.T) {
	client := newLocalClient()
	result := client.RunScript(context.Background(), "printf script")
	out := commandRunValue(result)
	core.AssertTrue(t, result.OK)
	core.AssertEqual(t, "script", out.Stdout)
	core.AssertEqual(t, 0, out.ExitCode)
}

func TestLocalClient_Client_RunScript_Bad(t *core.T) {
	client := newLocalClient()
	result := client.RunScript(context.Background(), "exit 9")
	out := commandRunValue(result)
	core.AssertTrue(t, result.OK)
	core.AssertEqual(t, 9, out.ExitCode)
}

func TestLocalClient_Client_RunScript_Ugly(t *core.T) {
	client := newLocalClient()
	result := client.RunScript(context.Background(), "\n")
	out := commandRunValue(result)
	core.AssertTrue(t, result.OK)
	core.AssertEmpty(t, out.Stdout)
	core.AssertEmpty(t, out.Stderr)
	core.AssertEqual(t, 0, out.ExitCode)
}

func TestLocalClient_Client_Upload_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "remote.txt")
	result := client.Upload(context.Background(), newReader("payload"), path, 0o644)
	core.AssertTrue(t, result.OK)
	content, readErr := readTestFile(path)
	core.AssertNoError(t, readErr)
	core.AssertEqual(t, "payload", string(content))
}

func TestLocalClient_Client_Upload_Bad(t *core.T) {
	client := newLocalClient()
	result := client.Upload(context.Background(), newReader("payload"), "", 0o644)
	core.AssertFalse(t, result.OK)
	core.AssertContains(t, result.Error(), "write remote file")
}

func TestLocalClient_Client_Upload_Ugly(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "nested", "remote.txt")
	result := client.Upload(context.Background(), newReader(""), path, 0)
	core.AssertTrue(t, result.OK)
	info, statErr := statTestFile(path)
	core.AssertNoError(t, statErr)
	core.AssertEqual(t, core.FileMode(0), info.Mode().Perm())
}

func TestLocalClient_Client_Download_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "remote.txt")
	writeTextFile(t, path, "download")
	result := client.Download(context.Background(), path)
	core.AssertTrue(t, result.OK)
	core.AssertEqual(t, "download", string(result.Value.([]byte)))
}

func TestLocalClient_Client_Download_Bad(t *core.T) {
	client := newLocalClient()
	result := client.Download(context.Background(), joinPath(t.TempDir(), "missing.txt"))
	core.AssertFalse(t, result.OK)
	core.AssertContains(t, resultErrorMessage(result), "read remote file")
}

func TestLocalClient_Client_Download_Ugly(t *core.T) {
	client := newLocalClient()
	result := client.Download(context.Background(), "")
	core.AssertFalse(t, result.OK)
	core.AssertContains(t, resultErrorMessage(result), "read remote file")
}

func TestLocalClient_Client_FileExists_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "exists.txt")
	writeTextFile(t, path, "x")
	result := client.FileExists(context.Background(), path)
	core.AssertTrue(t, result.OK)
	core.AssertTrue(t, result.Value.(bool))
}

func TestLocalClient_Client_FileExists_Bad(t *core.T) {
	client := newLocalClient()
	result := client.FileExists(context.Background(), joinPath(t.TempDir(), "missing.txt"))
	core.AssertTrue(t, result.OK)
	core.AssertFalse(t, result.Value.(bool))
}

func TestLocalClient_Client_FileExists_Ugly(t *core.T) {
	client := newLocalClient()
	result := client.FileExists(context.Background(), string([]byte{'b', 0, 'd'}))
	core.AssertFalse(t, result.OK)
}

func TestLocalClient_Client_Stat_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "exists.txt")
	writeTextFile(t, path, "x")
	result := client.Stat(context.Background(), path)
	core.AssertTrue(t, result.OK)
	info := result.Value.(map[string]any)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])
}

func TestLocalClient_Client_Stat_Bad(t *core.T) {
	client := newLocalClient()
	result := client.Stat(context.Background(), joinPath(t.TempDir(), "missing.txt"))
	core.AssertTrue(t, result.OK)
	info := result.Value.(map[string]any)
	core.AssertEqual(t, false, info["exists"])
}

func TestLocalClient_Client_Stat_Ugly(t *core.T) {
	client := newLocalClient()
	dir := t.TempDir()
	result := client.Stat(context.Background(), dir)
	core.AssertTrue(t, result.OK)
	info := result.Value.(map[string]any)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, true, info["isdir"])
}
