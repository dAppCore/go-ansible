package ansible

import (
	"context"
	core "dappco.re/go"
)

func TestLocalClient_Good_RunAndRunScript(t *core.T) {
	client := newLocalClient()

	stdout, stderr, rc, err := client.Run(context.Background(), "printf 'hello\\n'")
	core.RequireNoError(t, err)
	core.AssertEqual(t, "hello\n", stdout)
	core.AssertEqual(t, "", stderr)
	core.AssertEqual(t, 0, rc)

	stdout, stderr, rc, err = client.RunScript(context.Background(), "printf 'script\\n'")
	core.RequireNoError(t, err)
	core.AssertEqual(t, "script\n", stdout)
	core.AssertEqual(t, "", stderr)
	core.AssertEqual(t, 0, rc)
}

func TestLocalClient_Good_FileOperations(t *core.T) {
	client := newLocalClient()
	dir := t.TempDir()
	path := joinPath(dir, "nested", "file.txt")

	core.RequireNoError(t, client.Upload(context.Background(), newReader("content"), path, 0o644))

	exists, err := client.FileExists(context.Background(), path)
	core.RequireNoError(t, err)
	core.AssertTrue(t, exists)

	info, err := client.Stat(context.Background(), path)
	core.RequireNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])

	content, err := client.Download(context.Background(), path)
	core.RequireNoError(t, err)
	core.AssertEqual(t, []byte("content"), content)
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
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client)
}

func TestLocalClient_Client_Close_Bad(t *core.T) {
	client := newLocalClient()
	first := client.Close()
	second := client.Close()
	core.AssertNoError(t, first)
	core.AssertNoError(t, second)
}

func TestLocalClient_Client_Close_Ugly(t *core.T) {
	client := newLocalClient()
	client.SetBecome(true, "root", "pw")
	err := client.Close()
	core.AssertNoError(t, err)
	core.AssertNotNil(t, client)
}

func TestLocalClient_Client_Run_Good(t *core.T) {
	client := newLocalClient()
	stdout, stderr, code, err := client.Run(context.Background(), "printf local")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "local", stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestLocalClient_Client_Run_Bad(t *core.T) {
	client := newLocalClient()
	stdout, _, code, err := client.Run(context.Background(), "exit 7")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEqual(t, 7, code)
}

func TestLocalClient_Client_Run_Ugly(t *core.T) {
	client := newLocalClient()
	stdout, stderr, code, err := client.Run(context.Background(), "")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestLocalClient_Client_RunScript_Good(t *core.T) {
	client := newLocalClient()
	stdout, _, code, err := client.RunScript(context.Background(), "printf script")
	core.AssertNoError(t, err)
	core.AssertEqual(t, "script", stdout)
	core.AssertEqual(t, 0, code)
}

func TestLocalClient_Client_RunScript_Bad(t *core.T) {
	client := newLocalClient()
	_, _, code, err := client.RunScript(context.Background(), "exit 9")
	core.AssertNoError(t, err)
	core.AssertEqual(t, 9, code)
}

func TestLocalClient_Client_RunScript_Ugly(t *core.T) {
	client := newLocalClient()
	stdout, stderr, code, err := client.RunScript(context.Background(), "\n")
	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
}

func TestLocalClient_Client_Upload_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "remote.txt")
	err := client.Upload(context.Background(), newReader("payload"), path, 0o644)
	core.AssertNoError(t, err)
	content, readErr := readTestFile(path)
	core.AssertNoError(t, readErr)
	core.AssertEqual(t, "payload", string(content))
}

func TestLocalClient_Client_Upload_Bad(t *core.T) {
	client := newLocalClient()
	err := client.Upload(context.Background(), newReader("payload"), "", 0o644)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "write remote file")
}

func TestLocalClient_Client_Upload_Ugly(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "nested", "remote.txt")
	err := client.Upload(context.Background(), newReader(""), path, 0)
	core.AssertNoError(t, err)
	info, statErr := statTestFile(path)
	core.AssertNoError(t, statErr)
	core.AssertEqual(t, core.FileMode(0), info.Mode().Perm())
}

func TestLocalClient_Client_Download_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "remote.txt")
	writeTextFile(t, path, "download")
	data, err := client.Download(context.Background(), path)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "download", string(data))
}

func TestLocalClient_Client_Download_Bad(t *core.T) {
	client := newLocalClient()
	data, err := client.Download(context.Background(), joinPath(t.TempDir(), "missing.txt"))
	core.AssertError(t, err)
	core.AssertNil(t, data)
}

func TestLocalClient_Client_Download_Ugly(t *core.T) {
	client := newLocalClient()
	data, err := client.Download(context.Background(), "")
	core.AssertError(t, err)
	core.AssertNil(t, data)
}

func TestLocalClient_Client_FileExists_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "exists.txt")
	writeTextFile(t, path, "x")
	exists, err := client.FileExists(context.Background(), path)
	core.AssertNoError(t, err)
	core.AssertTrue(t, exists)
}

func TestLocalClient_Client_FileExists_Bad(t *core.T) {
	client := newLocalClient()
	exists, err := client.FileExists(context.Background(), joinPath(t.TempDir(), "missing.txt"))
	core.AssertNoError(t, err)
	core.AssertFalse(t, exists)
}

func TestLocalClient_Client_FileExists_Ugly(t *core.T) {
	client := newLocalClient()
	exists, err := client.FileExists(context.Background(), string([]byte{'b', 0, 'd'}))
	core.AssertError(t, err)
	core.AssertFalse(t, exists)
}

func TestLocalClient_Client_Stat_Good(t *core.T) {
	client := newLocalClient()
	path := joinPath(t.TempDir(), "exists.txt")
	writeTextFile(t, path, "x")
	info, err := client.Stat(context.Background(), path)
	core.AssertNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, false, info["isdir"])
}

func TestLocalClient_Client_Stat_Bad(t *core.T) {
	client := newLocalClient()
	info, err := client.Stat(context.Background(), joinPath(t.TempDir(), "missing.txt"))
	core.AssertNoError(t, err)
	core.AssertEqual(t, false, info["exists"])
}

func TestLocalClient_Client_Stat_Ugly(t *core.T) {
	client := newLocalClient()
	dir := t.TempDir()
	info, err := client.Stat(context.Background(), dir)
	core.AssertNoError(t, err)
	core.AssertEqual(t, true, info["exists"])
	core.AssertEqual(t, true, info["isdir"])
}
