package ansible

import (
	"context"
	core "dappco.re/go"
	"path/filepath"
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
	path := filepath.Join(dir, "nested", "file.txt")

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
