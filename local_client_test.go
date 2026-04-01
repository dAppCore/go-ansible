package ansible

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalClient_Good_RunAndRunScript(t *testing.T) {
	client := newLocalClient()

	stdout, stderr, rc, err := client.Run(context.Background(), "printf 'hello\\n'")
	require.NoError(t, err)
	assert.Equal(t, "hello\n", stdout)
	assert.Equal(t, "", stderr)
	assert.Equal(t, 0, rc)

	stdout, stderr, rc, err = client.RunScript(context.Background(), "printf 'script\\n'")
	require.NoError(t, err)
	assert.Equal(t, "script\n", stdout)
	assert.Equal(t, "", stderr)
	assert.Equal(t, 0, rc)
}

func TestLocalClient_Good_FileOperations(t *testing.T) {
	client := newLocalClient()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "file.txt")

	require.NoError(t, client.Upload(context.Background(), newReader("content"), path, 0o644))

	exists, err := client.FileExists(context.Background(), path)
	require.NoError(t, err)
	assert.True(t, exists)

	info, err := client.Stat(context.Background(), path)
	require.NoError(t, err)
	assert.Equal(t, true, info["exists"])
	assert.Equal(t, false, info["isdir"])

	content, err := client.Download(context.Background(), path)
	require.NoError(t, err)
	assert.Equal(t, []byte("content"), content)
}

func TestExecutor_RunTaskOnHost_Good_LocalConnection(t *testing.T) {
	e := NewExecutor("/tmp")

	task := &Task{
		Name:     "Local shell",
		Module:   "shell",
		Args:     map[string]any{"_raw_params": "printf 'local ok\\n'"},
		Register: "local_result",
	}
	play := &Play{Connection: "local"}

	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, task, play))

	result := e.results["host1"]["local_result"]
	require.NotNil(t, result)
	assert.Equal(t, "local ok\n", result.Stdout)
	assert.False(t, result.Failed)

	_, ok := e.clients["host1"].(*localClient)
	assert.True(t, ok)
}

func TestExecutor_GatherFacts_Good_LocalConnection(t *testing.T) {
	e := NewExecutor("/tmp")

	require.NoError(t, e.gatherFacts(context.Background(), "host1", &Play{Connection: "local"}))

	facts := e.facts["host1"]
	require.NotNil(t, facts)
	assert.NotEmpty(t, facts.Hostname)
	assert.NotEmpty(t, facts.Kernel)
}
