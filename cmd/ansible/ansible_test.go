package ansiblecmd

import (
	"os"
	"path/filepath"
	"testing"

	"dappco.re/go/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtraVars_Good_RepeatableAndCommaSeparated(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "version=1.2.3,env=prod"},
		core.Option{Key: "extra-vars", Value: "region=us-east-1"},
		core.Option{Key: "extra-vars", Value: []string{"build=42"}},
	)

	vars, err := extraVars(opts)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"version": "1.2.3",
		"env":     "prod",
		"region":  "us-east-1",
		"build":   "42",
	}, vars)
}

func TestExtraVars_Good_UsesShortAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "e", Value: "version=1.2.3,env=prod"},
	)

	vars, err := extraVars(opts)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"version": "1.2.3",
		"env":     "prod",
	}, vars)
}

func TestExtraVars_Good_IgnoresMalformedPairs(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "missing_equals,keep=this"},
		core.Option{Key: "extra-vars", Value: "also_bad="},
	)

	vars, err := extraVars(opts)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"keep":     "this",
		"also_bad": "",
	}, vars)
}

func TestExtraVars_Good_SupportsStructuredYAMLAndJSON(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "app:\n  port: 8080\n  debug: true"},
		core.Option{Key: "extra-vars", Value: `{"image":"nginx:latest","replicas":3}`},
	)

	vars, err := extraVars(opts)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"app": map[string]any{
			"port":  int(8080),
			"debug": true,
		},
		"image":    "nginx:latest",
		"replicas": int(3),
	}, vars)
}

func TestExtraVars_Good_LoadsFileReferences(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vars.yml")
	require.NoError(t, os.WriteFile(path, []byte("deploy_env: prod\nrelease: 42\n"), 0644))

	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "@" + path},
	)

	vars, err := extraVars(opts)
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"deploy_env": "prod",
		"release":    int(42),
	}, vars)
}

func TestExtraVars_Bad_MissingFile(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "@/definitely/missing/vars.yml"},
	)

	_, err := extraVars(opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read extra vars file")
}

func TestFirstString_Good_PrefersFirstNonEmptyKey(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "inventory", Value: ""},
		core.Option{Key: "i", Value: "/tmp/inventory.yml"},
	)

	assert.Equal(t, "/tmp/inventory.yml", firstStringOption(opts, "inventory", "i"))
}

func TestFirstBool_Good_UsesAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "v", Value: true},
	)

	assert.True(t, firstBoolOption(opts, "verbose", "v"))
}

func TestVerbosityLevel_Good_CountsStackedShortFlags(t *testing.T) {
	opts := core.NewOptions()

	assert.Equal(t, 3, verbosityLevel(opts, []string{"-vvv"}))
}

func TestVerbosityLevel_Good_CountsLongForm(t *testing.T) {
	opts := core.NewOptions()

	assert.Equal(t, 1, verbosityLevel(opts, []string{"--verbose"}))
}

func TestVerbosityLevel_Good_PreservesExplicitNumericLevel(t *testing.T) {
	opts := core.NewOptions(core.Option{Key: "verbose", Value: 2})

	assert.Equal(t, 2, verbosityLevel(opts, nil))
}

func TestBuildPlaybookCommandSettings_Good_AppliesFlags(t *testing.T) {
	dir := t.TempDir()
	playbookPath := filepath.Join(dir, "site.yml")
	require.NoError(t, os.WriteFile(playbookPath, []byte("- hosts: all\n  tasks: []\n"), 0644))

	opts := core.NewOptions(
		core.Option{Key: "_arg", Value: playbookPath},
		core.Option{Key: "limit", Value: "web1"},
		core.Option{Key: "tags", Value: "deploy,setup"},
		core.Option{Key: "skip-tags", Value: "slow"},
		core.Option{Key: "extra-vars", Value: "version=1.2.3"},
		core.Option{Key: "check", Value: true},
		core.Option{Key: "diff", Value: true},
	)

	settings, err := buildPlaybookCommandSettings(opts, []string{"-vvv"})
	require.NoError(t, err)

	assert.Equal(t, playbookPath, settings.playbookPath)
	assert.Equal(t, dir, settings.basePath)
	assert.Equal(t, "web1", settings.limit)
	assert.Equal(t, []string{"deploy", "setup"}, settings.tags)
	assert.Equal(t, []string{"slow"}, settings.skipTags)
	assert.Equal(t, 3, settings.verbose)
	assert.True(t, settings.checkMode)
	assert.True(t, settings.diff)
	assert.Equal(t, map[string]any{"version": "1.2.3"}, settings.extraVars)
}

func TestBuildPlaybookCommandSettings_Bad_MissingPlaybook(t *testing.T) {
	_, err := buildPlaybookCommandSettings(core.NewOptions(), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage: ansible <playbook>")
}

func TestTestKeyFile_Good_PrefersExplicitKey(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "key", Value: "/tmp/id_ed25519"},
		core.Option{Key: "i", Value: "/tmp/ignored"},
	)

	assert.Equal(t, "/tmp/id_ed25519", resolveTestSSHKeyFile(opts))
}

func TestTestKeyFile_Good_FallsBackToShortAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "i", Value: "/tmp/id_ed25519"},
	)

	assert.Equal(t, "/tmp/id_ed25519", resolveTestSSHKeyFile(opts))
}

func TestFirstString_Good_ResolvesShortUserAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "u", Value: "deploy"},
	)

	cfgUser := firstStringOption(opts, "user", "u")

	assert.Equal(t, "deploy", cfgUser)
}

func TestRegister_Good_RegistersAnsibleCommands(t *testing.T) {
	app := core.New()

	Register(app)

	ansible := app.Command("ansible")
	require.True(t, ansible.OK)
	ansibleCmd := ansible.Value.(*core.Command)

	assert.Equal(t, "ansible", ansibleCmd.Path)
	assert.Equal(t, "ansible", ansibleCmd.Name)
	assert.Equal(t, "Run Ansible playbooks natively (no Python required)", ansibleCmd.Description)
	require.NotNil(t, ansibleCmd.Action)

	test := app.Command("ansible/test")
	require.True(t, test.OK)
	testCmd := test.Value.(*core.Command)

	assert.Equal(t, "ansible/test", testCmd.Path)
	assert.Equal(t, "test", testCmd.Name)
	assert.Equal(t, "Test SSH connectivity to a host", testCmd.Description)
	require.NotNil(t, testCmd.Action)

	paths := app.Commands()
	assert.Contains(t, paths, "ansible")
	assert.Contains(t, paths, "ansible/test")
}

func TestRegister_Good_ExposesExpectedFlags(t *testing.T) {
	app := core.New()

	Register(app)

	ansibleCmd := app.Command("ansible").Value.(*core.Command)
	assert.True(t, ansibleCmd.Flags.Has("inventory"))
	assert.True(t, ansibleCmd.Flags.Has("i"))
	assert.True(t, ansibleCmd.Flags.Has("limit"))
	assert.True(t, ansibleCmd.Flags.Has("l"))
	assert.True(t, ansibleCmd.Flags.Has("tags"))
	assert.True(t, ansibleCmd.Flags.Has("t"))
	assert.True(t, ansibleCmd.Flags.Has("skip-tags"))
	assert.True(t, ansibleCmd.Flags.Has("extra-vars"))
	assert.True(t, ansibleCmd.Flags.Has("e"))
	assert.True(t, ansibleCmd.Flags.Has("verbose"))
	assert.True(t, ansibleCmd.Flags.Has("v"))
	assert.True(t, ansibleCmd.Flags.Has("check"))
	assert.True(t, ansibleCmd.Flags.Has("diff"))

	assert.Equal(t, "", ansibleCmd.Flags.String("inventory"))
	assert.Equal(t, "", ansibleCmd.Flags.String("i"))
	assert.Equal(t, "", ansibleCmd.Flags.String("limit"))
	assert.Equal(t, "", ansibleCmd.Flags.String("l"))
	assert.Equal(t, "", ansibleCmd.Flags.String("tags"))
	assert.Equal(t, "", ansibleCmd.Flags.String("t"))
	assert.Equal(t, "", ansibleCmd.Flags.String("skip-tags"))
	assert.Equal(t, "", ansibleCmd.Flags.String("extra-vars"))
	assert.Equal(t, "", ansibleCmd.Flags.String("e"))
	assert.Equal(t, 0, ansibleCmd.Flags.Int("verbose"))
	assert.False(t, ansibleCmd.Flags.Bool("v"))
	assert.False(t, ansibleCmd.Flags.Bool("check"))
	assert.False(t, ansibleCmd.Flags.Bool("diff"))

	testCmd := app.Command("ansible/test").Value.(*core.Command)
	assert.True(t, testCmd.Flags.Has("user"))
	assert.True(t, testCmd.Flags.Has("u"))
	assert.True(t, testCmd.Flags.Has("password"))
	assert.True(t, testCmd.Flags.Has("key"))
	assert.True(t, testCmd.Flags.Has("i"))
	assert.True(t, testCmd.Flags.Has("port"))

	assert.Equal(t, "root", testCmd.Flags.String("user"))
	assert.Equal(t, "root", testCmd.Flags.String("u"))
	assert.Equal(t, "", testCmd.Flags.String("password"))
	assert.Equal(t, "", testCmd.Flags.String("key"))
	assert.Equal(t, "", testCmd.Flags.String("i"))
	assert.Equal(t, 22, testCmd.Flags.Int("port"))
}

func TestRunAnsible_Bad_MissingPlaybook(t *testing.T) {
	result := runPlaybookCommand(core.NewOptions())

	require.False(t, result.OK)
	err, ok := result.Value.(error)
	require.True(t, ok)
	assert.Contains(t, err.Error(), "usage: ansible <playbook>")
}

func TestRunAnsibleTest_Bad_MissingHost(t *testing.T) {
	result := runSSHTestCommand(core.NewOptions())

	require.False(t, result.OK)
	err, ok := result.Value.(error)
	require.True(t, ok)
	assert.Contains(t, err.Error(), "usage: ansible test <host>")
}
