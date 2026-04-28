package ansiblecmd

import (
	"os"
	"path/filepath"

	"dappco.re/go"
)

func TestExtraVars_Good_RepeatableAndCommaSeparated(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "version=1.2.3,env=prod"},
		core.Option{Key: "extra-vars", Value: "region=us-east-1"},
		core.Option{Key: "extra-vars", Value: []string{"build=42"}},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"version": "1.2.3",
		"env":     "prod",
		"region":  "us-east-1",
		"build":   42,
	}, vars)
}

func TestExtraVars_Good_UsesShortAlias(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "e", Value: "version=1.2.3,env=prod"},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"version": "1.2.3",
		"env":     "prod",
	}, vars)
}

func TestExtraVars_Good_TrimsWhitespaceAroundPairs(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: " version = 1.2.3 , env = prod , empty = "},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"version": "1.2.3",
		"env":     "prod",
		"empty":   "",
	}, vars)
}

func TestExtraVars_Good_IgnoresMalformedPairs(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "missing_equals,keep=this"},
		core.Option{Key: "extra-vars", Value: "also_bad="},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"keep":     "this",
		"also_bad": "",
	}, vars)
}

func TestExtraVars_Good_ParsesYAMLScalarsInKeyValuePairs(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "enabled=true,count=42,threshold=3.5"},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"enabled":   true,
		"count":     42,
		"threshold": 3.5,
	}, vars)
}

func TestSplitCommaSeparatedOption_Good_TrimsWhitespace(t *core.T) {
	result := splitCommaSeparatedOption(" deploy, setup ,smoke ")
	core.AssertEqual(t, []string{"deploy", "setup", "smoke"}, result)
	core.AssertLen(t, result, 3)
}

func TestExtraVars_Good_SupportsStructuredYAMLAndJSON(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "app:\n  port: 8080\n  debug: true"},
		core.Option{Key: "extra-vars", Value: `{"image":"nginx:latest","replicas":3}`},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"app": map[string]any{
			"port":  int(8080),
			"debug": true,
		},
		"image":    "nginx:latest",
		"replicas": int(3),
	}, vars)
}

func TestExtraVars_Good_LoadsFileReferences(t *core.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vars.yml")
	core.RequireNoError(t, os.WriteFile(path, []byte("deploy_env: prod\nrelease: 42\n"), 0644))

	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "@" + path},
	)

	vars, err := extraVars(opts)
	core.RequireNoError(t, err)

	core.AssertEqual(t, map[string]any{
		"deploy_env": "prod",
		"release":    int(42),
	}, vars)
}

func TestExtraVars_Bad_MissingFile(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "@/definitely/missing/vars.yml"},
	)

	_, err := extraVars(opts)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "read extra vars file")
}

func TestFirstString_Good_PrefersFirstNonEmptyKey(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "inventory", Value: ""},
		core.Option{Key: "i", Value: "/tmp/inventory.yml"},
	)

	core.AssertEqual(t, "/tmp/inventory.yml", firstStringOption(opts, "inventory", "i"))
}

func TestFirstBool_Good_UsesAlias(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "v", Value: true},
	)

	core.AssertTrue(t, firstBoolOption(opts, "verbose", "v"))
}

func TestVerbosityLevel_Good_CountsStackedShortFlags(t *core.T) {
	opts := core.NewOptions()
	result := verbosityLevel(opts, []string{"-vvv"})

	core.AssertEqual(t, 3, result)
}

func TestVerbosityLevel_Good_CountsLongForm(t *core.T) {
	opts := core.NewOptions()
	result := verbosityLevel(opts, []string{"--verbose"})

	core.AssertEqual(t, 1, result)
}

func TestVerbosityLevel_Good_PreservesExplicitNumericLevel(t *core.T) {
	opts := core.NewOptions(core.Option{Key: "verbose", Value: 2})
	result := verbosityLevel(opts, nil)

	core.AssertEqual(t, 2, result)
}

func TestBuildPlaybookCommandSettings_Good_AppliesFlags(t *core.T) {
	dir := t.TempDir()
	playbookPath := filepath.Join(dir, "site.yml")
	core.RequireNoError(t, os.WriteFile(playbookPath, []byte("- hosts: all\n  tasks: []\n"), 0644))

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
	core.RequireNoError(t, err)

	core.AssertEqual(t, playbookPath, settings.playbookPath)
	core.AssertEqual(t, dir, settings.basePath)
	core.AssertEqual(t, "web1", settings.limit)
	core.AssertEqual(t, []string{"deploy", "setup"}, settings.tags)
	core.AssertEqual(t, []string{"slow"}, settings.skipTags)
	core.AssertEqual(t, 3, settings.verbose)
	core.AssertTrue(t, settings.checkMode)
	core.AssertTrue(t, settings.diff)
	core.AssertEqual(t, map[string]any{"version": "1.2.3"}, settings.extraVars)
}

func TestBuildPlaybookCommandSettings_Good_MergesRepeatedListFlags(t *core.T) {
	dir := t.TempDir()
	playbookPath := filepath.Join(dir, "site.yml")
	core.RequireNoError(t, os.WriteFile(playbookPath, []byte("- hosts: all\n  tasks: []\n"), 0644))

	opts := core.NewOptions(
		core.Option{Key: "_arg", Value: playbookPath},
		core.Option{Key: "limit", Value: "web1"},
		core.Option{Key: "limit", Value: []string{"web2"}},
		core.Option{Key: "tags", Value: "deploy,setup"},
		core.Option{Key: "tags", Value: []string{"smoke"}},
		core.Option{Key: "skip-tags", Value: "slow"},
		core.Option{Key: "skip-tags", Value: []string{"flaky,experimental"}},
	)

	settings, err := buildPlaybookCommandSettings(opts, nil)
	core.RequireNoError(t, err)

	core.AssertEqual(t, "web1,web2", settings.limit)
	core.AssertEqual(t, []string{"deploy", "setup", "smoke"}, settings.tags)
	core.AssertEqual(t, []string{"slow", "flaky", "experimental"}, settings.skipTags)
}

func TestBuildPlaybookCommandSettings_Bad_MissingPlaybook(t *core.T) {
	_, err := buildPlaybookCommandSettings(core.NewOptions(), nil)

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "usage: ansible <playbook>")
}

func TestDiffOutputLines_Good_IncludesPathAndBeforeAfter(t *core.T) {
	lines := diffOutputLines(map[string]any{
		"path":   "/etc/nginx/conf.d/app.conf",
		"before": "server_name=old.example.com;",
		"after":  "server_name=web01.example.com;",
	})

	core.AssertEqual(t, []string{
		"diff:",
		"path: /etc/nginx/conf.d/app.conf",
		"- server_name=old.example.com;",
		"+ server_name=web01.example.com;",
	}, lines)
}

func TestTestKeyFile_Good_PrefersExplicitKey(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "key", Value: "/tmp/id_ed25519"},
		core.Option{Key: "i", Value: "/tmp/ignored"},
	)

	core.AssertEqual(t, "/tmp/id_ed25519", resolveSSHTestKeyFile(opts))
}

func TestTestKeyFile_Good_FallsBackToShortAlias(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "i", Value: "/tmp/id_ed25519"},
	)

	core.AssertEqual(t, "/tmp/id_ed25519", resolveSSHTestKeyFile(opts))
}

func TestFirstString_Good_ResolvesShortUserAlias(t *core.T) {
	opts := core.NewOptions(
		core.Option{Key: "u", Value: "deploy"},
	)

	cfgUser := firstStringOption(opts, "user", "u")

	core.AssertEqual(t, "deploy", cfgUser)
}

func TestRegister_Good_RegistersAnsibleCommands(t *core.T) {
	app := core.New()

	Register(app)

	ansible := app.Command("ansible")
	core.RequireTrue(t, ansible.OK)
	ansibleCmd := ansible.Value.(*core.Command)

	core.AssertEqual(t, "ansible", ansibleCmd.Path)
	core.AssertEqual(t, "ansible", ansibleCmd.Name)
	core.AssertEqual(t, "Run Ansible playbooks natively (no Python required)", ansibleCmd.Description)
	core.AssertNotNil(t, ansibleCmd.Action)

	test := app.Command("ansible/test")
	core.RequireTrue(t, test.OK)
	testCmd := test.Value.(*core.Command)

	core.AssertEqual(t, "ansible/test", testCmd.Path)
	core.AssertEqual(t, "test", testCmd.Name)
	core.AssertEqual(t, "Test SSH connectivity to a host", testCmd.Description)
	core.AssertNotNil(t, testCmd.Action)

	paths := app.Commands()
	core.AssertContains(t, paths, "ansible")
	core.AssertContains(t, paths, "ansible/test")
}

func TestRegister_Good_ExposesExpectedFlags(t *core.T) {
	app := core.New()

	Register(app)

	ansibleCmd := app.Command("ansible").Value.(*core.Command)
	core.AssertTrue(t, ansibleCmd.Flags.Has("inventory"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("i"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("limit"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("l"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("tags"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("t"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("skip-tags"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("extra-vars"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("e"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("verbose"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("v"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("check"))
	core.AssertTrue(t, ansibleCmd.Flags.Has("diff"))

	core.AssertEqual(t, "", ansibleCmd.Flags.String("inventory"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("i"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("limit"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("l"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("tags"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("t"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("skip-tags"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("extra-vars"))
	core.AssertEqual(t, "", ansibleCmd.Flags.String("e"))
	core.AssertEqual(t, 0, ansibleCmd.Flags.Int("verbose"))
	core.AssertFalse(t, ansibleCmd.Flags.Bool("v"))
	core.AssertFalse(t, ansibleCmd.Flags.Bool("check"))
	core.AssertFalse(t, ansibleCmd.Flags.Bool("diff"))

	testCmd := app.Command("ansible/test").Value.(*core.Command)
	core.AssertTrue(t, testCmd.Flags.Has("user"))
	core.AssertTrue(t, testCmd.Flags.Has("u"))
	core.AssertTrue(t, testCmd.Flags.Has("password"))
	core.AssertTrue(t, testCmd.Flags.Has("key"))
	core.AssertTrue(t, testCmd.Flags.Has("i"))
	core.AssertTrue(t, testCmd.Flags.Has("port"))

	core.AssertEqual(t, "root", testCmd.Flags.String("user"))
	core.AssertEqual(t, "root", testCmd.Flags.String("u"))
	core.AssertEqual(t, "", testCmd.Flags.String("password"))
	core.AssertEqual(t, "", testCmd.Flags.String("key"))
	core.AssertEqual(t, "", testCmd.Flags.String("i"))
	core.AssertEqual(t, 22, testCmd.Flags.Int("port"))
}

func TestRunAnsible_Bad_MissingPlaybook(t *core.T) {
	result := runPlaybookCommand(core.NewOptions())

	core.AssertFalse(t, result.OK)
	err, ok := result.Value.(error)
	core.RequireTrue(t, ok)
	core.AssertContains(t, err.Error(), "usage: ansible <playbook>")
}

func TestRunAnsibleTest_Bad_MissingHost(t *core.T) {
	result := runSSHTestCommand(core.NewOptions())

	core.AssertFalse(t, result.OK)
	err, ok := result.Value.(error)
	core.RequireTrue(t, ok)
	core.AssertContains(t, err.Error(), "usage: ansible test <host>")
}
