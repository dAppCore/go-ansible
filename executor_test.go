package ansible

import (
	"context"
	core "dappco.re/go"
	coreio "dappco.re/go/io"
	"time"

	"gopkg.in/yaml.v3"
)

type becomeCall struct {
	become   bool
	user     string
	password string
}

type trackingMockClient struct {
	*MockSSHClient
	becomeCalls []becomeCall
}

func newTrackingMockClient() *trackingMockClient {
	return &trackingMockClient{MockSSHClient: NewMockSSHClient()}
}

func (c *trackingMockClient) SetBecome(become bool, user, password string) {
	c.becomeCalls = append(c.becomeCalls, becomeCall{become: become, user: user, password: password})
	c.MockSSHClient.SetBecome(become, user, password)
}

func boolPtr(v bool) *bool {
	return &v
}

// --- NewExecutor ---

func TestExecutor_NewExecutor_Good(t *core.T) {
	e := NewExecutor("/some/path")

	core.AssertNotNil(t, e)
	core.AssertNotNil(t, e.parser)
	core.AssertNotNil(t, e.vars)
	core.AssertNotNil(t, e.facts)
	core.AssertNotNil(t, e.results)
	core.AssertNotNil(t, e.handlers)
	core.AssertNotNil(t, e.notified)
	core.AssertNotNil(t, e.clients)
}

func TestExecutor_SSHClient_Run_Good(t *core.T) {
	mock := NewMockSSHClient()
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: "export APP_ENV=prod; "}

	stdout, stderr, code, err := client.Run(context.Background(), "echo $APP_ENV")

	core.AssertNoError(t, err)
	core.AssertEmpty(t, stdout)
	core.AssertEmpty(t, stderr)
	core.AssertEqual(t, 0, code)
	core.AssertEqual(t, "export APP_ENV=prod; echo $APP_ENV", mock.lastCommand().Cmd)
}

func TestExecutor_SSHClient_Run_Bad(t *core.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("false", "", "failed", 2)
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: "set -e; "}

	_, stderr, code, err := client.Run(context.Background(), "false")

	core.AssertNoError(t, err)
	core.AssertEqual(t, "failed", stderr)
	core.AssertEqual(t, 2, code)
	core.AssertEqual(t, "set -e; false", mock.lastCommand().Cmd)
}

func TestExecutor_SSHClient_Run_Ugly(t *core.T) {
	mock := NewMockSSHClient()
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: ""}

	_, _, code, err := client.Run(context.Background(), "")

	core.AssertNoError(t, err)
	core.AssertEqual(t, 0, code)
	core.AssertEqual(t, "", mock.lastCommand().Cmd)
}

func TestExecutor_SSHClient_RunScript_Good(t *core.T) {
	mock := NewMockSSHClient()
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: "export APP_ENV=prod\n"}

	_, _, code, err := client.RunScript(context.Background(), "echo $APP_ENV")

	core.AssertNoError(t, err)
	core.AssertEqual(t, 0, code)
	core.AssertEqual(t, "export APP_ENV=prod\necho $APP_ENV", mock.lastCommand().Cmd)
}

func TestExecutor_SSHClient_RunScript_Bad(t *core.T) {
	mock := NewMockSSHClient()
	mock.expectCommand("exit 3", "", "bad script", 3)
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: "set -e\n"}

	_, stderr, code, err := client.RunScript(context.Background(), "exit 3")

	core.AssertNoError(t, err)
	core.AssertEqual(t, "bad script", stderr)
	core.AssertEqual(t, 3, code)
	core.AssertEqual(t, "set -e\nexit 3", mock.lastCommand().Cmd)
}

func TestExecutor_SSHClient_RunScript_Ugly(t *core.T) {
	mock := NewMockSSHClient()
	client := &environmentSSHClient{sshExecutorClient: mock, prefix: ""}

	_, _, code, err := client.RunScript(context.Background(), "")

	core.AssertNoError(t, err)
	core.AssertEqual(t, 0, code)
	core.AssertEqual(t, "", mock.lastCommand().Cmd)
}

// --- SetVar ---

func TestExecutor_SetVar_Good(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetVar("foo", "bar")
	e.SetVar("count", 42)

	core.AssertEqual(t, "bar", e.vars["foo"])
	core.AssertEqual(t, 42, e.vars["count"])
}

// --- SetInventoryDirect ---

func TestExecutor_SetInventoryDirect_Good(t *core.T) {
	e := NewExecutor("/tmp")
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {AnsibleHost: "10.0.0.1"},
			},
		},
	}

	e.SetInventoryDirect(inv)
	core.AssertEqual(t, inv, e.inventory)
}

func TestExecutor_Run_Good_UsesCachedClient(t *core.T) {
	dir := t.TempDir()
	playbookPath := joinPath(dir, "site.yml")
	core.RequireNoError(t, writeTestFile(playbookPath, []byte(`---
- hosts: localhost
  gather_facts: false
  tasks:
    - name: run shell command
      shell: echo ok
      register: shell_result
`), 0644))

	e := NewExecutor(dir)
	mock := NewMockSSHClient()
	mock.expectCommand(`^echo ok$`, "ok\n", "", 0)
	e.clients["localhost"] = mock

	core.RequireNoError(t, e.Run(context.Background(), playbookPath))

	core.AssertNotNil(t, e.results["localhost"]["shell_result"])
	core.AssertEqual(t, "ok\n", e.results["localhost"]["shell_result"].Stdout)
	core.AssertTrue(t, e.results["localhost"]["shell_result"].Changed)
	core.AssertEqual(t, 1, len(mock.executed))
	core.AssertEqual(t, "RunScript", mock.executed[0].Method)
	core.AssertEqual(t, "echo ok", mock.executed[0].Cmd)
}

// --- getHosts ---

func TestExecutor_GetHosts_Good_WithInventory(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
			},
		},
	})

	hosts := e.getHosts("all")
	core.AssertLen(t, hosts, 2)
}

func TestExecutor_GetHosts_Good_Localhost(t *core.T) {
	e := NewExecutor("/tmp")
	// No inventory set

	hosts := e.getHosts("localhost")
	core.AssertEqual(t, []string{"localhost"}, hosts)
}

func TestExecutor_GetHosts_Good_NoInventory(t *core.T) {
	e := NewExecutor("/tmp")

	hosts := e.getHosts("webservers")
	core.AssertNil(t, hosts)
}

func TestExecutor_GetHosts_Good_WithLimit(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
				"host3": {},
			},
		},
	})
	e.Limit = "host2"

	hosts := e.getHosts("all")
	core.AssertLen(t, hosts, 1)
	core.AssertContains(t, hosts, "host2")
}

func TestExecutor_GetClient_Good_PlayVarsOverrideInventoryVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {
					AnsibleHost: "10.0.0.10",
					AnsibleUser: "inventory-user",
				},
			},
		},
	})
	e.SetVar("ansible_host", "10.0.0.20")
	e.SetVar("ansible_user", "play-user")

	client, err := e.getClient("host1", &Play{})
	core.RequireNoError(t, err)

	sshClient, ok := client.(*SSHClient)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "10.0.0.20", sshClient.host)
	core.AssertEqual(t, "play-user", sshClient.user)
}

func TestExecutor_GetClient_Good_UsesInventoryBecomePassword(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {
					AnsibleHost:           "127.0.0.1",
					AnsibleBecomePassword: "secret",
				},
			},
		},
	})

	client, err := e.getClient("host1", &Play{Become: true, BecomeUser: "admin"})
	core.RequireNoError(t, err)

	sshClient, ok := client.(*SSHClient)
	core.RequireTrue(t, ok)
	become, user, pass := sshClient.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "admin", user)
	core.AssertEqual(t, "secret", pass)
}

func TestExecutor_GetClient_Good_UpdatesCachedBecomeState(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {AnsibleHost: "127.0.0.1"},
			},
		},
	})

	cached := &becomeRecordingClient{}
	e.clients["host1"] = cached

	play := &Play{Become: true, BecomeUser: "admin"}
	client, err := e.getClient("host1", play)
	core.RequireNoError(t, err)
	core.AssertSame(t, cached, client)

	become, user, pass := cached.BecomeState()
	core.AssertTrue(t, become)
	core.AssertEqual(t, "admin", user)
	core.AssertEmpty(t, pass)
}

// --- matchesTags ---

func TestExecutor_MatchesTags_Good_NoTagsFilter(t *core.T) {
	e := NewExecutor("/tmp")

	core.AssertTrue(t, e.matchesTags(nil))
	core.AssertTrue(t, e.matchesTags([]string{"any", "tags"}))
}

func TestExecutor_MatchesTags_Good_IncludeTag(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	core.AssertTrue(t, e.matchesTags([]string{"deploy"}))
	core.AssertTrue(t, e.matchesTags([]string{"setup", "deploy"}))
	core.AssertFalse(t, e.matchesTags([]string{"other"}))
}

func TestExecutor_MatchesTags_Good_SkipTag(t *core.T) {
	e := NewExecutor("/tmp")
	e.SkipTags = []string{"slow"}

	core.AssertTrue(t, e.matchesTags([]string{"fast"}))
	core.AssertFalse(t, e.matchesTags([]string{"slow"}))
	core.AssertFalse(t, e.matchesTags([]string{"fast", "slow"}))
}

func TestExecutor_MatchesTags_Good_AllTag(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"all"}

	core.AssertTrue(t, e.matchesTags([]string{"anything"}))
}

func TestExecutor_MatchesTags_Good_AlwaysTagIgnoresIncludeFilter(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	core.AssertTrue(t, e.matchesTags([]string{"always"}))
	core.AssertTrue(t, e.matchesTags([]string{"always", "other"}))
}

func TestExecutor_MatchesTags_Good_AlwaysTagStillRespectsSkipFilter(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}
	e.SkipTags = []string{"always"}

	core.AssertFalse(t, e.matchesTags([]string{"always"}))
}

func TestExecutor_MatchesTags_Good_NoTaskTags(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Tasks with no tags should not match when include tags are set
	core.AssertFalse(t, e.matchesTags(nil))
	core.AssertFalse(t, e.matchesTags([]string{}))
}

// --- handleNotify ---

func TestExecutor_HandleNotify_Good_String(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("restart nginx")

	core.AssertTrue(t, e.notified["restart nginx"])
}

func TestExecutor_HandleNotify_Good_StringList(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]string{"restart nginx", "reload config"})

	core.AssertTrue(t, e.notified["restart nginx"])
	core.AssertTrue(t, e.notified["reload config"])
}

func TestExecutor_HandleNotify_Good_AnyList(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]any{"restart nginx", "reload config"})

	core.AssertTrue(t, e.notified["restart nginx"])
	core.AssertTrue(t, e.notified["reload config"])
}

// --- run_once ---

func TestExecutor_RunTaskOnHosts_Good_RunOnceSharesRegisteredResult(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
			},
		},
	})

	var started []string
	task := &Task{
		Name:     "Run once debug",
		Module:   "debug",
		Args:     map[string]any{"msg": "hello"},
		Register: "debug_result",
		RunOnce:  true,
	}

	e.OnTaskStart = func(host string, _ *Task) {
		started = append(started, host)
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1", "host2"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertLen(t, started, 1)
	core.AssertLen(t, e.results["host1"], 1)
	core.AssertLen(t, e.results["host2"], 1)
	core.AssertNotNil(t, e.results["host1"]["debug_result"])
	core.AssertNotNil(t, e.results["host2"]["debug_result"])
	core.AssertEqual(t, "hello", e.results["host1"]["debug_result"].Msg)
	core.AssertEqual(t, "hello", e.results["host2"]["debug_result"].Msg)
}

func TestExecutor_RunTaskOnHost_Good_DelegateToUsesDelegatedClient(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	e.SetVar("delegate_host", "delegate1")
	e.inventory.All.Hosts["delegate1"] = &Host{AnsibleHost: "127.0.0.2"}
	e.clients["delegate1"] = mock
	mock.expectCommand(`echo delegated`, "delegated", "", 0)

	task := &Task{
		Name:     "Delegate command",
		Module:   "command",
		Args:     map[string]any{"cmd": "echo delegated"},
		Delegate: "{{ delegate_host }}",
		Register: "delegated_result",
	}

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["delegated_result"])
	core.AssertEqual(t, "delegated", e.results["host1"]["delegated_result"].Stdout)
	core.AssertTrue(t, mock.hasExecuted(`echo delegated`))
	core.AssertEqual(t, 1, mock.commandCount())
}

func TestExecutor_RunTaskOnHost_Good_DelegateToTemplatesInventoryHostname(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	e.clients["host1-delegate"] = mock
	mock.expectCommand(`echo templated`, "templated", "", 0)

	task := &Task{
		Name:     "Templated delegate",
		Module:   "shell",
		Args:     map[string]any{"_raw_params": "echo templated"},
		Delegate: "{{ inventory_hostname }}-delegate",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertTrue(t, mock.hasExecuted(`echo templated`))
	_, leaked := e.vars["inventory_hostname"]
	core.AssertFalse(t, leaked)
}

func TestExecutor_RunTaskOnHost_Good_ActionAliasExecutesCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo action-alias`, "action-alias", "", 0)

	var task Task
	core.RequireNoError(t, yaml.Unmarshal([]byte(`
name: Legacy action
action: command echo action-alias
`), &task))
	task.Register = "action_result"

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, &task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["action_result"])
	core.AssertEqual(t, "action-alias", e.results["host1"]["action_result"].Stdout)
	core.AssertTrue(t, mock.hasExecuted(`echo action-alias`))
}

func TestExecutor_Run_Good_VarsFilesMergeIntoPlayVars(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars", "common.yml"), []byte(`---
http_port: 8080
app_name: base
environment: staging
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars", "override.yml"), []byte(`---
app_name: demo
`), 0644))

	playbookPath := joinPath(dir, "playbook.yml")
	core.RequireNoError(t, writeTestFile(playbookPath, []byte(`---
- name: Vars files
  hosts: localhost
  gather_facts: false
  vars:
    environment: prod
  vars_files:
    - vars/common.yml
    - vars/override.yml
  tasks:
    - name: Show merged vars
      debug:
        msg: "{{ http_port }} {{ app_name }} {{ environment }}"
      register: vars_result
`), 0644))

	e := NewExecutor(dir)
	core.RequireNoError(t, e.Run(context.Background(), playbookPath))

	core.AssertNotNil(t, e.results["localhost"])
	core.AssertNotNil(t, e.results["localhost"]["vars_result"])
	core.AssertEqual(t, "8080 demo prod", e.results["localhost"]["vars_result"].Msg)
}

func TestExecutor_Run_Good_VarsFilesSupportTemplatedPaths(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars", "prod.yml"), []byte(`---
app_name: templated
`), 0644))

	playbookPath := joinPath(dir, "playbook.yml")
	core.RequireNoError(t, writeTestFile(playbookPath, []byte(`---
- name: Vars files templated path
  hosts: localhost
  gather_facts: false
  vars:
    environment: prod
  vars_files:
    - vars/{{ environment }}.yml
  tasks:
    - name: Show templated var
      debug:
        msg: "{{ app_name }}"
      register: vars_result
`), 0644))

	e := NewExecutor(dir)
	core.RequireNoError(t, e.Run(context.Background(), playbookPath))

	core.AssertNotNil(t, e.results["localhost"])
	core.AssertNotNil(t, e.results["localhost"]["vars_result"])
	core.AssertEqual(t, "templated", e.results["localhost"]["vars_result"].Msg)
}

func TestExecutor_Run_Good_VarsFilesSupportPlaybookDirMagicVar(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars", "prod.yml"), []byte(`---
app_name: magic
`), 0644))

	playbookPath := joinPath(dir, "playbook.yml")
	core.RequireNoError(t, writeTestFile(playbookPath, []byte(`---
- name: Vars files playbook dir
  hosts: localhost
  gather_facts: false
  vars_files:
    - "{{ playbook_dir }}/vars/prod.yml"
  tasks:
    - name: Show magic var
      debug:
        msg: "{{ app_name }} {{ playbook_dir }}"
      register: vars_result
`), 0644))

	e := NewExecutor(dir)
	core.RequireNoError(t, e.Run(context.Background(), playbookPath))

	core.AssertNotNil(t, e.results["localhost"])
	core.AssertNotNil(t, e.results["localhost"]["vars_result"])
	core.AssertEqual(t, "magic "+dir, e.results["localhost"]["vars_result"].Msg)
}

func TestExecutor_RunTaskOnHosts_Good_WithFileUsesFileContents(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "fragments", "hello.txt"), []byte("hello from file"), 0644))

	e := NewExecutor(dir)
	mock := NewMockSSHClient()
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = mock

	task := &Task{
		Name:     "Read file loop",
		Module:   "debug",
		Args:     map[string]any{"msg": "{{ item }}"},
		Register: "debug_result",
		WithFile: []any{
			"fragments/hello.txt",
		},
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"])
	core.AssertNotNil(t, e.results["host1"]["debug_result"])
	core.AssertLen(t, e.results["host1"]["debug_result"].Results, 1)
	core.AssertEqual(t, "hello from file", e.results["host1"]["debug_result"].Results[0].Msg)
}

func TestExecutor_RunTaskOnHosts_Good_WithFileGlobExpandsMatches(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "fragments", "alpha.txt"), []byte("alpha"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "fragments", "beta.txt"), []byte("beta"), 0644))

	e := NewExecutor(dir)
	mock := NewMockSSHClient()
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = mock

	task := &Task{
		Name:   "Glob files",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		Register: "glob_result",
		WithFileGlob: []any{
			"fragments/*.txt",
		},
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"])
	core.AssertNotNil(t, e.results["host1"]["glob_result"])
	core.AssertLen(t, e.results["host1"]["glob_result"].Results, 2)
	core.AssertEqual(t, []string{
		joinPath(dir, "fragments", "alpha.txt"),
		joinPath(dir, "fragments", "beta.txt"),
	}, []string{
		e.results["host1"]["glob_result"].Results[0].Msg,
		e.results["host1"]["glob_result"].Results[1].Msg,
	})
}

func TestExecutor_ExecuteModule_Good_ShortFormCommunityAlias(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()

	task := &Task{
		Name:   "Install SSH key",
		Module: "authorized_key",
		Args: map[string]any{
			"user": "deploy",
			"key":  "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDshortformalias",
		},
	}

	result, err := e.executeModule(context.Background(), "host1", mock, task, &Play{})
	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`getent passwd deploy`))
	core.AssertTrue(t, mock.hasExecuted(`authorized_keys`))
}

func TestExecutor_RunTaskOnHost_Good_EnvironmentMergesForCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	play := &Play{
		Environment: map[string]string{
			"APP_ENV":   "play",
			"PLAY_ONLY": "from-play",
		},
	}
	task := &Task{
		Name:   "Environment command",
		Module: "command",
		Args: map[string]any{
			"cmd": `echo "$APP_ENV:$PLAY_ONLY:$TASK_ONLY"`,
		},
		Environment: map[string]string{
			"APP_ENV":   "task",
			"TASK_ONLY": "from-task",
		},
		Register: "env_result",
	}

	mock.expectCommand(`export APP_ENV='task'; export PLAY_ONLY='from-play'; export TASK_ONLY='from-task'; echo`, "task:from-play:from-task\n", "", 0)

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, play)
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["env_result"])
	core.AssertEqual(t, "task:from-play:from-task\n", e.results["host1"]["env_result"].Stdout)
	core.AssertTrue(t, mock.hasExecuted(`export APP_ENV='task'; export PLAY_ONLY='from-play'; export TASK_ONLY='from-task'; echo`))
}

func TestExecutor_RunTaskOnHost_Good_EnvironmentAppliesToShellScript(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	play := &Play{
		Environment: map[string]string{
			"SHELL_ONLY": "from-play",
		},
	}
	task := &Task{
		Name:   "Environment shell",
		Module: "shell",
		Args: map[string]any{
			"_raw_params": `echo "$SHELL_ONLY"`,
		},
		Environment: map[string]string{
			"SHELL_ONLY": "from-task",
		},
		Register: "shell_env_result",
	}

	mock.expectCommand(`export SHELL_ONLY='from-task'; echo`, "from-task\n", "", 0)

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, play)
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["shell_env_result"])
	core.AssertEqual(t, "from-task\n", e.results["host1"]["shell_env_result"].Stdout)
	core.AssertTrue(t, mock.hasExecuted(`export SHELL_ONLY='from-task'; echo`))
}

func TestExecutor_RunRole_Good_AppliesRoleTagsToTasks(t *core.T) {
	dir := t.TempDir()
	roleTasks := `---
- name: tagged role task
  debug:
    msg: role ran
  register: role_result
`
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "tasks", "main.yml"), []byte(roleTasks), 0644))

	e := NewExecutor(dir)
	e.Tags = []string{"web"}
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	err := e.runRole(context.Background(), []string{"host1"}, &RoleRef{
		Role: "webserver",
		Tags: []string{"web"},
	}, &Play{}, nil)
	core.RequireNoError(t, err)

	core.AssertEqual(t, []string{"host1:tagged role task"}, started)
	core.AssertNotNil(t, e.results["host1"]["role_result"])
	core.AssertEqual(t, "role ran", e.results["host1"]["role_result"].Msg)
}

func TestExecutor_RunRole_Good_MetaEndRoleStopsRemainingRoleTasks(t *core.T) {
	dir := t.TempDir()
	roleTasks := `---
- name: before end_role
  debug:
    msg: before
  register: before_result
- name: stop role
  meta: end_role
- name: after end_role
  debug:
    msg: after
  register: after_result
`
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "deploy", "tasks", "main.yml"), []byte(roleTasks), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})

	play := &Play{Connection: "local"}

	var executed []string
	e.OnTaskEnd = func(_ string, task *Task, _ *TaskResult) {
		executed = append(executed, task.Name)
	}

	err := e.runRole(context.Background(), []string{"host1"}, &RoleRef{
		Role: "deploy",
	}, play, nil)
	core.RequireNoError(t, err)

	core.AssertEqual(t, []string{"before end_role", "stop role"}, executed)
	core.AssertNotNil(t, e.results["host1"]["before_result"])
	_, hasAfter := e.results["host1"]["after_result"]
	core.AssertFalse(t, hasAfter)
}

func TestExecutor_RunRole_Good_AppliesRoleDefaultsAndVars(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "tasks", "main.yml"), []byte(`---
- name: role var task
  debug:
    msg: "{{ role_value }}|{{ role_param }}"
  register: role_result
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "defaults", "main.yml"), []byte(`---
role_value: default-value
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "vars", "main.yml"), []byte(`---
role_value: vars-value
`), 0644))

	e := NewExecutor(dir)
	e.SetVar("outer_value", "outer")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})

	play := &Play{Connection: "local"}

	err := e.runRole(context.Background(), []string{"host1"}, &RoleRef{
		Role: "webserver",
		Vars: map[string]any{
			"role_param": "include-value",
		},
	}, play, nil)
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["role_result"])
	core.AssertEqual(t, "vars-value|include-value", e.results["host1"]["role_result"].Msg)
	core.AssertEqual(t, "outer", e.vars["outer_value"])
	_, leaked := e.vars["role_value"]
	core.AssertFalse(t, leaked)
}

func TestExecutor_RunRole_Good_UsesCustomRoleDefaultsAndVarsFiles(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "tasks", "main.yml"), []byte(`---
- name: role file selector task
  debug:
    msg: "{{ role_value }}|{{ role_param }}"
  register: role_result
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "defaults", "main.yml"), []byte(`---
role_value: default-main
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "defaults", "custom.yml"), []byte(`---
role_value: default-custom
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "vars", "main.yml"), []byte(`---
role_value: vars-main
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "vars", "custom.yml"), []byte(`---
role_value: vars-custom
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()

	err := e.runRole(context.Background(), []string{"host1"}, &RoleRef{
		Role:         "webserver",
		DefaultsFrom: "custom.yml",
		VarsFrom:     "custom.yml",
		Vars: map[string]any{
			"role_param": "include-value",
		},
	}, &Play{Connection: "local"}, nil)
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["role_result"])
	core.AssertEqual(t, "vars-custom|include-value", e.results["host1"]["role_result"].Msg)
}

func TestExecutor_RunIncludeRole_Good_TemplatesRoleName(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "tasks", "main.yml"), []byte(`---
- name: templated role task
  debug:
    msg: role ran
  register: role_result
`), 0644))

	e := NewExecutor(dir)
	e.SetVar("role_name", "webserver")

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	err := e.runIncludeRole(context.Background(), []string{"localhost"}, &Task{
		IncludeRole: &RoleRef{
			Role: "{{ role_name }}",
		},
	}, &Play{})
	core.RequireNoError(t, err)

	core.AssertEqual(t, []string{"localhost:templated role task"}, started)
	core.AssertNotNil(t, e.results["localhost"]["role_result"])
	core.AssertEqual(t, "role ran", e.results["localhost"]["role_result"].Msg)
}

func TestExecutor_RunIncludeTasks_Good_AppliesTaskDefaultsToChildren(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "tasks", "child.yml"), []byte(`---
- name: included shell task
  shell: echo "{{ included_value }}"
  register: child_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	mock := newTrackingMockClient()
	e.clients["host1"] = mock
	become := true
	mock.expectCommand(`echo "from-apply"`, "from-apply\n", "", 0)

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, &Task{
		Name:         "include child tasks",
		IncludeTasks: "tasks/child.yml",
		Apply: &TaskApply{
			Vars: map[string]any{
				"included_value": "from-apply",
			},
			Become:     &become,
			BecomeUser: "root",
		},
	}, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["child_result"])
	core.AssertEqual(t, "from-apply\n", e.results["host1"]["child_result"].Stdout)
	core.AssertTrue(t, mock.hasExecuted(`echo "from-apply"`))
	core.RequireNotEmpty(t, mock.becomeCalls)
	core.AssertContains(t, mock.becomeCalls, becomeCall{become: true, user: "root", password: ""})
}

func TestExecutor_RunIncludeTasks_Good_ImportTasksReevaluatesWhenPerTask(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "tasks", "imported.yml"), []byte(`---
- name: set gate flag
  set_fact:
    gate_flag: true
  register: first_result
- name: gated follow-up
  debug:
    msg: should skip
  register: second_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {Vars: map[string]any{"gate_flag": false}},
			},
		},
	})

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
	}

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name:        "import child tasks",
		ImportTasks: "tasks/imported.yml",
		When:        "not gate_flag",
	}, play))

	core.AssertNotNil(t, e.results["localhost"]["first_result"])
	core.AssertTrue(t, e.results["localhost"]["first_result"].Changed)
	core.AssertNotNil(t, e.results["localhost"]["second_result"])
	core.AssertTrue(t, e.results["localhost"]["second_result"].Skipped)
	core.AssertEqual(t, "Skipped due to when condition", e.results["localhost"]["second_result"].Msg)
}

func TestExecutor_RunPlay_Good_AppliesPlayTagsToTasks(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()

	play := &Play{
		Hosts: "all",
		Tags:  []string{"deploy"},
		Tasks: []Task{
			{
				Name:     "tagged play task",
				Module:   "debug",
				Args:     map[string]any{"msg": "play ran"},
				Register: "play_result",
			},
		},
	}

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	err := e.runPlay(context.Background(), play)
	core.RequireNoError(t, err)

	core.AssertEqual(t, []string{"host1:tagged play task"}, started)
	core.AssertNotNil(t, e.results["host1"]["play_result"])
	core.AssertEqual(t, "play ran", e.results["host1"]["play_result"].Msg)
}

func TestExecutor_RunRole_Good_HostSpecificWhen(t *core.T) {
	dir := t.TempDir()
	roleTasks := `---
- name: gated role task
  debug:
    msg: role ran
  register: gated_result
`
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "tasks", "main.yml"), []byte(roleTasks), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {Vars: map[string]any{"enabled": true}},
				"host2": {Vars: map[string]any{"enabled": false}},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	err := e.runRole(context.Background(), []string{"host1", "host2"}, &RoleRef{
		Role: "webserver",
		When: "enabled",
	}, &Play{}, nil)
	core.RequireNoError(t, err)

	core.AssertEqual(t, []string{"host1:gated role task"}, started)
	core.AssertNotNil(t, e.results["host1"]["gated_result"])
	_, ok := e.results["host2"]["gated_result"]
	core.AssertFalse(t, ok)
}

func TestExecutor_RunIncludeRole_Good_ImportRoleReevaluatesWhenPerTask(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "webserver", "tasks", "main.yml"), []byte(`---
- name: set role gate
  set_fact:
    role_gate: true
  register: role_first_result
- name: gated role follow-up
  debug:
    msg: role should skip
  register: role_second_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {Vars: map[string]any{"role_gate": false}},
			},
		},
	})

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
	}

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name: "import conditional role",
		ImportRole: &RoleRef{
			Role: "webserver",
			When: "not role_gate",
		},
	}, play))

	core.AssertNotNil(t, e.results["localhost"]["role_first_result"])
	core.AssertTrue(t, e.results["localhost"]["role_first_result"].Changed)
	core.AssertNotNil(t, e.results["localhost"]["role_second_result"])
	core.AssertTrue(t, e.results["localhost"]["role_second_result"].Skipped)
	core.AssertEqual(t, "Skipped due to when condition", e.results["localhost"]["role_second_result"].Msg)
}

func TestExecutor_RunPlay_Good_LoadsRoleHandlers(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "demo", "tasks", "main.yml"), []byte(`---
- name: role task
  set_fact:
    role_triggered: true
  notify: role handler
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "demo", "handlers", "main.yml"), []byte(`---
- name: role handler
  debug:
    msg: handler ran
  register: role_handler_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {},
			},
		},
	})

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
		Roles: []RoleRef{
			{Role: "demo"},
		},
	}

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	err := e.runPlay(context.Background(), play)
	core.RequireNoError(t, err)

	core.AssertContains(t, started, "localhost:role task")
	core.AssertContains(t, started, "localhost:role handler")
	core.AssertNotNil(t, e.results["localhost"]["role_handler_result"])
	core.AssertEqual(t, "handler ran", e.results["localhost"]["role_handler_result"].Msg)
}

func TestExecutor_RunRole_Good_UsesCustomRoleHandlersFile(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "demo", "tasks", "main.yml"), []byte(`---
- name: role task
  set_fact:
    role_triggered: true
  notify: custom role handler
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "demo", "handlers", "custom.yml"), []byte(`---
- name: custom role handler
  debug:
    msg: custom handler ran
  register: custom_role_handler_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {},
			},
		},
	})

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
		Roles: []RoleRef{
			{Role: "demo", HandlersFrom: "custom.yml"},
		},
	}

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	err := e.runPlay(context.Background(), play)
	core.RequireNoError(t, err)

	core.AssertContains(t, started, "localhost:role task")
	core.AssertContains(t, started, "localhost:custom role handler")
	core.AssertNotNil(t, e.results["localhost"]["custom_role_handler_result"])
	core.AssertEqual(t, "custom handler ran", e.results["localhost"]["custom_role_handler_result"].Msg)
}

func TestExecutor_RunPlay_Good_SerialBatchesHosts(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
				"host3": {},
			},
		},
	})

	gatherFacts := false
	play := &Play{
		Hosts:       "all",
		GatherFacts: &gatherFacts,
		Serial:      1,
		Tasks: []Task{
			{Name: "first", Module: "debug", Args: map[string]any{"msg": "one"}},
			{Name: "second", Module: "debug", Args: map[string]any{"msg": "two"}},
		},
	}

	var got []string
	e.OnTaskStart = func(host string, task *Task) {
		got = append(got, host+":"+task.Name)
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertEqual(t, []string{
		"host1:first",
		"host1:second",
		"host2:first",
		"host2:second",
		"host3:first",
		"host3:second",
	}, got)
}

func TestExecutor_RunPlay_Good_RestoresPlayVarsBetweenPlays(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {},
			},
		},
	})
	e.clients["localhost"] = NewMockSSHClient()

	gatherFacts := false
	first := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
		Vars: map[string]any{
			"play_var": "one",
		},
		Tasks: []Task{
			{
				Name:     "use play var",
				Module:   "debug",
				Args:     map[string]any{"msg": "{{ play_var }}"},
				Register: "first_result",
			},
		},
	}
	second := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:     "check play var",
				Module:   "debug",
				Args:     map[string]any{"msg": "{{ play_var | default('missing') }}"},
				Register: "second_result",
			},
		},
	}

	core.RequireNoError(t, e.runPlay(context.Background(), first))
	_, leaked := e.vars["play_var"]
	core.AssertFalse(t, leaked)

	core.RequireNoError(t, e.runPlay(context.Background(), second))
	core.AssertNotNil(t, e.results["localhost"]["first_result"])
	core.AssertNotNil(t, e.results["localhost"]["second_result"])
	core.AssertEqual(t, "one", e.results["localhost"]["first_result"].Msg)
	core.AssertEqual(t, "missing", e.results["localhost"]["second_result"].Msg)
}

func TestExecutor_Templating_Good_ExposesInventoryMagicVars(t *core.T) {
	dir := t.TempDir()
	inventoryPath := joinPath(dir, "inventory.yml")

	core.RequireNoError(t, writeTestFile(inventoryPath, []byte(`---
all:
  hosts:
    host1:
`), 0644))

	e := NewExecutor(dir)
	core.RequireNoError(t, e.SetInventory(inventoryPath))

	result := e.templateString("{{ inventory_file }}|{{ inventory_dir }}", "host1", nil)

	core.AssertEqual(t, inventoryPath+"|"+dir, result)
}

func TestExecutor_Templating_Good_ExposesModeMagicVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.CheckMode = true
	e.Diff = true

	result := e.templateString("{{ ansible_check_mode }}|{{ ansible_diff_mode }}", "localhost", nil)

	core.AssertEqual(t, "true|true", result)
	core.AssertTrue(t, e.evalCondition("ansible_check_mode and ansible_diff_mode", "localhost"))
}

func TestExecutor_RunPlay_Good_ExposesRoleContextVars(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "demo", "tasks", "main.yml"), []byte(`---
- name: role context
  debug:
    msg: "{{ role_name }}|{{ role_path }}"
  register: role_context_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {},
			},
		},
	})
	e.clients["localhost"] = NewMockSSHClient()

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
		Roles: []RoleRef{
			{Role: "demo"},
		},
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertNotNil(t, e.results["localhost"]["role_context_result"])
	core.AssertEqual(t, "demo|"+joinPath(dir, "roles", "demo"), e.results["localhost"]["role_context_result"].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopControlPause(t *core.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		Name:   "Pause between loop items",
		Module: "debug",
		Args:   map[string]any{"msg": "ok"},
		Loop:   []any{"one", "two"},
		LoopControl: &LoopControl{
			Pause: 1,
		},
	}

	start := time.Now()
	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	elapsed := time.Since(start)

	core.RequireNoError(t, err)
	core.AssertGreaterOrEqual(t, elapsed, 900*time.Millisecond)
}

func TestExecutor_RunTaskOnHost_Good_LoopWhenEvaluatedPerItem(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Loop with when",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		Loop:     []any{"skip", "run"},
		When:     "item != 'skip'",
		Register: "loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertTrue(t, result.Results[0].Skipped)
	core.AssertEqual(t, "Skipped due to when condition", result.Results[0].Msg)
	core.AssertFalse(t, result.Results[1].Skipped)
	core.AssertEqual(t, "run", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopControlExtendedExposesMetadata(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Extended loop metadata",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ ansible_loop.label }} {{ ansible_loop.index0 }}/{{ ansible_loop.length }} first={{ ansible_loop.first }} last={{ ansible_loop.last }}",
		},
		Loop: []any{"one", "two"},
		LoopControl: &LoopControl{
			Extended: true,
			Label:    "{{ item }}",
		},
		Register: "loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "one 0/2 first=true last=false", result.Results[0].Msg)
	core.AssertEqual(t, "two 1/2 first=false last=true", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopControlLabelWithoutExtended(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Label-only loop metadata",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ ansible_loop.label }}={{ item }}",
		},
		Loop: []any{"one", "two"},
		LoopControl: &LoopControl{
			Label: "{{ item }}",
		},
		Register: "loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "one=one", result.Results[0].Msg)
	core.AssertEqual(t, "two=two", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopControlExtendedExposesNeighbourItems(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Neighbour loop metadata",
		Module: "debug",
		Args: map[string]any{
			"msg": "prev={{ ansible_loop.previtem | default('NONE') }} next={{ ansible_loop.nextitem | default('NONE') }} all={{ ansible_loop.allitems }}",
		},
		Loop: []any{"one", "two"},
		LoopControl: &LoopControl{
			Extended: true,
		},
		Register: "loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "prev=NONE next=two all=[one two]", result.Results[0].Msg)
	core.AssertEqual(t, "prev=one next=NONE all=[one two]", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopTemplateDefaultExpandsItems(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Loop default fallback",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		Loop:     "{{ missing_items | default(['alpha', 'beta']) }}",
		Register: "loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "alpha", result.Results[0].Msg)
	core.AssertEqual(t, "beta", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithDictItems(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Dict loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.key }}={{ item.value }}",
		},
		Loop: []any{
			map[string]any{"key": "alpha", "value": "one"},
			map[string]any{"key": "beta", "value": "two"},
		},
		Register: "dict_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["dict_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "alpha=one", result.Results[0].Msg)
	core.AssertEqual(t, "beta=two", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithIndexedItems(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Indexed loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.0 }}={{ item.1 }}",
		},
		Loop: []any{
			[]any{0, "apple"},
			[]any{1, "banana"},
		},
		Register: "indexed_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["indexed_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "0=apple", result.Results[0].Msg)
	core.AssertEqual(t, "1=banana", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithSequence(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Sequence loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		WithSequence: "start=1 end=3 format=%02d",
		Register:     "sequence_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["sequence_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 3)
	core.AssertEqual(t, "01", result.Results[0].Msg)
	core.AssertEqual(t, "02", result.Results[1].Msg)
	core.AssertEqual(t, "03", result.Results[2].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithNested(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Nested loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.0 }}-{{ item.1 }}",
		},
		Loop: []any{
			[]any{"red", "small"},
			[]any{"red", "large"},
			[]any{"blue", "small"},
			[]any{"blue", "large"},
		},
		Register: "nested_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["nested_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 4)
	core.AssertEqual(t, "red-small", result.Results[0].Msg)
	core.AssertEqual(t, "red-large", result.Results[1].Msg)
	core.AssertEqual(t, "blue-small", result.Results[2].Msg)
	core.AssertEqual(t, "blue-large", result.Results[3].Msg)
}

func TestExecutor_RunTaskOnHost_Good_TemplatedLoopItems(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()
	e.vars["colour"] = "red"

	task := &Task{
		Name:   "Templated loop items",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		Loop: []any{
			"{{ colour }}",
			[]any{"{{ colour }}", "large"},
		},
		Register: "templated_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["templated_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "red", result.Results[0].Msg)
	core.AssertEqual(t, "[red large]", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithTogether(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Together loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.0 }}={{ item.1 }}",
		},
		WithTogether: []any{
			[]any{"red", "blue"},
			[]any{"small", "large", "medium"},
		},
		Register: "together_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["together_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "red=small", result.Results[0].Msg)
	core.AssertEqual(t, "blue=large", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithSubelements(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()
	e.vars["users"] = []any{
		map[string]any{
			"name":       "alice",
			"authorized": []any{"ssh-rsa AAA", "ssh-ed25519 BBB"},
		},
		map[string]any{
			"name":       "bob",
			"authorized": "ssh-rsa CCC",
		},
	}

	task := &Task{
		Name:   "Subelements loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.0.name }}={{ item.1 }}",
		},
		WithSubelements: []any{"{{ users }}", "authorized"},
		Register:        "subelements_loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["subelements_loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 3)
	core.AssertEqual(t, "alice=ssh-rsa AAA", result.Results[0].Msg)
	core.AssertEqual(t, "alice=ssh-ed25519 BBB", result.Results[1].Msg)
	core.AssertEqual(t, "bob=ssh-rsa CCC", result.Results[2].Msg)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromWithSubelementsSkipMissing(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()
	e.vars["users"] = []any{
		map[string]any{
			"name":       "alice",
			"authorized": []any{"ssh-rsa AAA", "ssh-ed25519 BBB"},
		},
		map[string]any{
			"name": "bob",
		},
	}

	task := &Task{
		Name:   "Subelements loop skip missing",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.0.name }}={{ item.1 }}",
		},
		WithSubelements: []any{"users", "authorized", true},
		Register:        "subelements_loop_skip_missing_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["subelements_loop_skip_missing_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "alice=ssh-rsa AAA", result.Results[0].Msg)
	core.AssertEqual(t, "alice=ssh-ed25519 BBB", result.Results[1].Msg)
}

func TestExecutor_RunTaskOnHost_Bad_LoopFromWithSubelementsMissingSubelement(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()
	e.vars["users"] = []any{
		map[string]any{
			"name":       "alice",
			"authorized": []any{"ssh-rsa AAA"},
		},
		map[string]any{
			"name": "bob",
		},
	}

	task := &Task{
		Name:   "Subelements loop missing",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item.0.name }}={{ item.1 }}",
		},
		WithSubelements: []any{"users", "authorized"},
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "with_subelements missing subelement")
}

func TestExecutor_RunTaskOnHosts_Good_LoopNotifiesAndCallsCallback(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	var ended *TaskResult
	task := &Task{
		Name:   "Looped change",
		Module: "set_fact",
		Args: map[string]any{
			"changed_flag": true,
		},
		Loop:   []any{"one", "two"},
		Notify: "restart app",
	}
	play := &Play{
		Handlers: []Task{
			{
				Name:   "restart app",
				Module: "debug",
				Args:   map[string]any{"msg": "handler"},
			},
		},
	}

	e.OnTaskEnd = func(_ string, _ *Task, result *TaskResult) {
		ended = result
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, play)
	core.RequireNoError(t, err)

	core.AssertNotNil(t, ended)
	core.AssertTrue(t, ended.Changed)
	core.AssertLen(t, ended.Results, 2)
	core.AssertTrue(t, e.notified["restart app"])
}

func TestExecutor_RunTaskOnHosts_Bad_LoopFailurePropagates(t *core.T) {
	e := NewExecutor("/tmp")
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Looped failure",
		Module: "fail",
		Args:   map[string]any{"msg": "bad"},
		Loop:   []any{"one", "two"},
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "task failed")
}

func TestExecutor_RunTaskWithRetries_Good_UntilSuccess(t *core.T) {
	e := NewExecutor("/tmp")
	attempts := 0

	task := &Task{
		Until:   "result is success",
		Retries: 2,
		Delay:   0,
	}

	result, err := e.runTaskWithRetries(context.Background(), "host1", task, &Play{}, func() (*TaskResult, error) {
		attempts++
		if attempts < 2 {
			return &TaskResult{Failed: true, Msg: "not yet", RC: 1}, nil
		}
		return &TaskResult{Changed: true, Msg: "ok", RC: 0}, nil
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertEqual(t, 2, attempts)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "ok", result.Msg)
}

// --- check mode ---

func TestExecutor_RunTaskOnHost_Good_CheckModeSkipsMutatingTask(t *core.T) {
	e := NewExecutor("/tmp")
	e.CheckMode = true

	var ended *TaskResult
	task := &Task{
		Name:     "Run a shell command",
		Module:   "shell",
		Args:     map[string]any{"_raw_params": "echo hello"},
		Register: "shell_result",
	}

	e.OnTaskEnd = func(_ string, _ *Task, result *TaskResult) {
		ended = result
	}

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, ended)
	core.AssertTrue(t, ended.Skipped)
	core.AssertFalse(t, ended.Changed)
	core.AssertEqual(t, "Skipped in check mode", ended.Msg)
	core.AssertNotNil(t, e.results["host1"]["shell_result"])
	core.AssertTrue(t, e.results["host1"]["shell_result"].Skipped)
}

func TestExecutor_RunTaskOnHost_Good_TaskCheckModeOverridesExecutorCheckMode(t *core.T) {
	e := NewExecutor("/tmp")
	e.CheckMode = true
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:      "Run despite global check mode",
		Module:    "shell",
		Args:      map[string]any{"_raw_params": "echo hello"},
		CheckMode: boolPtr(false),
		Register:  "shell_result",
	}

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"]["shell_result"])
	core.AssertFalse(t, e.results["host1"]["shell_result"].Skipped)
	core.AssertTrue(t, e.results["host1"]["shell_result"].Changed)
}

func TestExecutor_RunTaskOnHost_Good_TaskDiffOverridesExecutorDiff(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:     "Inspect task diff mode",
		Module:   "debug",
		Args:     map[string]any{"msg": "{{ ansible_diff_mode }}"},
		Diff:     boolPtr(true),
		Register: "diff_result",
	}

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, e.results["host1"])
	core.AssertNotNil(t, e.results["host1"]["diff_result"])
	core.AssertEqual(t, "true", e.results["host1"]["diff_result"].Msg)
}

// --- no_log ---

func TestExecutor_RunTaskOnHost_Good_NoLogRedactsCallbackResult(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = &SSHClient{}

	var ended *TaskResult
	task := &Task{
		Name:     "Sensitive debug",
		Module:   "debug",
		Args:     map[string]any{"msg": "top secret"},
		Register: "debug_result",
		NoLog:    true,
	}

	e.OnTaskEnd = func(_ string, _ *Task, result *TaskResult) {
		ended = result
	}

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	core.AssertNotNil(t, ended)
	core.AssertEqual(t, "censored due to no_log", ended.Msg)
	core.AssertEmpty(t, ended.Stdout)
	core.AssertEmpty(t, ended.Stderr)
	core.AssertNil(t, ended.Data)
	core.AssertNotNil(t, e.results["host1"]["debug_result"])
	core.AssertEqual(t, "top secret", e.results["host1"]["debug_result"].Msg)
}

func TestExecutor_RunTaskOnHost_Bad_NoLogHidesFailureMessage(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = &SSHClient{}

	var ended *TaskResult
	task := &Task{
		Name:   "Sensitive failure",
		Module: "fail",
		Args:   map[string]any{"msg": "super secret"},
		NoLog:  true,
	}

	e.OnTaskEnd = func(_ string, _ *Task, result *TaskResult) {
		ended = result
	}

	err := e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{})
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "task failed")
	core.AssertNotContains(t, err.Error(), "super secret")
	core.AssertNotNil(t, ended)
	core.AssertEqual(t, "censored due to no_log", ended.Msg)
}

// --- normalizeConditions ---

func TestExecutor_NormalizeConditions_Good_String(t *core.T) {
	result := normalizeConditions("my_var is defined")
	core.AssertEqual(t, []string{"my_var is defined"}, result)
	core.AssertLen(t, result, 1)
}

// --- meta flush handlers ---

func TestExecutor_RunTaskOnHosts_Good_MetaFlushesHandlers(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = &SSHClient{}

	var executed []string
	e.OnTaskEnd = func(_ string, task *Task, _ *TaskResult) {
		executed = append(executed, task.Name)
	}

	play := &Play{
		Handlers: []Task{
			{
				Name:   "restart app",
				Module: "debug",
				Args:   map[string]any{"msg": "handler"},
			},
		},
	}

	notifyTask := &Task{
		Name:   "change config",
		Module: "set_fact",
		Args:   map[string]any{"restart_required": true},
		Notify: "restart app",
	}
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, notifyTask, play))
	core.AssertTrue(t, e.notified["restart app"])

	metaTask := &Task{
		Name:   "flush handlers",
		Module: "meta",
		Args:   map[string]any{"_raw_params": "flush_handlers"},
	}
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, metaTask, play))

	core.AssertFalse(t, e.notified["restart app"])
	core.AssertEqual(t, []string{"change config", "flush handlers", "restart app"}, executed)
}

func TestExecutor_RunTaskOnHosts_Good_MetaFlushesHandlerListenAlias(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = &SSHClient{}

	var executed []string
	e.OnTaskEnd = func(_ string, task *Task, _ *TaskResult) {
		executed = append(executed, task.Name)
	}

	play := &Play{
		Handlers: []Task{
			{
				Name:   "restart app",
				Listen: "reload app",
				Module: "debug",
				Args:   map[string]any{"msg": "handler"},
			},
		},
	}

	notifyTask := &Task{
		Name:   "change config",
		Module: "set_fact",
		Args:   map[string]any{"restart_required": true},
		Notify: "reload app",
	}
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, notifyTask, play))
	core.AssertTrue(t, e.notified["reload app"])

	metaTask := &Task{
		Name:   "flush handlers",
		Module: "meta",
		Args:   map[string]any{"_raw_params": "flush_handlers"},
	}
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, metaTask, play))

	core.AssertFalse(t, e.notified["reload app"])
	core.AssertEqual(t, []string{"change config", "flush handlers", "restart app"}, executed)
}

func TestExecutor_RunPlay_Good_ForceHandlersAfterFailure(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})

	var executed []string
	e.OnTaskEnd = func(_ string, task *Task, _ *TaskResult) {
		executed = append(executed, task.Name)
	}

	play := &Play{
		Name:          "force handlers",
		Hosts:         "host1",
		ForceHandlers: true,
		Tasks: []Task{
			{
				Name:   "change config",
				Module: "set_fact",
				Args:   map[string]any{"restart_required": true},
				Notify: "restart app",
			},
			{
				Name:   "boom",
				Module: "fail",
				Args:   map[string]any{"msg": "stop"},
			},
		},
		Handlers: []Task{
			{
				Name:     "restart app",
				Module:   "debug",
				Args:     map[string]any{"msg": "handler ran"},
				Register: "restart_result",
			},
		},
	}

	err := e.runPlay(context.Background(), play)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "task failed")
	core.AssertFalse(t, e.notified["restart app"])
	core.AssertEqual(t, []string{"change config", "boom", "restart app"}, executed)
	core.AssertNotNil(t, e.results["host1"]["restart_result"])
	core.AssertEqual(t, "handler ran", e.results["host1"]["restart_result"].Msg)
}

func TestExecutor_HandleMetaAction_Good_ClearHostErrors(t *core.T) {
	e := NewExecutor("/tmp")
	e.batchFailedHosts = map[string]bool{
		"host1": true,
		"host2": true,
	}

	result := &TaskResult{
		Data: map[string]any{"action": "clear_host_errors"},
	}

	core.RequireNoError(t, e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, &Play{}, result))
	core.AssertEmpty(t, e.batchFailedHosts)
}

func TestExecutor_HandleMetaAction_Good_RefreshInventory(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")

	initial := []byte("all:\n  hosts:\n    web1:\n      ansible_host: 10.0.0.1\n")
	updated := []byte("all:\n  hosts:\n    web1:\n      ansible_host: 10.0.0.2\n")

	core.RequireNoError(t, writeTestFile(path, initial, 0644))

	e := NewExecutor("/tmp")
	core.RequireNoError(t, e.SetInventory(path))
	e.clients["web1"] = &SSHClient{}

	core.RequireNoError(t, writeTestFile(path, updated, 0644))

	result := &TaskResult{
		Data: map[string]any{"action": "refresh_inventory"},
	}

	core.RequireNoError(t, e.handleMetaAction(context.Background(), "web1", []string{"web1"}, &Play{}, result))
	core.AssertNotNil(t, e.inventory)
	core.AssertNotNil(t, e.inventory.All)
	core.AssertContains(t, e.inventory.All.Hosts, "web1")
	core.AssertEqual(t, "10.0.0.2", e.inventory.All.Hosts["web1"].AnsibleHost)
	core.AssertEmpty(t, e.clients)
}

func TestExecutor_RunPlay_Good_MetaEndPlayStopsRemainingTasks(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.clients["host1"] = &SSHClient{}

	gatherFacts := false
	play := &Play{
		Hosts:       "all",
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{Name: "before", Module: "debug", Args: map[string]any{"msg": "before"}},
			{Name: "stop", Module: "meta", Args: map[string]any{"_raw_params": "end_play"}},
			{Name: "after", Module: "debug", Args: map[string]any{"msg": "after"}},
		},
	}

	var executed []string
	e.OnTaskEnd = func(_ string, task *Task, _ *TaskResult) {
		executed = append(executed, task.Name)
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))
	core.AssertEqual(t, []string{"before", "stop"}, executed)
}

func TestExecutor_RunPlay_Bad_MaxFailPercentageStopsPlay(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {Vars: map[string]any{"should_fail": true}},
				"host2": {Vars: map[string]any{"should_fail": false}},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()

	gatherFacts := false
	play := &Play{
		Hosts:          "all",
		GatherFacts:    &gatherFacts,
		MaxFailPercent: 49,
		Tasks: []Task{
			{
				Name:         "fail one host",
				Module:       "fail",
				Args:         map[string]any{"msg": "boom"},
				When:         "should_fail",
				IgnoreErrors: true,
			},
			{
				Name:   "after threshold",
				Module: "debug",
				Args:   map[string]any{"msg": "after"},
			},
		},
	}

	var executed []string
	e.OnTaskEnd = func(host string, task *Task, _ *TaskResult) {
		executed = append(executed, host+":"+task.Name)
	}

	err := e.runPlay(context.Background(), play)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "max fail percentage exceeded")
	core.AssertEqual(t, []string{"host1:fail one host"}, executed)
}

func TestExecutor_RunPlay_Good_TaskFailureContinuesAcrossHosts(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {Vars: map[string]any{"should_fail": true}},
				"host2": {Vars: map[string]any{"should_fail": false}},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()

	gatherFacts := false
	play := &Play{
		Hosts:       "all",
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:   "maybe fail",
				Module: "fail",
				Args:   map[string]any{"msg": "boom"},
				When:   "should_fail",
			},
			{
				Name:   "after failure",
				Module: "debug",
				Args:   map[string]any{"msg": "after"},
			},
		},
	}

	var executed []string
	e.OnTaskEnd = func(host string, task *Task, _ *TaskResult) {
		executed = append(executed, host+":"+task.Name)
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))
	core.AssertEqual(t, []string{
		"host1:maybe fail",
		"host2:maybe fail",
		"host1:after failure",
		"host2:after failure",
	}, executed)
}

func TestExecutor_RunPlay_Bad_AnyErrorsFatalStopsPlay(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {Vars: map[string]any{"should_fail": true}},
				"host2": {Vars: map[string]any{"should_fail": false}},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()

	gatherFacts := false
	play := &Play{
		Hosts:          "all",
		GatherFacts:    &gatherFacts,
		AnyErrorsFatal: true,
		Tasks: []Task{
			{
				Name:   "maybe fail",
				Module: "fail",
				Args:   map[string]any{"msg": "boom"},
				When:   "should_fail",
			},
			{
				Name:   "after failure",
				Module: "debug",
				Args:   map[string]any{"msg": "after"},
			},
		},
	}

	var executed []string
	e.OnTaskEnd = func(host string, task *Task, _ *TaskResult) {
		executed = append(executed, host+":"+task.Name)
	}

	err := e.runPlay(context.Background(), play)
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "task failed")
	core.AssertEqual(t, []string{"host1:maybe fail"}, executed)
}

func TestExecutor_NormalizeConditions_Good_StringSlice(t *core.T) {
	result := normalizeConditions([]string{"cond1", "cond2"})
	core.AssertEqual(t, []string{"cond1", "cond2"}, result)
	core.AssertLen(t, result, 2)
}

func TestExecutor_NormalizeConditions_Good_AnySlice(t *core.T) {
	result := normalizeConditions([]any{"cond1", "cond2"})
	core.AssertEqual(t, []string{"cond1", "cond2"}, result)
	core.AssertLen(t, result, 2)
}

func TestExecutor_NormalizeConditions_Good_Nil(t *core.T) {
	result := normalizeConditions(nil)
	core.AssertNil(t, result)
	core.AssertEmpty(t, result)
}

// --- evaluateWhen ---

func TestExecutor_EvaluateWhen_Good_TrueLiteral(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertTrue(t, e.evaluateWhen("true", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("True", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_FalseLiteral(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertFalse(t, e.evaluateWhen("false", "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("False", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_Negation(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertFalse(t, e.evaluateWhen("not true", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("not false", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_RegisteredVarDefined(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"myresult": {Changed: true, Failed: false},
	}

	core.AssertTrue(t, e.evaluateWhen("myresult is defined", "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("myresult is not defined", "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("nonexistent is defined", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("nonexistent is not defined", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_RegisteredVarStatus(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"success_result": {Changed: true, Failed: false},
		"failed_result":  {Failed: true},
		"skipped_result": {Skipped: true},
	}

	core.AssertTrue(t, e.evaluateWhen("success_result is success", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("success_result is succeeded", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("success_result is changed", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("failed_result is failed", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("skipped_result is skipped", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_VarTruthy(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.vars["disabled"] = false
	e.vars["name"] = "hello"
	e.vars["empty"] = ""
	e.vars["count"] = 5
	e.vars["zero"] = 0

	core.AssertTrue(t, e.evalCondition("enabled", "host1"))
	core.AssertFalse(t, e.evalCondition("disabled", "host1"))
	core.AssertTrue(t, e.evalCondition("name", "host1"))
	core.AssertFalse(t, e.evalCondition("empty", "host1"))
	core.AssertTrue(t, e.evalCondition("count", "host1"))
	core.AssertFalse(t, e.evalCondition("zero", "host1"))
}

func TestExecutor_EvaluateWhen_Good_MultipleConditions(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	// All conditions must be true (AND)
	core.AssertTrue(t, e.evaluateWhen([]any{"true", "True"}, "host1", nil))
	core.AssertFalse(t, e.evaluateWhen([]any{"true", "false"}, "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_LogicalAndOrExpressions(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.vars["maintenance"] = false

	core.AssertTrue(t, e.evaluateWhen("enabled and not maintenance", "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("enabled and maintenance", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("enabled or maintenance", "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("maintenance or false", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_LogicalExpressionParentheses(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.vars["maintenance"] = false
	e.vars["deployed"] = true

	core.AssertTrue(t, e.evaluateWhen("(enabled and not maintenance) or deployed", "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("enabled and (maintenance or false)", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_NestedVarAccess(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["feature"] = map[string]any{
		"enabled": true,
		"mode":    "active",
	}

	task := &Task{
		Vars: map[string]any{
			"settings": map[string]any{
				"ready": false,
			},
		},
	}

	core.AssertTrue(t, e.evaluateWhen("feature.enabled", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen(`feature.mode == "active"`, "host1", nil))
	core.AssertFalse(t, e.evaluateWhen("settings.ready", "host1", task))
}

func TestExecutor_ApplyTaskResultConditions_Good_ChangedWhen(t *core.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		ChangedWhen: "stdout == 'expected'",
	}
	result := &TaskResult{
		Changed: true,
		Stdout:  "actual",
	}

	e.applyTaskResultConditions("host1", task, result)

	core.AssertFalse(t, result.Changed)
}

func TestExecutor_ApplyTaskResultConditions_Good_FailedWhen(t *core.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		FailedWhen: []any{"rc != 0", "stdout == 'expected'"},
	}
	result := &TaskResult{
		Failed: true,
		Stdout: "expected",
		RC:     0,
	}

	e.applyTaskResultConditions("host1", task, result)

	core.AssertFalse(t, result.Failed)
}

func TestExecutor_ApplyTaskResultConditions_Good_DottedResultAccess(t *core.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		ChangedWhen: "result.rc == 0",
	}
	result := &TaskResult{
		Changed: false,
		RC:      0,
	}

	e.applyTaskResultConditions("host1", task, result)

	core.AssertTrue(t, result.Changed)
}

// --- templateString ---

func TestExecutor_TemplateString_Good_SimpleVar(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["name"] = "world"

	result := e.templateString("hello {{ name }}", "", nil)
	core.AssertEqual(t, "hello world", result)
}

func TestExecutor_TemplateString_Good_MultVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["host"] = "example.com"
	e.vars["port"] = 8080

	result := e.templateString("http://{{ host }}:{{ port }}", "", nil)
	core.AssertEqual(t, "http://example.com:8080", result)
}

func TestExecutor_TemplateString_Good_Unresolved(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("{{ undefined_var }}", "", nil)
	core.AssertEqual(t, "{{ undefined_var }}", result)
}

func TestExecutor_TemplateString_Good_NoTemplate(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("plain string", "", nil)
	core.AssertEqual(t, "plain string", result)
}

func TestExecutor_TemplateString_Good_InventoryHostnameShort(t *core.T) {
	e := NewExecutor("/tmp")

	result := e.templateString("{{ inventory_hostname_short }}", "web01.example.com", nil)

	core.AssertEqual(t, "web01", result)
}

func TestExecutor_TemplateString_Good_GroupNames(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"production": {
					Hosts: map[string]*Host{
						"web01.example.com": {},
					},
				},
				"web": {
					Children: map[string]*InventoryGroup{
						"frontend": {
							Hosts: map[string]*Host{
								"web01.example.com": {},
							},
						},
					},
				},
			},
		},
	})

	result := e.templateString("{{ group_names }}", "web01.example.com", nil)

	core.AssertEqual(t, "[frontend production web]", result)
}

func TestExecutor_TemplateString_Good_Groups(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"production": {
					Hosts: map[string]*Host{
						"web01": {},
						"web02": {},
					},
				},
				"web": {
					Children: map[string]*InventoryGroup{
						"frontend": {
							Hosts: map[string]*Host{
								"web01": {},
							},
						},
					},
				},
			},
		},
	})

	result := e.templateString("{{ groups.production }}", "web01", nil)

	core.AssertEqual(t, "[web01 web02]", result)
}

func TestExecutor_TemplateString_Good_HostVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"production": {
					Hosts: map[string]*Host{
						"web01": {
							AnsibleHost: "10.0.0.10",
						},
					},
				},
			},
		},
	})

	result := e.templateString("{{ hostvars.web01.ansible_host }}", "web01", nil)

	core.AssertEqual(t, "10.0.0.10", result)
}

func TestExecutor_EvalCondition_Good_InventoryHostnameShort(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.evalCondition("inventory_hostname_short == 'web01'", "web01.example.com")

	core.AssertTrue(t, result)
}

func TestExecutor_EvalCondition_Good_HostVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web01": {
					Vars: map[string]any{
						"deploy_enabled": true,
					},
				},
			},
		},
	})

	core.AssertTrue(t, e.evalCondition("hostvars.web01.deploy_enabled", "web01"))
}

// --- applyFilter ---

func TestExecutor_ApplyFilter_Good_Default(t *core.T) {
	e := NewExecutor("/tmp")

	core.AssertEqual(t, "hello", e.applyFilter("hello", "default('fallback')"))
	core.AssertEqual(t, "fallback", e.applyFilter("", "default('fallback')"))
}

func TestExecutor_ApplyFilter_Good_Bool(t *core.T) {
	e := NewExecutor("/tmp")

	core.AssertEqual(t, "true", e.applyFilter("true", "bool"))
	core.AssertEqual(t, "true", e.applyFilter("yes", "bool"))
	core.AssertEqual(t, "true", e.applyFilter("1", "bool"))
	core.AssertEqual(t, "false", e.applyFilter("false", "bool"))
	core.AssertEqual(t, "false", e.applyFilter("no", "bool"))
	core.AssertEqual(t, "false", e.applyFilter("anything", "bool"))
}

func TestExecutor_ApplyFilter_Good_Trim(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.applyFilter("  hello  ", "trim")
	core.AssertEqual(t, "hello", result)
}

func TestExecutor_ApplyFilter_Good_RegexReplace(t *core.T) {
	e := NewExecutor("/tmp")

	core.AssertEqual(t, "web-01", e.applyFilter("web_01", "regex_replace('_', '-')"))
	core.AssertEqual(t, "123", e.applyFilter("abc123", `regex_replace("\D+", "")`))
}

func TestExecutor_TemplateString_Good_ChainedFilters(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["padded"] = "  web01  "

	result := e.templateString("{{ missing_var | default('fallback') | trim }} {{ padded | trim }}", "", nil)

	core.AssertEqual(t, "fallback web01", result)
}

// --- resolveLoop ---

func TestExecutor_ResolveLoop_Good_SliceAny(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"a", "b", "c"}, "host1")
	core.AssertLen(t, items, 3)
}

func TestExecutor_ResolveLoop_Good_SliceString(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]string{"a", "b", "c"}, "host1")
	core.AssertLen(t, items, 3)
}

func TestExecutor_ResolveLoop_Good_Nil(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop(nil, "host1")
	core.AssertNil(t, items)
}

func TestExecutor_RunTaskOnHost_Good_LoopFromTemplatedListVariable(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["items"] = []any{"alpha", "beta"}
	e.clients["host1"] = NewMockSSHClient()

	task := &Task{
		Name:   "Templated list loop",
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		Loop:     "{{ items }}",
		Register: "loop_result",
	}

	err := e.runTaskOnHosts(context.Background(), []string{"host1"}, task, &Play{})
	core.RequireNoError(t, err)

	result := e.results["host1"]["loop_result"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, "alpha", result.Results[0].Msg)
	core.AssertEqual(t, "beta", result.Results[1].Msg)
}

// --- templateArgs ---

func TestExecutor_templateArgs_Good(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["myvar"] = "resolved"

	args := map[string]any{
		"plain":     "no template",
		"templated": "{{ myvar }}",
		"number":    42,
	}

	result := e.templateArgs(args, "host1", nil)
	core.AssertEqual(t, "no template", result["plain"])
	core.AssertEqual(t, "resolved", result["templated"])
	core.AssertEqual(t, 42, result["number"])
}

func TestExecutor_templateArgs_Good_NestedMap(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["port"] = "8080"

	args := map[string]any{
		"nested": map[string]any{
			"port": "{{ port }}",
		},
	}

	result := e.templateArgs(args, "host1", nil)
	nested := result["nested"].(map[string]any)
	core.AssertEqual(t, "8080", nested["port"])
}

func TestExecutor_templateArgs_Good_ArrayValues(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["pkg"] = "nginx"

	args := map[string]any{
		"packages": []any{"{{ pkg }}", "curl"},
	}

	result := e.templateArgs(args, "host1", nil)
	pkgs := result["packages"].([]any)
	core.AssertEqual(t, "nginx", pkgs[0])
	core.AssertEqual(t, "curl", pkgs[1])
}

// --- Helper functions ---

func TestExecutor_getStringArg_Good(t *core.T) {
	args := map[string]any{
		"name":   "value",
		"number": 42,
	}

	core.AssertEqual(t, "value", getStringArg(args, "name", ""))
	core.AssertEqual(t, "42", getStringArg(args, "number", ""))
	core.AssertEqual(t, "default", getStringArg(args, "missing", "default"))
}

func TestExecutor_getBoolArg_Good(t *core.T) {
	args := map[string]any{
		"enabled":  true,
		"disabled": false,
		"yes_str":  "yes",
		"true_str": "true",
		"one_str":  "1",
		"no_str":   "no",
	}

	core.AssertTrue(t, getBoolArg(args, "enabled", false))
	core.AssertFalse(t, getBoolArg(args, "disabled", true))
	core.AssertTrue(t, getBoolArg(args, "yes_str", false))
	core.AssertTrue(t, getBoolArg(args, "true_str", false))
	core.AssertTrue(t, getBoolArg(args, "one_str", false))
	core.AssertFalse(t, getBoolArg(args, "no_str", true))
	core.AssertTrue(t, getBoolArg(args, "missing", true))
	core.AssertFalse(t, getBoolArg(args, "missing", false))
}

// --- Close ---

func TestExecutor_Close_Good_EmptyClients(t *core.T) {
	e := NewExecutor("/tmp")
	// Should not panic with no clients
	e.Close()
	core.AssertEmpty(t, e.clients)
}

// --- File-aware public symbol triplets ---

func TestExecutor_NewExecutor_Bad(t *core.T) {
	executor := NewExecutor("")
	core.AssertNotNil(t, executor)
	core.AssertNotNil(t, executor.parser)
	core.AssertEmpty(t, executor.parser.basePath)
}

func TestExecutor_NewExecutor_Ugly(t *core.T) {
	executor := NewExecutor("relative/base")
	core.AssertNotNil(t, executor)
	core.AssertEqual(t, "relative/base", executor.parser.basePath)
	core.AssertNotNil(t, executor.clients)
}

func TestExecutor_Executor_SetVar_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetVar("answer", 42)
	core.AssertEqual(t, 42, executor.vars["answer"])
	core.AssertLen(t, executor.vars, 1)
}

func TestExecutor_Executor_SetVar_Bad(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetVar("", "empty")
	core.AssertEqual(t, "empty", executor.vars[""])
	core.AssertTrue(t, executor.vars != nil)
}

func TestExecutor_Executor_SetVar_Ugly(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetVar("nil", nil)
	core.AssertContains(t, executor.vars, "nil")
	core.AssertNil(t, executor.vars["nil"])
}

func TestExecutor_Executor_SetInventoryDirect_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	inv := testInventory()
	executor.SetInventoryDirect(inv)
	core.AssertEqual(t, inv, executor.inventory)
	core.AssertEmpty(t, executor.inventoryPath)
}

func TestExecutor_Executor_SetInventoryDirect_Bad(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetInventoryDirect(nil)
	core.AssertNil(t, executor.inventory)
	core.AssertEmpty(t, executor.inventoryPath)
}

func TestExecutor_Executor_SetInventoryDirect_Ugly(t *core.T) {
	executor := NewExecutor("/tmp")
	first := testInventory()
	second := &Inventory{All: &InventoryGroup{}}
	executor.SetInventoryDirect(first)
	executor.SetInventoryDirect(second)
	core.AssertEqual(t, second, executor.inventory)
}

func TestExecutor_Executor_SetInventory_Good(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "inventory.yml")
	writeTextFile(t, path, "all:\n  hosts:\n    web1: {}\n")
	executor := NewExecutor(dir)
	err := executor.SetInventory(path)
	core.AssertNoError(t, err)
	core.AssertContains(t, executor.inventory.All.Hosts, "web1")
}

func TestExecutor_Executor_SetInventory_Bad(t *core.T) {
	executor := NewExecutor(t.TempDir())
	err := executor.SetInventory("missing.yml")
	core.AssertError(t, err)
	core.AssertNil(t, executor.inventory)
}

func TestExecutor_Executor_SetInventory_Ugly(t *core.T) {
	dir := t.TempDir()
	writeTextFile(t, joinPath(dir, "hosts.yml"), "all:\n  hosts:\n    edge1: {}\n")
	executor := NewExecutor(dir)
	err := executor.SetInventory(dir)
	core.AssertNoError(t, err)
	core.AssertContains(t, executor.inventory.All.Hosts, "edge1")
}

func TestExecutor_Executor_SetMedium_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.SetMedium(coreio.Local)
	core.AssertNotNil(t, executor.parser.configuredMedium())
	core.AssertEqual(t, coreio.Local, executor.parser.configuredMedium())
}

func TestExecutor_Executor_SetMedium_Bad(t *core.T) {
	var executor *Executor
	core.AssertNotPanics(t, func() { executor.SetMedium(coreio.Local) })
	core.AssertNil(t, executor)
}

func TestExecutor_Executor_SetMedium_Ugly(t *core.T) {
	executor := &Executor{}
	core.AssertNotPanics(t, func() { executor.SetMedium(coreio.Local) })
	core.AssertNil(t, executor.parser)
}

func TestExecutor_Executor_Run_Good(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "site.yml")
	writeTextFile(t, path, "- hosts: localhost\n  gather_facts: false\n  tasks: []\n")
	executor := NewExecutor(dir)
	err := executor.Run(context.Background(), path)
	core.AssertNoError(t, err)
}

func TestExecutor_Executor_Run_Bad(t *core.T) {
	executor := NewExecutor(t.TempDir())
	err := executor.Run(context.Background(), "missing.yml")
	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "parse playbook")
}

func TestExecutor_Executor_Run_Ugly(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "site.yml")
	writeTextFile(t, path, "[]\n")
	executor := NewExecutor(dir)
	err := executor.Run(context.Background(), path)
	core.AssertNoError(t, err)
}

func TestExecutor_Executor_Close_Good(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.clients["local"] = newLocalClient()
	executor.Close()
	core.AssertNotNil(t, executor.clients)
	core.AssertLen(t, executor.clients, 0)
}

func TestExecutor_Executor_Close_Bad(t *core.T) {
	executor := NewExecutor("/tmp")
	executor.Close()
	core.AssertNotNil(t, executor.clients)
	core.AssertLen(t, executor.clients, 0)
}

func TestExecutor_Executor_Close_Ugly(t *core.T) {
	executor := &Executor{clients: nil}
	core.AssertNotPanics(t, executor.Close)
	core.AssertNotNil(t, executor.clients)
	core.AssertLen(t, executor.clients, 0)
}

func TestExecutor_Executor_TemplateFile_Good(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "template.j2")
	writeTextFile(t, path, "hello {{ name }}")
	executor := NewExecutor(dir)
	executor.SetVar("name", "world")
	content, err := executor.TemplateFile(path, "", nil)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "hello world", content)
}

func TestExecutor_Executor_TemplateFile_Bad(t *core.T) {
	executor := NewExecutor(t.TempDir())
	content, err := executor.TemplateFile("", "", nil)
	core.AssertError(t, err)
	core.AssertEmpty(t, content)
}

func TestExecutor_Executor_TemplateFile_Ugly(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "template.j2")
	writeTextFile(t, path, "hello {{ missing }}")
	executor := NewExecutor(dir)
	content, err := executor.TemplateFile(path, "", nil)
	core.AssertNoError(t, err)
	core.AssertEqual(t, "hello {{ missing }}", content)
}
