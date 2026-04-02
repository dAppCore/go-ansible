package ansible

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Tests for non-SSH module handlers (0% coverage)
// ============================================================

// --- moduleDebug ---

func TestExecutorExtra_ModuleDebug_Good_Message(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleDebug(map[string]any{"msg": "Hello world"})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Equal(t, "Hello world", result.Msg)
}

func TestExecutorExtra_ModuleDebug_Good_Var(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_version"] = "1.2.3"

	result, err := e.moduleDebug(map[string]any{"var": "my_version"})

	require.NoError(t, err)
	assert.Contains(t, result.Msg, "1.2.3")
}

func TestExecutorExtra_ModuleDebug_Good_EmptyArgs(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleDebug(map[string]any{})

	require.NoError(t, err)
	assert.Equal(t, "", result.Msg)
}

// --- moduleFail ---

func TestExecutorExtra_ModuleFail_Good_DefaultMessage(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleFail(map[string]any{})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Equal(t, "Failed as requested", result.Msg)
}

func TestExecutorExtra_ModuleFail_Good_CustomMessage(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleFail(map[string]any{"msg": "deployment blocked"})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Equal(t, "deployment blocked", result.Msg)
}

// --- modulePing ---

func TestExecutorExtra_ModulePing_Good_DefaultPong(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "ping",
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.Equal(t, "pong", result.Msg)
	assert.True(t, mock.hasExecuted(`^true$`))
}

func TestExecutorExtra_ModulePing_Good_CustomData(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "ansible.builtin.ping",
		Args: map[string]any{
			"data": "hello",
		},
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, "hello", result.Msg)
}

// --- moduleAssert ---

func TestExecutorExtra_ModuleAssert_Good_PassingAssertion(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	result, err := e.moduleAssert(map[string]any{"that": "enabled"}, "host1")

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, "All assertions passed", result.Msg)
}

func TestExecutorExtra_ModuleAssert_Bad_FailingAssertion(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = false

	result, err := e.moduleAssert(map[string]any{"that": "enabled"}, "host1")

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "Assertion failed")
}

func TestExecutorExtra_ModuleAssert_Bad_MissingThat(t *testing.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleAssert(map[string]any{}, "host1")
	assert.Error(t, err)
}

func TestExecutorExtra_ModuleAssert_Good_CustomFailMsg(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["ready"] = false

	result, err := e.moduleAssert(map[string]any{
		"that":     "ready",
		"fail_msg": "Service not ready",
	}, "host1")

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Equal(t, "Service not ready", result.Msg)
}

func TestExecutorExtra_ModuleAssert_Good_MultipleConditions(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.vars["count"] = 5

	result, err := e.moduleAssert(map[string]any{
		"that": []any{"enabled", "count"},
	}, "host1")

	require.NoError(t, err)
	assert.False(t, result.Failed)
}

// --- moduleSetFact ---

func TestExecutorExtra_ModuleSetFact_Good(t *testing.T) {
	e := NewExecutor("/tmp")

	result, err := e.moduleSetFact(map[string]any{
		"app_version": "2.0.0",
		"deploy_env":  "production",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "2.0.0", e.vars["app_version"])
	assert.Equal(t, "production", e.vars["deploy_env"])
}

func TestExecutorExtra_ModuleSetFact_Good_SkipsCacheable(t *testing.T) {
	e := NewExecutor("/tmp")

	e.moduleSetFact(map[string]any{
		"my_fact":   "value",
		"cacheable": true,
	})

	assert.Equal(t, "value", e.vars["my_fact"])
	_, hasCacheable := e.vars["cacheable"]
	assert.False(t, hasCacheable)
}

// --- moduleAddHost ---

func TestExecutorExtra_ModuleAddHost_Good_AddsHostAndGroups(t *testing.T) {
	e := NewExecutor("/tmp")

	result, err := e.moduleAddHost(map[string]any{
		"name":                    "db1",
		"groups":                  "databases,production",
		"ansible_host":            "10.0.0.5",
		"ansible_port":            "2222",
		"ansible_user":            "deploy",
		"ansible_connection":      "ssh",
		"ansible_become_password": "secret",
		"environment":             "prod",
		"custom_var":              "custom-value",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "db1", result.Data["host"])
	assert.Contains(t, result.Msg, "db1")

	require.NotNil(t, e.inventory)
	require.NotNil(t, e.inventory.All)
	require.NotNil(t, e.inventory.All.Hosts["db1"])

	host := e.inventory.All.Hosts["db1"]
	assert.Equal(t, "10.0.0.5", host.AnsibleHost)
	assert.Equal(t, 2222, host.AnsiblePort)
	assert.Equal(t, "deploy", host.AnsibleUser)
	assert.Equal(t, "ssh", host.AnsibleConnection)
	assert.Equal(t, "secret", host.AnsibleBecomePassword)
	assert.Equal(t, "custom-value", host.Vars["custom_var"])

	require.NotNil(t, e.inventory.All.Children["databases"])
	require.NotNil(t, e.inventory.All.Children["production"])
	assert.Same(t, host, e.inventory.All.Children["databases"].Hosts["db1"])
	assert.Same(t, host, e.inventory.All.Children["production"].Hosts["db1"])

	assert.Equal(t, []string{"db1"}, GetHosts(e.inventory, "all"))
	assert.Equal(t, []string{"db1"}, GetHosts(e.inventory, "databases"))
	assert.Equal(t, []string{"db1"}, GetHosts(e.inventory, "production"))
}

func TestExecutorExtra_ModuleAddHost_Good_ThroughDispatcher(t *testing.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		Module: "add_host",
		Args: map[string]any{
			"name":  "cache1",
			"group": "caches",
			"role":  "redis",
		},
	}

	result, err := e.executeModule(context.Background(), "host1", &SSHClient{}, task, &Play{})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "cache1", result.Data["host"])
	assert.Equal(t, []string{"caches"}, result.Data["groups"])
	assert.Equal(t, []string{"cache1"}, GetHosts(e.inventory, "all"))
	assert.Equal(t, []string{"cache1"}, GetHosts(e.inventory, "caches"))
	assert.Equal(t, "redis", e.inventory.All.Hosts["cache1"].Vars["role"])
}

// --- moduleGroupBy ---

func TestExecutorExtra_ModuleGroupBy_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": &Host{AnsibleHost: "10.0.0.10"},
			},
		},
	})

	result, err := e.moduleGroupBy("web1", map[string]any{"key": "debian"})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "web1", result.Data["host"])
	assert.Equal(t, "debian", result.Data["group"])
	assert.Contains(t, result.Msg, "web1")
	assert.Contains(t, result.Msg, "debian")
	assert.Equal(t, []string{"web1"}, GetHosts(e.inventory, "debian"))
}

func TestExecutorExtra_ModuleGroupBy_Good_ThroughDispatcher(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	task := &Task{
		Module: "group_by",
		Args: map[string]any{
			"key": "linux",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, []string{"host1"}, GetHosts(e.inventory, "linux"))
}

func TestExecutorExtra_ModuleGroupBy_Bad_MissingKey(t *testing.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleGroupBy("host1", map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key required")
}

// --- moduleIncludeVars ---

func TestExecutorExtra_ModuleIncludeVars_Good_WithFile(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "main.yml")
	require.NoError(t, writeTestFile(path, []byte("app_name: demo\n"), 0644))

	e := NewExecutor("/tmp")
	result, err := e.moduleIncludeVars(map[string]any{"file": path})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Contains(t, result.Msg, path)
	assert.Equal(t, "demo", e.vars["app_name"])
}

func TestExecutorExtra_ModuleIncludeVars_Good_WithRawParams(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "defaults.yml")
	require.NoError(t, writeTestFile(path, []byte("app_port: 8080\n"), 0644))

	e := NewExecutor("/tmp")
	result, err := e.moduleIncludeVars(map[string]any{"_raw_params": path})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Contains(t, result.Msg, path)
	assert.Equal(t, 8080, e.vars["app_port"])
}

func TestExecutorExtra_ModuleIncludeVars_Good_Empty(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleIncludeVars(map[string]any{})

	require.NoError(t, err)
	assert.False(t, result.Changed)
}

// --- moduleMeta ---

func TestExecutorExtra_ModuleMeta_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "flush_handlers"})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "flush_handlers", result.Data["action"])
}

func TestExecutorExtra_ModuleMeta_Good_ClearFacts(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{Hostname: "web01"}

	result, err := e.moduleMeta(map[string]any{"_raw_params": "clear_facts"})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "clear_facts", result.Data["action"])
}

func TestExecutorExtra_ModuleMeta_Good_ResetConnection(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "reset_connection"})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "reset_connection", result.Data["action"])
}

func TestExecutorExtra_ModuleMeta_Good_EndHost(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "end_host"})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "end_host", result.Data["action"])
}

func TestExecutorExtra_ModuleMeta_Good_EndBatch(t *testing.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "end_batch"})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "end_batch", result.Data["action"])
}

func TestExecutorExtra_HandleMetaAction_Good_ClearFacts(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{Hostname: "web01"}
	e.facts["host2"] = &Facts{Hostname: "web02"}

	result := &TaskResult{Data: map[string]any{"action": "clear_facts"}}
	require.NoError(t, e.handleMetaAction(context.Background(), "host1", []string{"host1"}, nil, result))

	_, ok := e.facts["host1"]
	assert.False(t, ok)
	require.NotNil(t, e.facts["host2"])
	assert.Equal(t, "web02", e.facts["host2"].Hostname)
}

func TestExecutorExtra_HandleMetaAction_Good_EndHost(t *testing.T) {
	e := NewExecutor("/tmp")

	result := &TaskResult{Data: map[string]any{"action": "end_host"}}
	err := e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, nil, result)

	require.ErrorIs(t, err, errEndHost)
	assert.True(t, e.isHostEnded("host1"))
	assert.False(t, e.isHostEnded("host2"))
}

func TestExecutorExtra_HandleMetaAction_Good_EndBatch(t *testing.T) {
	e := NewExecutor("/tmp")

	result := &TaskResult{Data: map[string]any{"action": "end_batch"}}
	err := e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, nil, result)

	require.ErrorIs(t, err, errEndBatch)
	assert.False(t, e.isHostEnded("host1"))
	assert.False(t, e.isHostEnded("host2"))
}

func TestExecutorExtra_HandleMetaAction_Good_ResetConnection(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	e.clients["host1"] = mock
	e.clients["host2"] = NewMockSSHClient()

	result := &TaskResult{Data: map[string]any{"action": "reset_connection"}}
	require.NoError(t, e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, nil, result))

	_, ok := e.clients["host1"]
	assert.False(t, ok)
	_, ok = e.clients["host2"]
	assert.True(t, ok)
	assert.True(t, mock.closed)
}

func TestExecutorExtra_RunTaskOnHosts_Good_EndHostSkipsFutureTasks(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {Vars: map[string]any{"retire_host": true}},
				"host2": {Vars: map[string]any{"retire_host": false}},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	play := &Play{}
	first := &Task{
		Name:   "Retire host",
		Module: "meta",
		Args:   map[string]any{"_raw_params": "end_host"},
		When:   "retire_host",
	}
	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"host1", "host2"}, first, play))
	assert.True(t, e.isHostEnded("host1"))
	assert.False(t, e.isHostEnded("host2"))

	second := &Task{
		Name:   "Follow-up",
		Module: "debug",
		Args:   map[string]any{"msg": "still running"},
	}
	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"host1", "host2"}, second, play))

	assert.Contains(t, started, "host1:Retire host")
	assert.Contains(t, started, "host2:Follow-up")
	assert.NotContains(t, started, "host1:Follow-up")
}

func TestExecutorExtra_RunPlay_Good_MetaEndBatchAdvancesToNextSerialBatch(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {Vars: map[string]any{"end_batch": true}},
				"host2": {Vars: map[string]any{"end_batch": false}},
				"host3": {Vars: map[string]any{"end_batch": false}},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()
	e.clients["host3"] = NewMockSSHClient()

	serial := 1
	gatherFacts := false
	play := &Play{
		Hosts:       "all",
		Serial:      serial,
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:   "end current batch",
				Module: "meta",
				Args:   map[string]any{"_raw_params": "end_batch"},
				When:   "end_batch",
			},
			{
				Name:   "follow-up",
				Module: "debug",
				Args:   map[string]any{"msg": "next batch"},
			},
		},
	}

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	require.NoError(t, e.runPlay(context.Background(), play))

	assert.Contains(t, started, "host1:end current batch")
	assert.NotContains(t, started, "host1:follow-up")
	assert.Contains(t, started, "host2:follow-up")
	assert.Contains(t, started, "host3:follow-up")
}

func TestExecutorExtra_SplitSerialHosts_Good_ListValues(t *testing.T) {
	batches := splitSerialHosts([]string{"host1", "host2", "host3", "host4"}, []any{1, "50%"})

	require.Len(t, batches, 3)
	assert.Equal(t, []string{"host1"}, batches[0])
	assert.Equal(t, []string{"host2", "host3"}, batches[1])
	assert.Equal(t, []string{"host4"}, batches[2])
}

func TestExecutorExtra_SplitSerialHosts_Good_ListRepeatsLastValue(t *testing.T) {
	batches := splitSerialHosts([]string{"host1", "host2", "host3", "host4", "host5"}, []any{2, 1})

	require.Len(t, batches, 4)
	assert.Equal(t, []string{"host1", "host2"}, batches[0])
	assert.Equal(t, []string{"host3"}, batches[1])
	assert.Equal(t, []string{"host4"}, batches[2])
	assert.Equal(t, []string{"host5"}, batches[3])
}

// ============================================================
// Tests for handleLookup (0% coverage)
// ============================================================

func TestExecutorExtra_HandleLookup_Good_EnvVar(t *testing.T) {
	e := NewExecutor("/tmp")
	t.Setenv("TEST_ANSIBLE_LOOKUP", "found_it")

	result := e.handleLookup("lookup('env', 'TEST_ANSIBLE_LOOKUP')", "", nil)
	assert.Equal(t, "found_it", result)
}

func TestExecutorExtra_HandleLookup_Good_EnvVarMissing(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.handleLookup("lookup('env', 'NONEXISTENT_VAR_12345')", "", nil)
	assert.Equal(t, "", result)
}

func TestExecutorExtra_HandleLookup_Good_FileLookupResolvesBasePath(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "vars.txt"), []byte("from base path"), 0644))

	e := NewExecutor(dir)
	result := e.handleLookup("lookup('file', 'vars.txt')", "", nil)

	assert.Equal(t, "from base path", result)
}

func TestExecutorExtra_HandleLookup_Good_VarsLookup(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetVar("lookup_value", "resolved from vars")

	result := e.handleLookup("lookup('vars', 'lookup_value')", "", nil)

	assert.Equal(t, "resolved from vars", result)
}

func TestExecutorExtra_HandleLookup_Bad_InvalidSyntax(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.handleLookup("lookup(invalid)", "", nil)
	assert.Equal(t, "", result)
}

// ============================================================
// Tests for SetInventory (0% coverage)
// ============================================================

func TestExecutorExtra_SetInventory_Good(t *testing.T) {
	dir := t.TempDir()
	invPath := joinPath(dir, "inventory.yml")
	yaml := `all:
  hosts:
    web1:
      ansible_host: 10.0.0.1
    web2:
      ansible_host: 10.0.0.2
`
	require.NoError(t, writeTestFile(invPath, []byte(yaml), 0644))

	e := NewExecutor(dir)
	err := e.SetInventory(invPath)

	require.NoError(t, err)
	assert.NotNil(t, e.inventory)
	assert.Len(t, e.inventory.All.Hosts, 2)
}

func TestExecutorExtra_SetInventory_Good_Directory(t *testing.T) {
	dir := t.TempDir()
	inventoryDir := joinPath(dir, "inventory")
	require.NoError(t, os.MkdirAll(inventoryDir, 0755))

	invPath := joinPath(inventoryDir, "inventory.yml")
	yaml := `all:
  hosts:
    web1:
      ansible_host: 10.0.0.1
`
	require.NoError(t, writeTestFile(invPath, []byte(yaml), 0644))

	e := NewExecutor(dir)
	err := e.SetInventory(inventoryDir)

	require.NoError(t, err)
	assert.NotNil(t, e.inventory)
	assert.Contains(t, e.inventory.All.Hosts, "web1")
}

func TestExecutorExtra_SetInventory_Bad_FileNotFound(t *testing.T) {
	e := NewExecutor("/tmp")
	err := e.SetInventory("/nonexistent/inventory.yml")
	assert.Error(t, err)
}

// ============================================================
// Tests for iterator functions (0% coverage)
// ============================================================

func TestExecutorExtra_ParsePlaybookIter_Good(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "playbook.yml")
	yaml := `- name: First play
  hosts: all
  tasks:
    - debug:
        msg: hello

- name: Second play
  hosts: localhost
  tasks:
    - debug:
        msg: world
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	iter, err := p.ParsePlaybookIter(path)
	require.NoError(t, err)

	var plays []Play
	for play := range iter {
		plays = append(plays, play)
	}
	assert.Len(t, plays, 2)
	assert.Equal(t, "First play", plays[0].Name)
	assert.Equal(t, "Second play", plays[1].Name)
}

func TestExecutorExtra_ParsePlaybookIter_Bad_InvalidFile(t *testing.T) {
	_, err := NewParser("/tmp").ParsePlaybookIter("/nonexistent.yml")
	assert.Error(t, err)
}

func TestExecutorExtra_ParseTasksIter_Good(t *testing.T) {
	dir := t.TempDir()
	path := joinPath(dir, "tasks.yml")
	yaml := `- name: Task one
  debug:
    msg: first

- name: Task two
  debug:
    msg: second
`
	require.NoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	iter, err := p.ParseTasksIter(path)
	require.NoError(t, err)

	var tasks []Task
	for task := range iter {
		tasks = append(tasks, task)
	}
	assert.Len(t, tasks, 2)
	assert.Equal(t, "Task one", tasks[0].Name)
}

func TestExecutorExtra_ParseTasksIter_Bad_InvalidFile(t *testing.T) {
	_, err := NewParser("/tmp").ParseTasksIter("/nonexistent.yml")
	assert.Error(t, err)
}

func TestExecutorExtra_RunIncludeTasks_Good_RelativePath(t *testing.T) {
	dir := t.TempDir()
	includedPath := joinPath(dir, "included.yml")
	yaml := `- name: Included first task
  debug:
    msg: first

- name: Included second task
  debug:
    msg: second
`
	require.NoError(t, writeTestFile(includedPath, []byte(yaml), 0644))

	gatherFacts := false
	play := &Play{
		Name:        "Include tasks",
		Hosts:       "localhost",
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:         "Load included tasks",
				IncludeTasks: "included.yml",
			},
		},
	}

	e := NewExecutor(dir)
	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	require.NoError(t, e.runPlay(context.Background(), play))

	assert.Contains(t, started, "localhost:Included first task")
	assert.Contains(t, started, "localhost:Included second task")
}

func TestExecutorExtra_RunIncludeTasks_Good_InheritsTaskVars(t *testing.T) {
	dir := t.TempDir()
	includedPath := joinPath(dir, "included-vars.yml")
	yaml := `- name: Included var task
  debug:
    msg: "{{ include_message }}"
  register: included_result
`
	require.NoError(t, writeTestFile(includedPath, []byte(yaml), 0644))

	gatherFacts := false
	play := &Play{
		Name:        "Include vars",
		Hosts:       "localhost",
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:         "Load included tasks",
				IncludeTasks: "included-vars.yml",
				Vars:         map[string]any{"include_message": "hello from include"},
			},
		},
	}

	e := NewExecutor(dir)
	require.NoError(t, e.runPlay(context.Background(), play))

	require.NotNil(t, e.results["localhost"]["included_result"])
	assert.Equal(t, "hello from include", e.results["localhost"]["included_result"].Msg)
}

func TestExecutorExtra_RunIncludeTasks_Good_HostSpecificTemplate(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, writeTestFile(joinPath(dir, "web.yml"), []byte(`- name: Web included task
  debug:
    msg: web
`), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "db.yml"), []byte(`- name: DB included task
  debug:
    msg: db
`), 0644))

	gatherFacts := false
	play := &Play{
		Name:        "Include host-specific tasks",
		Hosts:       "all",
		Connection:  "local",
		GatherFacts: &gatherFacts,
	}

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {
					AnsibleConnection: "local",
					Vars: map[string]any{
						"include_file": "web.yml",
					},
				},
				"db1": {
					AnsibleConnection: "local",
					Vars: map[string]any{
						"include_file": "db.yml",
					},
				},
			},
		},
	})

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"web1", "db1"}, &Task{
		Name:         "Load host-specific tasks",
		IncludeTasks: "{{ include_file }}",
	}, play))

	assert.Contains(t, started, "web1:Web included task")
	assert.Contains(t, started, "db1:DB included task")
}

func TestExecutorExtra_RunIncludeRole_Good_InheritsTaskVars(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "roles", "demo", "tasks", "main.yml"), []byte(`---
- name: Role var task
  debug:
    msg: "{{ role_message }}"
  register: role_result
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
	}

	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name: "Load role",
		IncludeRole: &RoleRef{
			Role: "demo",
		},
		Vars: map[string]any{"role_message": "hello from role"},
	}, play))

	require.NotNil(t, e.results["localhost"]["role_result"])
	assert.Equal(t, "hello from role", e.results["localhost"]["role_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_AppliesRoleDefaults(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "roles", "app", "tasks", "main.yml"), []byte(`---
- name: Applied role task
  vars:
    role_message: from-task
  shell: printf '%s|%s|%s' "$APP_ENV" "{{ apply_message }}" "{{ role_message }}"
  register: role_result
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
	}

	var started *Task
	e.OnTaskStart = func(host string, task *Task) {
		if task.Name == "Applied role task" {
			started = task
		}
	}

	// Re-run with callback attached so we can inspect the merged task state.
	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name: "Load role with apply",
		IncludeRole: &RoleRef{
			Role: "app",
			Apply: &TaskApply{
				Tags: []string{"role-apply"},
				Vars: map[string]any{
					"apply_message": "from-apply",
					"role_message":  "from-apply",
				},
				Environment: map[string]string{
					"APP_ENV": "production",
				},
			},
		},
	}, play))

	require.NotNil(t, started)
	assert.Contains(t, started.Tags, "role-apply")
	assert.Equal(t, "production", started.Environment["APP_ENV"])
	assert.Equal(t, "from-apply", started.Vars["apply_message"])
	assert.Equal(t, "from-task", started.Vars["role_message"])
	require.NotNil(t, e.results["localhost"]["role_result"])
	assert.Equal(t, "production|from-apply|from-task", e.results["localhost"]["role_result"].Stdout)
}

func TestExecutorExtra_RunIncludeRole_Good_AppliesRoleWhen(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "roles", "app", "tasks", "main.yml"), []byte(`---
- name: Conditional role task
  debug:
    msg: "role task ran"
  when: task_enabled
  register: role_result
`), 0644))

	e, _ := newTestExecutorWithMock("localhost")
	e.parser = NewParser(dir)
	e.SetVar("task_enabled", true)
	e.SetVar("apply_enabled", false)

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
	}

	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name: "Load role with conditional apply",
		IncludeRole: &RoleRef{
			Role: "app",
			Apply: &TaskApply{
				When: "apply_enabled",
			},
		},
	}, play))

	require.NotNil(t, e.results["localhost"]["role_result"])
	assert.True(t, e.results["localhost"]["role_result"].Skipped)
	assert.Equal(t, "Skipped due to when condition", e.results["localhost"]["role_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_UsesRoleRefTagsForSelection(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "roles", "tagged", "tasks", "main.yml"), []byte(`---
- name: Tagged role task
  debug:
    msg: "role task ran"
  register: role_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.Tags = []string{"role-tag"}

	gatherFacts := false
	play := &Play{
		Hosts:       "host1",
		GatherFacts: &gatherFacts,
	}

	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, &Task{
		Name: "Load tagged role",
		IncludeRole: &RoleRef{
			Role: "tagged",
			Tags: []string{"role-tag"},
		},
	}, play))

	require.NotNil(t, e.results["host1"]["role_result"])
	assert.Equal(t, "role task ran", e.results["host1"]["role_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_HonoursRoleRefWhen(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "roles", "conditional", "tasks", "main.yml"), []byte(`---
- name: Conditional role task
  debug:
    msg: "role task ran"
  register: role_result
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
			},
		},
	})
	e.SetVar("role_enabled", false)

	gatherFacts := false
	play := &Play{
		Hosts:       "host1",
		GatherFacts: &gatherFacts,
	}

	var started []string
	e.OnTaskStart = func(_ string, task *Task) {
		started = append(started, task.Name)
	}

	require.NoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, &Task{
		Name: "Load conditional role",
		IncludeRole: &RoleRef{
			Role: "conditional",
			When: "role_enabled",
		},
	}, play))

	assert.Empty(t, started)
	if results := e.results["host1"]; results != nil {
		_, ok := results["role_result"]
		assert.False(t, ok)
	}
}

func TestExecutorExtra_RunIncludeRole_Good_PublicVarsPersist(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "roles", "shared", "tasks", "main.yml"), []byte(`---
- name: Shared role task
  debug:
    msg: "{{ shared_message }}"
  register: shared_role_result
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
		Tasks: []Task{
			{
				Name: "Load public role",
				IncludeRole: &RoleRef{
					Role:   "shared",
					Public: true,
				},
				Vars: map[string]any{"shared_message": "hello from public role"},
			},
			{
				Name:   "Use public role vars",
				Module: "debug",
				Args: map[string]any{
					"msg": "{{ shared_message }}",
				},
				Register: "after_public_role",
			},
		},
	}

	require.NoError(t, e.runPlay(context.Background(), play))

	require.NotNil(t, e.results["localhost"]["shared_role_result"])
	assert.Equal(t, "hello from public role", e.results["localhost"]["shared_role_result"].Msg)
	require.NotNil(t, e.results["localhost"]["after_public_role"])
	assert.Equal(t, "hello from public role", e.results["localhost"]["after_public_role"].Msg)
}

func TestExecutorExtra_GetHostsIter_Good(t *testing.T) {
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {},
				"web2": {},
				"db1":  {},
			},
		},
	}

	var hosts []string
	for host := range GetHostsIter(inv, "all") {
		hosts = append(hosts, host)
	}
	assert.Len(t, hosts, 3)
}

func TestExecutorExtra_AllHostsIter_Good(t *testing.T) {
	group := &InventoryGroup{
		Hosts: map[string]*Host{
			"alpha": {},
			"beta":  {},
		},
		Children: map[string]*InventoryGroup{
			"db": {
				Hosts: map[string]*Host{
					"gamma": {},
				},
			},
		},
	}

	var hosts []string
	for host := range AllHostsIter(group) {
		hosts = append(hosts, host)
	}
	assert.Len(t, hosts, 3)
	// AllHostsIter sorts keys
	assert.Equal(t, "alpha", hosts[0])
	assert.Equal(t, "beta", hosts[1])
	assert.Equal(t, "gamma", hosts[2])
}

func TestExecutorExtra_AllHostsIter_Good_NilGroup(t *testing.T) {
	var count int
	for range AllHostsIter(nil) {
		count++
	}
	assert.Equal(t, 0, count)
}

// ============================================================
// Tests for resolveExpr with registered vars (additional coverage)
// ============================================================

func TestExecutorExtra_ResolveExpr_Good_RegisteredVarFields(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {
			Stdout:  "output text",
			Stderr:  "error text",
			RC:      0,
			Changed: true,
			Failed:  false,
		},
	}

	assert.Equal(t, "output text", e.resolveExpr("cmd_result.stdout", "host1", nil))
	assert.Equal(t, "error text", e.resolveExpr("cmd_result.stderr", "host1", nil))
	assert.Equal(t, "0", e.resolveExpr("cmd_result.rc", "host1", nil))
	assert.Equal(t, "true", e.resolveExpr("cmd_result.changed", "host1", nil))
	assert.Equal(t, "false", e.resolveExpr("cmd_result.failed", "host1", nil))
}

func TestExecutorExtra_ResolveExpr_Good_TaskVars(t *testing.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		Vars: map[string]any{"local_var": "local_value"},
	}

	result := e.resolveExpr("local_var", "host1", task)
	assert.Equal(t, "local_value", result)
}

func TestExecutorExtra_ResolveExpr_Good_HostVars(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {AnsibleHost: "10.0.0.1"},
			},
		},
	})

	result := e.resolveExpr("ansible_host", "host1", nil)
	assert.Equal(t, "10.0.0.1", result)
}

func TestExecutorExtra_ResolveExpr_Good_Facts(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname:     "web01",
		FQDN:         "web01.example.com",
		Distribution: "ubuntu",
		Version:      "22.04",
		Architecture: "x86_64",
		Kernel:       "5.15.0",
	}

	assert.Equal(t, "web01", e.resolveExpr("ansible_hostname", "host1", nil))
	assert.Equal(t, "web01.example.com", e.resolveExpr("ansible_fqdn", "host1", nil))
	assert.Equal(t, "ubuntu", e.resolveExpr("ansible_distribution", "host1", nil))
	assert.Equal(t, "22.04", e.resolveExpr("ansible_distribution_version", "host1", nil))
	assert.Equal(t, "x86_64", e.resolveExpr("ansible_architecture", "host1", nil))
	assert.Equal(t, "5.15.0", e.resolveExpr("ansible_kernel", "host1", nil))
}

// --- applyFilter additional coverage ---

func TestExecutorExtra_ApplyFilter_Good_B64Decode(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "hello", e.applyFilter("aGVsbG8=", "b64decode"))
}

func TestExecutorExtra_ApplyFilter_Good_B64Encode(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "aGVsbG8=", e.applyFilter("hello", "b64encode"))
}

func TestExecutorExtra_ApplyFilter_Good_UnknownFilter(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "value", e.applyFilter("value", "unknown_filter"))
}

// --- evalCondition with default filter ---

func TestExecutorExtra_EvalCondition_Good_DefaultFilter(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.True(t, e.evalCondition("myvar | default('fallback')", "host1"))
}

func TestExecutorExtra_EvalCondition_Good_UndefinedCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.True(t, e.evalCondition("missing_var is not defined", "host1"))
	assert.True(t, e.evalCondition("missing_var is undefined", "host1"))
}

// --- resolveExpr with filter pipe ---

func TestExecutorExtra_ResolveExpr_Good_WithFilter(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["raw_value"] = "  trimmed  "

	result := e.resolveExpr("raw_value | trim", "host1", nil)
	assert.Equal(t, "trimmed", result)
}

func TestExecutorExtra_ResolveExpr_Good_WithB64Encode(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["raw_value"] = "hello"

	result := e.resolveExpr("raw_value | b64encode", "host1", nil)
	assert.Equal(t, "aGVsbG8=", result)
}
