package ansible

import (
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
}

// ============================================================
// Tests for handleLookup (0% coverage)
// ============================================================

func TestExecutorExtra_HandleLookup_Good_EnvVar(t *testing.T) {
	e := NewExecutor("/tmp")
	t.Setenv("TEST_ANSIBLE_LOOKUP", "found_it")

	result := e.handleLookup("lookup('env', 'TEST_ANSIBLE_LOOKUP')")
	assert.Equal(t, "found_it", result)
}

func TestExecutorExtra_HandleLookup_Good_EnvVarMissing(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.handleLookup("lookup('env', 'NONEXISTENT_VAR_12345')")
	assert.Equal(t, "", result)
}

func TestExecutorExtra_HandleLookup_Bad_InvalidSyntax(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.handleLookup("lookup(invalid)")
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
	// b64decode is a no-op stub currently
	assert.Equal(t, "hello", e.applyFilter("hello", "b64decode"))
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
