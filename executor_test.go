package ansible

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- NewExecutor ---

func TestExecutor_NewExecutor_Good(t *testing.T) {
	e := NewExecutor("/some/path")

	assert.NotNil(t, e)
	assert.NotNil(t, e.parser)
	assert.NotNil(t, e.vars)
	assert.NotNil(t, e.facts)
	assert.NotNil(t, e.results)
	assert.NotNil(t, e.handlers)
	assert.NotNil(t, e.notified)
	assert.NotNil(t, e.clients)
}

// --- SetVar ---

func TestExecutor_SetVar_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetVar("foo", "bar")
	e.SetVar("count", 42)

	assert.Equal(t, "bar", e.vars["foo"])
	assert.Equal(t, 42, e.vars["count"])
}

// --- SetInventoryDirect ---

func TestExecutor_SetInventoryDirect_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	inv := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {AnsibleHost: "10.0.0.1"},
			},
		},
	}

	e.SetInventoryDirect(inv)
	assert.Equal(t, inv, e.inventory)
}

// --- getHosts ---

func TestExecutor_GetHosts_Good_WithInventory(t *testing.T) {
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
	assert.Len(t, hosts, 2)
}

func TestExecutor_GetHosts_Good_Localhost(t *testing.T) {
	e := NewExecutor("/tmp")
	// No inventory set

	hosts := e.getHosts("localhost")
	assert.Equal(t, []string{"localhost"}, hosts)
}

func TestExecutor_GetHosts_Good_NoInventory(t *testing.T) {
	e := NewExecutor("/tmp")

	hosts := e.getHosts("webservers")
	assert.Nil(t, hosts)
}

func TestExecutor_GetHosts_Good_WithLimit(t *testing.T) {
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
	assert.Len(t, hosts, 1)
	assert.Contains(t, hosts, "host2")
}

// --- matchesTags ---

func TestExecutor_MatchesTags_Good_NoTagsFilter(t *testing.T) {
	e := NewExecutor("/tmp")

	assert.True(t, e.matchesTags(nil))
	assert.True(t, e.matchesTags([]string{"any", "tags"}))
}

func TestExecutor_MatchesTags_Good_IncludeTag(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	assert.True(t, e.matchesTags([]string{"deploy"}))
	assert.True(t, e.matchesTags([]string{"setup", "deploy"}))
	assert.False(t, e.matchesTags([]string{"other"}))
}

func TestExecutor_MatchesTags_Good_SkipTag(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SkipTags = []string{"slow"}

	assert.True(t, e.matchesTags([]string{"fast"}))
	assert.False(t, e.matchesTags([]string{"slow"}))
	assert.False(t, e.matchesTags([]string{"fast", "slow"}))
}

func TestExecutor_MatchesTags_Good_AllTag(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"all"}

	assert.True(t, e.matchesTags([]string{"anything"}))
}

func TestExecutor_MatchesTags_Good_NoTaskTags(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Tasks with no tags should not match when include tags are set
	assert.False(t, e.matchesTags(nil))
	assert.False(t, e.matchesTags([]string{}))
}

// --- handleNotify ---

func TestExecutor_HandleNotify_Good_String(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("restart nginx")

	assert.True(t, e.notified["restart nginx"])
}

func TestExecutor_HandleNotify_Good_StringList(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]string{"restart nginx", "reload config"})

	assert.True(t, e.notified["restart nginx"])
	assert.True(t, e.notified["reload config"])
}

func TestExecutor_HandleNotify_Good_AnyList(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]any{"restart nginx", "reload config"})

	assert.True(t, e.notified["restart nginx"])
	assert.True(t, e.notified["reload config"])
}

// --- run_once ---

func TestExecutor_RunTaskOnHosts_Good_RunOnceSharesRegisteredResult(t *testing.T) {
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
	require.NoError(t, err)

	assert.Len(t, started, 1)
	assert.Len(t, e.results["host1"], 1)
	assert.Len(t, e.results["host2"], 1)
	require.NotNil(t, e.results["host1"]["debug_result"])
	require.NotNil(t, e.results["host2"]["debug_result"])
	assert.Equal(t, "hello", e.results["host1"]["debug_result"].Msg)
	assert.Equal(t, "hello", e.results["host2"]["debug_result"].Msg)
}

func TestExecutor_RunTaskWithRetries_Good_UntilSuccess(t *testing.T) {
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

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 2, attempts)
	assert.False(t, result.Failed)
	assert.True(t, result.Changed)
	assert.Equal(t, "ok", result.Msg)
}

// --- check mode ---

func TestExecutor_RunTaskOnHost_Good_CheckModeSkipsMutatingTask(t *testing.T) {
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

	err := e.runTaskOnHost(context.Background(), "host1", task, &Play{})
	require.NoError(t, err)

	require.NotNil(t, ended)
	assert.True(t, ended.Skipped)
	assert.False(t, ended.Changed)
	assert.Equal(t, "Skipped in check mode", ended.Msg)
	require.NotNil(t, e.results["host1"]["shell_result"])
	assert.True(t, e.results["host1"]["shell_result"].Skipped)
}

// --- normalizeConditions ---

func TestExecutor_NormalizeConditions_Good_String(t *testing.T) {
	result := normalizeConditions("my_var is defined")
	assert.Equal(t, []string{"my_var is defined"}, result)
}

func TestExecutor_NormalizeConditions_Good_StringSlice(t *testing.T) {
	result := normalizeConditions([]string{"cond1", "cond2"})
	assert.Equal(t, []string{"cond1", "cond2"}, result)
}

func TestExecutor_NormalizeConditions_Good_AnySlice(t *testing.T) {
	result := normalizeConditions([]any{"cond1", "cond2"})
	assert.Equal(t, []string{"cond1", "cond2"}, result)
}

func TestExecutor_NormalizeConditions_Good_Nil(t *testing.T) {
	result := normalizeConditions(nil)
	assert.Nil(t, result)
}

// --- evaluateWhen ---

func TestExecutor_EvaluateWhen_Good_TrueLiteral(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.True(t, e.evaluateWhen("true", "host1", nil))
	assert.True(t, e.evaluateWhen("True", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_FalseLiteral(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.False(t, e.evaluateWhen("false", "host1", nil))
	assert.False(t, e.evaluateWhen("False", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_Negation(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.False(t, e.evaluateWhen("not true", "host1", nil))
	assert.True(t, e.evaluateWhen("not false", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_RegisteredVarDefined(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"myresult": {Changed: true, Failed: false},
	}

	assert.True(t, e.evaluateWhen("myresult is defined", "host1", nil))
	assert.False(t, e.evaluateWhen("myresult is not defined", "host1", nil))
	assert.False(t, e.evaluateWhen("nonexistent is defined", "host1", nil))
	assert.True(t, e.evaluateWhen("nonexistent is not defined", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_RegisteredVarStatus(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"success_result": {Changed: true, Failed: false},
		"failed_result":  {Failed: true},
		"skipped_result": {Skipped: true},
	}

	assert.True(t, e.evaluateWhen("success_result is success", "host1", nil))
	assert.True(t, e.evaluateWhen("success_result is succeeded", "host1", nil))
	assert.True(t, e.evaluateWhen("success_result is changed", "host1", nil))
	assert.True(t, e.evaluateWhen("failed_result is failed", "host1", nil))
	assert.True(t, e.evaluateWhen("skipped_result is skipped", "host1", nil))
}

func TestExecutor_EvaluateWhen_Good_VarTruthy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.vars["disabled"] = false
	e.vars["name"] = "hello"
	e.vars["empty"] = ""
	e.vars["count"] = 5
	e.vars["zero"] = 0

	assert.True(t, e.evalCondition("enabled", "host1"))
	assert.False(t, e.evalCondition("disabled", "host1"))
	assert.True(t, e.evalCondition("name", "host1"))
	assert.False(t, e.evalCondition("empty", "host1"))
	assert.True(t, e.evalCondition("count", "host1"))
	assert.False(t, e.evalCondition("zero", "host1"))
}

func TestExecutor_EvaluateWhen_Good_MultipleConditions(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	// All conditions must be true (AND)
	assert.True(t, e.evaluateWhen([]any{"true", "True"}, "host1", nil))
	assert.False(t, e.evaluateWhen([]any{"true", "false"}, "host1", nil))
}

func TestExecutor_ApplyTaskResultConditions_Good_ChangedWhen(t *testing.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		ChangedWhen: "stdout == 'expected'",
	}
	result := &TaskResult{
		Changed: true,
		Stdout:  "actual",
	}

	e.applyTaskResultConditions("host1", task, result)

	assert.False(t, result.Changed)
}

func TestExecutor_ApplyTaskResultConditions_Good_FailedWhen(t *testing.T) {
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

	assert.False(t, result.Failed)
}

func TestExecutor_ApplyTaskResultConditions_Good_DottedResultAccess(t *testing.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		ChangedWhen: "result.rc == 0",
	}
	result := &TaskResult{
		Changed: false,
		RC:      0,
	}

	e.applyTaskResultConditions("host1", task, result)

	assert.True(t, result.Changed)
}

// --- templateString ---

func TestExecutor_TemplateString_Good_SimpleVar(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["name"] = "world"

	result := e.templateString("hello {{ name }}", "", nil)
	assert.Equal(t, "hello world", result)
}

func TestExecutor_TemplateString_Good_MultVars(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["host"] = "example.com"
	e.vars["port"] = 8080

	result := e.templateString("http://{{ host }}:{{ port }}", "", nil)
	assert.Equal(t, "http://example.com:8080", result)
}

func TestExecutor_TemplateString_Good_Unresolved(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("{{ undefined_var }}", "", nil)
	assert.Equal(t, "{{ undefined_var }}", result)
}

func TestExecutor_TemplateString_Good_NoTemplate(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("plain string", "", nil)
	assert.Equal(t, "plain string", result)
}

// --- applyFilter ---

func TestExecutor_ApplyFilter_Good_Default(t *testing.T) {
	e := NewExecutor("/tmp")

	assert.Equal(t, "hello", e.applyFilter("hello", "default('fallback')"))
	assert.Equal(t, "fallback", e.applyFilter("", "default('fallback')"))
}

func TestExecutor_ApplyFilter_Good_Bool(t *testing.T) {
	e := NewExecutor("/tmp")

	assert.Equal(t, "true", e.applyFilter("true", "bool"))
	assert.Equal(t, "true", e.applyFilter("yes", "bool"))
	assert.Equal(t, "true", e.applyFilter("1", "bool"))
	assert.Equal(t, "false", e.applyFilter("false", "bool"))
	assert.Equal(t, "false", e.applyFilter("no", "bool"))
	assert.Equal(t, "false", e.applyFilter("anything", "bool"))
}

func TestExecutor_ApplyFilter_Good_Trim(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "hello", e.applyFilter("  hello  ", "trim"))
}

// --- resolveLoop ---

func TestExecutor_ResolveLoop_Good_SliceAny(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"a", "b", "c"}, "host1")
	assert.Len(t, items, 3)
}

func TestExecutor_ResolveLoop_Good_SliceString(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]string{"a", "b", "c"}, "host1")
	assert.Len(t, items, 3)
}

func TestExecutor_ResolveLoop_Good_Nil(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop(nil, "host1")
	assert.Nil(t, items)
}

// --- templateArgs ---

func TestExecutor_TemplateArgs_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["myvar"] = "resolved"

	args := map[string]any{
		"plain":     "no template",
		"templated": "{{ myvar }}",
		"number":    42,
	}

	result := e.templateArgs(args, "host1", nil)
	assert.Equal(t, "no template", result["plain"])
	assert.Equal(t, "resolved", result["templated"])
	assert.Equal(t, 42, result["number"])
}

func TestExecutor_TemplateArgs_Good_NestedMap(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["port"] = "8080"

	args := map[string]any{
		"nested": map[string]any{
			"port": "{{ port }}",
		},
	}

	result := e.templateArgs(args, "host1", nil)
	nested := result["nested"].(map[string]any)
	assert.Equal(t, "8080", nested["port"])
}

func TestExecutor_TemplateArgs_Good_ArrayValues(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["pkg"] = "nginx"

	args := map[string]any{
		"packages": []any{"{{ pkg }}", "curl"},
	}

	result := e.templateArgs(args, "host1", nil)
	pkgs := result["packages"].([]any)
	assert.Equal(t, "nginx", pkgs[0])
	assert.Equal(t, "curl", pkgs[1])
}

// --- Helper functions ---

func TestExecutor_GetStringArg_Good(t *testing.T) {
	args := map[string]any{
		"name":   "value",
		"number": 42,
	}

	assert.Equal(t, "value", getStringArg(args, "name", ""))
	assert.Equal(t, "42", getStringArg(args, "number", ""))
	assert.Equal(t, "default", getStringArg(args, "missing", "default"))
}

func TestExecutor_GetBoolArg_Good(t *testing.T) {
	args := map[string]any{
		"enabled":  true,
		"disabled": false,
		"yes_str":  "yes",
		"true_str": "true",
		"one_str":  "1",
		"no_str":   "no",
	}

	assert.True(t, getBoolArg(args, "enabled", false))
	assert.False(t, getBoolArg(args, "disabled", true))
	assert.True(t, getBoolArg(args, "yes_str", false))
	assert.True(t, getBoolArg(args, "true_str", false))
	assert.True(t, getBoolArg(args, "one_str", false))
	assert.False(t, getBoolArg(args, "no_str", true))
	assert.True(t, getBoolArg(args, "missing", true))
	assert.False(t, getBoolArg(args, "missing", false))
}

// --- Close ---

func TestExecutor_Close_Good_EmptyClients(t *testing.T) {
	e := NewExecutor("/tmp")
	// Should not panic with no clients
	e.Close()
	assert.Empty(t, e.clients)
}
