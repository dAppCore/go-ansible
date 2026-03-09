package ansible

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- NewExecutor ---

func TestNewExecutor_Good(t *testing.T) {
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

func TestSetVar_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetVar("foo", "bar")
	e.SetVar("count", 42)

	assert.Equal(t, "bar", e.vars["foo"])
	assert.Equal(t, 42, e.vars["count"])
}

// --- SetInventoryDirect ---

func TestSetInventoryDirect_Good(t *testing.T) {
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

func TestGetHosts_Executor_Good_WithInventory(t *testing.T) {
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

func TestGetHosts_Executor_Good_Localhost(t *testing.T) {
	e := NewExecutor("/tmp")
	// No inventory set

	hosts := e.getHosts("localhost")
	assert.Equal(t, []string{"localhost"}, hosts)
}

func TestGetHosts_Executor_Good_NoInventory(t *testing.T) {
	e := NewExecutor("/tmp")

	hosts := e.getHosts("webservers")
	assert.Nil(t, hosts)
}

func TestGetHosts_Executor_Good_WithLimit(t *testing.T) {
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

func TestMatchesTags_Good_NoTagsFilter(t *testing.T) {
	e := NewExecutor("/tmp")

	assert.True(t, e.matchesTags(nil))
	assert.True(t, e.matchesTags([]string{"any", "tags"}))
}

func TestMatchesTags_Good_IncludeTag(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	assert.True(t, e.matchesTags([]string{"deploy"}))
	assert.True(t, e.matchesTags([]string{"setup", "deploy"}))
	assert.False(t, e.matchesTags([]string{"other"}))
}

func TestMatchesTags_Good_SkipTag(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SkipTags = []string{"slow"}

	assert.True(t, e.matchesTags([]string{"fast"}))
	assert.False(t, e.matchesTags([]string{"slow"}))
	assert.False(t, e.matchesTags([]string{"fast", "slow"}))
}

func TestMatchesTags_Good_AllTag(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"all"}

	assert.True(t, e.matchesTags([]string{"anything"}))
}

func TestMatchesTags_Good_NoTaskTags(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Tasks with no tags should not match when include tags are set
	assert.False(t, e.matchesTags(nil))
	assert.False(t, e.matchesTags([]string{}))
}

// --- handleNotify ---

func TestHandleNotify_Good_String(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("restart nginx")

	assert.True(t, e.notified["restart nginx"])
}

func TestHandleNotify_Good_StringList(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]string{"restart nginx", "reload config"})

	assert.True(t, e.notified["restart nginx"])
	assert.True(t, e.notified["reload config"])
}

func TestHandleNotify_Good_AnyList(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]any{"restart nginx", "reload config"})

	assert.True(t, e.notified["restart nginx"])
	assert.True(t, e.notified["reload config"])
}

// --- normalizeConditions ---

func TestNormalizeConditions_Good_String(t *testing.T) {
	result := normalizeConditions("my_var is defined")
	assert.Equal(t, []string{"my_var is defined"}, result)
}

func TestNormalizeConditions_Good_StringSlice(t *testing.T) {
	result := normalizeConditions([]string{"cond1", "cond2"})
	assert.Equal(t, []string{"cond1", "cond2"}, result)
}

func TestNormalizeConditions_Good_AnySlice(t *testing.T) {
	result := normalizeConditions([]any{"cond1", "cond2"})
	assert.Equal(t, []string{"cond1", "cond2"}, result)
}

func TestNormalizeConditions_Good_Nil(t *testing.T) {
	result := normalizeConditions(nil)
	assert.Nil(t, result)
}

// --- evaluateWhen ---

func TestEvaluateWhen_Good_TrueLiteral(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.True(t, e.evaluateWhen("true", "host1", nil))
	assert.True(t, e.evaluateWhen("True", "host1", nil))
}

func TestEvaluateWhen_Good_FalseLiteral(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.False(t, e.evaluateWhen("false", "host1", nil))
	assert.False(t, e.evaluateWhen("False", "host1", nil))
}

func TestEvaluateWhen_Good_Negation(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.False(t, e.evaluateWhen("not true", "host1", nil))
	assert.True(t, e.evaluateWhen("not false", "host1", nil))
}

func TestEvaluateWhen_Good_RegisteredVarDefined(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"myresult": {Changed: true, Failed: false},
	}

	assert.True(t, e.evaluateWhen("myresult is defined", "host1", nil))
	assert.False(t, e.evaluateWhen("myresult is not defined", "host1", nil))
	assert.False(t, e.evaluateWhen("nonexistent is defined", "host1", nil))
	assert.True(t, e.evaluateWhen("nonexistent is not defined", "host1", nil))
}

func TestEvaluateWhen_Good_RegisteredVarStatus(t *testing.T) {
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

func TestEvaluateWhen_Good_VarTruthy(t *testing.T) {
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

func TestEvaluateWhen_Good_MultipleConditions(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	// All conditions must be true (AND)
	assert.True(t, e.evaluateWhen([]any{"true", "True"}, "host1", nil))
	assert.False(t, e.evaluateWhen([]any{"true", "false"}, "host1", nil))
}

// --- templateString ---

func TestTemplateString_Good_SimpleVar(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["name"] = "world"

	result := e.templateString("hello {{ name }}", "", nil)
	assert.Equal(t, "hello world", result)
}

func TestTemplateString_Good_MultVars(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["host"] = "example.com"
	e.vars["port"] = 8080

	result := e.templateString("http://{{ host }}:{{ port }}", "", nil)
	assert.Equal(t, "http://example.com:8080", result)
}

func TestTemplateString_Good_Unresolved(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("{{ undefined_var }}", "", nil)
	assert.Equal(t, "{{ undefined_var }}", result)
}

func TestTemplateString_Good_NoTemplate(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("plain string", "", nil)
	assert.Equal(t, "plain string", result)
}

// --- applyFilter ---

func TestApplyFilter_Good_Default(t *testing.T) {
	e := NewExecutor("/tmp")

	assert.Equal(t, "hello", e.applyFilter("hello", "default('fallback')"))
	assert.Equal(t, "fallback", e.applyFilter("", "default('fallback')"))
}

func TestApplyFilter_Good_Bool(t *testing.T) {
	e := NewExecutor("/tmp")

	assert.Equal(t, "true", e.applyFilter("true", "bool"))
	assert.Equal(t, "true", e.applyFilter("yes", "bool"))
	assert.Equal(t, "true", e.applyFilter("1", "bool"))
	assert.Equal(t, "false", e.applyFilter("false", "bool"))
	assert.Equal(t, "false", e.applyFilter("no", "bool"))
	assert.Equal(t, "false", e.applyFilter("anything", "bool"))
}

func TestApplyFilter_Good_Trim(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "hello", e.applyFilter("  hello  ", "trim"))
}

// --- resolveLoop ---

func TestResolveLoop_Good_SliceAny(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"a", "b", "c"}, "host1")
	assert.Len(t, items, 3)
}

func TestResolveLoop_Good_SliceString(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]string{"a", "b", "c"}, "host1")
	assert.Len(t, items, 3)
}

func TestResolveLoop_Good_Nil(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop(nil, "host1")
	assert.Nil(t, items)
}

// --- templateArgs ---

func TestTemplateArgs_Good(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["myvar"] = "resolved"

	args := map[string]any{
		"plain":    "no template",
		"templated": "{{ myvar }}",
		"number":   42,
	}

	result := e.templateArgs(args, "host1", nil)
	assert.Equal(t, "no template", result["plain"])
	assert.Equal(t, "resolved", result["templated"])
	assert.Equal(t, 42, result["number"])
}

func TestTemplateArgs_Good_NestedMap(t *testing.T) {
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

func TestTemplateArgs_Good_ArrayValues(t *testing.T) {
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

func TestGetStringArg_Good(t *testing.T) {
	args := map[string]any{
		"name":   "value",
		"number": 42,
	}

	assert.Equal(t, "value", getStringArg(args, "name", ""))
	assert.Equal(t, "42", getStringArg(args, "number", ""))
	assert.Equal(t, "default", getStringArg(args, "missing", "default"))
}

func TestGetBoolArg_Good(t *testing.T) {
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

func TestClose_Good_EmptyClients(t *testing.T) {
	e := NewExecutor("/tmp")
	// Should not panic with no clients
	e.Close()
	assert.Empty(t, e.clients)
}
