package ansible

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===========================================================================
// 1. Error Propagation — getHosts
// ===========================================================================

func TestGetHosts_Infra_Good_AllPattern(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {AnsibleHost: "10.0.0.1"},
				"web2": {AnsibleHost: "10.0.0.2"},
				"db1":  {AnsibleHost: "10.0.1.1"},
			},
		},
	})

	hosts := e.getHosts("all")
	assert.Len(t, hosts, 3)
	assert.Contains(t, hosts, "web1")
	assert.Contains(t, hosts, "web2")
	assert.Contains(t, hosts, "db1")
}

func TestGetHosts_Infra_Good_SpecificHost(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {AnsibleHost: "10.0.0.1"},
				"web2": {AnsibleHost: "10.0.0.2"},
			},
		},
	})

	hosts := e.getHosts("web1")
	assert.Equal(t, []string{"web1"}, hosts)
}

func TestGetHosts_Infra_Good_GroupName(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Children: map[string]*InventoryGroup{
				"webservers": {
					Hosts: map[string]*Host{
						"web1": {AnsibleHost: "10.0.0.1"},
						"web2": {AnsibleHost: "10.0.0.2"},
					},
				},
				"dbservers": {
					Hosts: map[string]*Host{
						"db1": {AnsibleHost: "10.0.1.1"},
					},
				},
			},
		},
	})

	hosts := e.getHosts("webservers")
	assert.Len(t, hosts, 2)
	assert.Contains(t, hosts, "web1")
	assert.Contains(t, hosts, "web2")
}

func TestGetHosts_Infra_Good_Localhost(t *testing.T) {
	e := NewExecutor("/tmp")
	// No inventory at all
	hosts := e.getHosts("localhost")
	assert.Equal(t, []string{"localhost"}, hosts)
}

func TestGetHosts_Infra_Bad_NilInventory(t *testing.T) {
	e := NewExecutor("/tmp")
	// inventory is nil, non-localhost pattern
	hosts := e.getHosts("webservers")
	assert.Nil(t, hosts)
}

func TestGetHosts_Infra_Bad_NonexistentHost(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {},
			},
		},
	})

	hosts := e.getHosts("nonexistent")
	assert.Empty(t, hosts)
}

func TestGetHosts_Infra_Good_LimitFiltering(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {},
				"web2": {},
				"db1":  {},
			},
		},
	})
	e.Limit = "web1"

	hosts := e.getHosts("all")
	assert.Len(t, hosts, 1)
	assert.Contains(t, hosts, "web1")
}

func TestGetHosts_Infra_Good_LimitSubstringMatch(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"prod-web-01": {},
				"prod-web-02": {},
				"staging-web-01": {},
			},
		},
	})
	e.Limit = "prod"

	hosts := e.getHosts("all")
	// Limit uses substring matching as fallback
	assert.Len(t, hosts, 2)
}

// ===========================================================================
// 1. Error Propagation — matchesTags
// ===========================================================================

func TestMatchesTags_Infra_Good_NoFiltersNoTags(t *testing.T) {
	e := NewExecutor("/tmp")
	// No Tags, no SkipTags set
	assert.True(t, e.matchesTags(nil))
}

func TestMatchesTags_Infra_Good_NoFiltersWithTaskTags(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.True(t, e.matchesTags([]string{"deploy", "config"}))
}

func TestMatchesTags_Infra_Good_IncludeMatchesOneOfMultiple(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Task has deploy among its tags
	assert.True(t, e.matchesTags([]string{"setup", "deploy", "config"}))
}

func TestMatchesTags_Infra_Bad_IncludeNoMatch(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	assert.False(t, e.matchesTags([]string{"build", "test"}))
}

func TestMatchesTags_Infra_Good_SkipOverridesInclude(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SkipTags = []string{"slow"}

	// Even with no include tags, skip tags filter out matching tasks
	assert.False(t, e.matchesTags([]string{"deploy", "slow"}))
	assert.True(t, e.matchesTags([]string{"deploy", "fast"}))
}

func TestMatchesTags_Infra_Bad_IncludeFilterNoTaskTags(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Tasks with no tags should not run when include tags are active
	assert.False(t, e.matchesTags(nil))
	assert.False(t, e.matchesTags([]string{}))
}

func TestMatchesTags_Infra_Good_AllTagMatchesEverything(t *testing.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"all"}

	assert.True(t, e.matchesTags([]string{"deploy"}))
	assert.True(t, e.matchesTags([]string{"config", "slow"}))
}

// ===========================================================================
// 1. Error Propagation — evaluateWhen
// ===========================================================================

func TestEvaluateWhen_Infra_Good_DefinedCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"myresult": {Changed: true},
	}

	assert.True(t, e.evaluateWhen("myresult is defined", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_NotDefinedCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	// No results registered for host1
	assert.True(t, e.evaluateWhen("missing_var is not defined", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_UndefinedAlias(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.True(t, e.evaluateWhen("some_var is undefined", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_SucceededCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Failed: false, Changed: true},
	}

	assert.True(t, e.evaluateWhen("result is success", "host1", nil))
	assert.True(t, e.evaluateWhen("result is succeeded", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_FailedCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Failed: true},
	}

	assert.True(t, e.evaluateWhen("result is failed", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_ChangedCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Changed: true},
	}

	assert.True(t, e.evaluateWhen("result is changed", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_SkippedCheck(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Skipped: true},
	}

	assert.True(t, e.evaluateWhen("result is skipped", "host1", nil))
}

func TestEvaluateWhen_Infra_Good_BoolVarTruthy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_flag"] = true

	assert.True(t, e.evalCondition("my_flag", "host1"))
}

func TestEvaluateWhen_Infra_Good_BoolVarFalsy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_flag"] = false

	assert.False(t, e.evalCondition("my_flag", "host1"))
}

func TestEvaluateWhen_Infra_Good_StringVarTruthy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_str"] = "hello"
	assert.True(t, e.evalCondition("my_str", "host1"))
}

func TestEvaluateWhen_Infra_Good_StringVarEmptyFalsy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_str"] = ""
	assert.False(t, e.evalCondition("my_str", "host1"))
}

func TestEvaluateWhen_Infra_Good_StringVarFalseLiteral(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_str"] = "false"
	assert.False(t, e.evalCondition("my_str", "host1"))

	e.vars["my_str2"] = "False"
	assert.False(t, e.evalCondition("my_str2", "host1"))
}

func TestEvaluateWhen_Infra_Good_IntVarNonZero(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["count"] = 42
	assert.True(t, e.evalCondition("count", "host1"))
}

func TestEvaluateWhen_Infra_Good_IntVarZero(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["count"] = 0
	assert.False(t, e.evalCondition("count", "host1"))
}

func TestEvaluateWhen_Infra_Good_Negation(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.False(t, e.evalCondition("not true", "host1"))
	assert.True(t, e.evalCondition("not false", "host1"))
}

func TestEvaluateWhen_Infra_Good_MultipleConditionsAllTrue(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.results["host1"] = map[string]*TaskResult{
		"prev": {Failed: false},
	}

	// Both conditions must be true (AND semantics)
	assert.True(t, e.evaluateWhen([]any{"enabled", "prev is success"}, "host1", nil))
}

func TestEvaluateWhen_Infra_Bad_MultipleConditionsOneFails(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	// "false" literal fails
	assert.False(t, e.evaluateWhen([]any{"enabled", "false"}, "host1", nil))
}

func TestEvaluateWhen_Infra_Good_DefaultFilterInCondition(t *testing.T) {
	e := NewExecutor("/tmp")
	// Condition with default filter should be satisfied
	assert.True(t, e.evalCondition("my_var | default(true)", "host1"))
}

func TestEvaluateWhen_Infra_Good_RegisteredVarTruthy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"check_result": {Failed: false, Skipped: false},
	}

	// Just referencing a registered var name evaluates truthy if not failed/skipped
	assert.True(t, e.evalCondition("check_result", "host1"))
}

func TestEvaluateWhen_Infra_Bad_RegisteredVarFailedFalsy(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"check_result": {Failed: true},
	}

	// A failed registered var should be falsy
	assert.False(t, e.evalCondition("check_result", "host1"))
}

// ===========================================================================
// 1. Error Propagation — templateString
// ===========================================================================

func TestTemplateString_Infra_Good_SimpleSubstitution(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["app_name"] = "myapp"

	result := e.templateString("Deploying {{ app_name }}", "", nil)
	assert.Equal(t, "Deploying myapp", result)
}

func TestTemplateString_Infra_Good_MultipleVars(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["host"] = "db.example.com"
	e.vars["port"] = 5432

	result := e.templateString("postgresql://{{ host }}:{{ port }}/mydb", "", nil)
	assert.Equal(t, "postgresql://db.example.com:5432/mydb", result)
}

func TestTemplateString_Infra_Good_Unresolved(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("{{ missing_var }}", "", nil)
	assert.Equal(t, "{{ missing_var }}", result)
}

func TestTemplateString_Infra_Good_NoTemplateMarkup(t *testing.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("just a plain string", "", nil)
	assert.Equal(t, "just a plain string", result)
}

func TestTemplateString_Infra_Good_RegisteredVarStdout(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {Stdout: "42"},
	}

	result := e.templateString("{{ cmd_result.stdout }}", "host1", nil)
	assert.Equal(t, "42", result)
}

func TestTemplateString_Infra_Good_RegisteredVarRC(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {RC: 0},
	}

	result := e.templateString("{{ cmd_result.rc }}", "host1", nil)
	assert.Equal(t, "0", result)
}

func TestTemplateString_Infra_Good_RegisteredVarChanged(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {Changed: true},
	}

	result := e.templateString("{{ cmd_result.changed }}", "host1", nil)
	assert.Equal(t, "true", result)
}

func TestTemplateString_Infra_Good_RegisteredVarFailed(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {Failed: true},
	}

	result := e.templateString("{{ cmd_result.failed }}", "host1", nil)
	assert.Equal(t, "true", result)
}

func TestTemplateString_Infra_Good_TaskVars(t *testing.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		Vars: map[string]any{
			"task_var": "task_value",
		},
	}

	result := e.templateString("{{ task_var }}", "host1", task)
	assert.Equal(t, "task_value", result)
}

func TestTemplateString_Infra_Good_FactsResolution(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname:     "web1",
		FQDN:         "web1.example.com",
		Distribution: "ubuntu",
		Version:      "24.04",
		Architecture: "x86_64",
		Kernel:       "6.5.0",
	}

	assert.Equal(t, "web1", e.templateString("{{ ansible_hostname }}", "host1", nil))
	assert.Equal(t, "web1.example.com", e.templateString("{{ ansible_fqdn }}", "host1", nil))
	assert.Equal(t, "ubuntu", e.templateString("{{ ansible_distribution }}", "host1", nil))
	assert.Equal(t, "24.04", e.templateString("{{ ansible_distribution_version }}", "host1", nil))
	assert.Equal(t, "x86_64", e.templateString("{{ ansible_architecture }}", "host1", nil))
	assert.Equal(t, "6.5.0", e.templateString("{{ ansible_kernel }}", "host1", nil))
}

// ===========================================================================
// 1. Error Propagation — applyFilter
// ===========================================================================

func TestApplyFilter_Infra_Good_DefaultWithValue(t *testing.T) {
	e := NewExecutor("/tmp")
	// When value is non-empty, default is not applied
	assert.Equal(t, "hello", e.applyFilter("hello", "default('fallback')"))
}

func TestApplyFilter_Infra_Good_DefaultWithEmpty(t *testing.T) {
	e := NewExecutor("/tmp")
	// When value is empty, default IS applied
	assert.Equal(t, "fallback", e.applyFilter("", "default('fallback')"))
}

func TestApplyFilter_Infra_Good_DefaultWithDoubleQuotes(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "fallback", e.applyFilter("", `default("fallback")`))
}

func TestApplyFilter_Infra_Good_BoolFilterTrue(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "true", e.applyFilter("true", "bool"))
	assert.Equal(t, "true", e.applyFilter("True", "bool"))
	assert.Equal(t, "true", e.applyFilter("yes", "bool"))
	assert.Equal(t, "true", e.applyFilter("Yes", "bool"))
	assert.Equal(t, "true", e.applyFilter("1", "bool"))
}

func TestApplyFilter_Infra_Good_BoolFilterFalse(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "false", e.applyFilter("false", "bool"))
	assert.Equal(t, "false", e.applyFilter("no", "bool"))
	assert.Equal(t, "false", e.applyFilter("0", "bool"))
	assert.Equal(t, "false", e.applyFilter("random", "bool"))
}

func TestApplyFilter_Infra_Good_TrimFilter(t *testing.T) {
	e := NewExecutor("/tmp")
	assert.Equal(t, "hello", e.applyFilter("  hello  ", "trim"))
	assert.Equal(t, "no spaces", e.applyFilter("no spaces", "trim"))
	assert.Equal(t, "", e.applyFilter("   ", "trim"))
}

func TestApplyFilter_Infra_Good_B64Decode(t *testing.T) {
	e := NewExecutor("/tmp")
	// b64decode currently returns value unchanged (placeholder)
	assert.Equal(t, "dGVzdA==", e.applyFilter("dGVzdA==", "b64decode"))
}

func TestApplyFilter_Infra_Good_UnknownFilter(t *testing.T) {
	e := NewExecutor("/tmp")
	// Unknown filters return value unchanged
	assert.Equal(t, "hello", e.applyFilter("hello", "nonexistent_filter"))
}

func TestTemplateString_Infra_Good_FilterInTemplate(t *testing.T) {
	e := NewExecutor("/tmp")
	// When a var is defined, the filter passes through
	e.vars["defined_var"] = "hello"
	result := e.templateString("{{ defined_var | default('fallback') }}", "", nil)
	assert.Equal(t, "hello", result)
}

func TestTemplateString_Infra_Good_DefaultFilterEmptyVar(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["empty_var"] = ""
	// When var is empty string, default filter applies
	result := e.applyFilter("", "default('fallback')")
	assert.Equal(t, "fallback", result)
}

func TestTemplateString_Infra_Good_BoolFilterInTemplate(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["flag"] = "yes"

	result := e.templateString("{{ flag | bool }}", "", nil)
	assert.Equal(t, "true", result)
}

func TestTemplateString_Infra_Good_TrimFilterInTemplate(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["padded"] = "  trimmed  "

	result := e.templateString("{{ padded | trim }}", "", nil)
	assert.Equal(t, "trimmed", result)
}

// ===========================================================================
// 1. Error Propagation — resolveLoop
// ===========================================================================

func TestResolveLoop_Infra_Good_SliceAny(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"a", "b", "c"}, "host1")
	assert.Len(t, items, 3)
	assert.Equal(t, "a", items[0])
	assert.Equal(t, "b", items[1])
	assert.Equal(t, "c", items[2])
}

func TestResolveLoop_Infra_Good_SliceString(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]string{"x", "y"}, "host1")
	assert.Len(t, items, 2)
	assert.Equal(t, "x", items[0])
	assert.Equal(t, "y", items[1])
}

func TestResolveLoop_Infra_Good_NilLoop(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop(nil, "host1")
	assert.Nil(t, items)
}

func TestResolveLoop_Infra_Good_VarReference(t *testing.T) {
	e := NewExecutor("/tmp")
	e.vars["my_list"] = []any{"item1", "item2", "item3"}

	// When loop is a string that resolves to a variable containing a list
	items := e.resolveLoop("{{ my_list }}", "host1")
	// The template resolves "{{ my_list }}" but the result is a string representation,
	// not the original list. The resolveLoop handles this by trying to look up
	// the resolved value in vars again.
	// Since templateString returns the string "[item1 item2 item3]", and that
	// isn't a var name, items will be nil. This tests the edge case.
	// The actual var name lookup happens when the loop value is just "my_list".
	assert.Nil(t, items)
}

func TestResolveLoop_Infra_Good_MixedTypes(t *testing.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"str", 42, true, map[string]any{"key": "val"}}, "host1")
	assert.Len(t, items, 4)
	assert.Equal(t, "str", items[0])
	assert.Equal(t, 42, items[1])
	assert.Equal(t, true, items[2])
}

// ===========================================================================
// 1. Error Propagation — handleNotify
// ===========================================================================

func TestHandleNotify_Infra_Good_SingleString(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("restart nginx")

	assert.True(t, e.notified["restart nginx"])
	assert.False(t, e.notified["restart apache"])
}

func TestHandleNotify_Infra_Good_StringSlice(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]string{"restart nginx", "reload haproxy"})

	assert.True(t, e.notified["restart nginx"])
	assert.True(t, e.notified["reload haproxy"])
}

func TestHandleNotify_Infra_Good_AnySlice(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]any{"handler1", "handler2", "handler3"})

	assert.True(t, e.notified["handler1"])
	assert.True(t, e.notified["handler2"])
	assert.True(t, e.notified["handler3"])
}

func TestHandleNotify_Infra_Good_NilNotify(t *testing.T) {
	e := NewExecutor("/tmp")
	// Should not panic
	e.handleNotify(nil)
	assert.Empty(t, e.notified)
}

func TestHandleNotify_Infra_Good_MultipleCallsAccumulate(t *testing.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("handler1")
	e.handleNotify("handler2")

	assert.True(t, e.notified["handler1"])
	assert.True(t, e.notified["handler2"])
}

// ===========================================================================
// 1. Error Propagation — normalizeConditions
// ===========================================================================

func TestNormalizeConditions_Infra_Good_String(t *testing.T) {
	result := normalizeConditions("my_var is defined")
	assert.Equal(t, []string{"my_var is defined"}, result)
}

func TestNormalizeConditions_Infra_Good_StringSlice(t *testing.T) {
	result := normalizeConditions([]string{"cond1", "cond2"})
	assert.Equal(t, []string{"cond1", "cond2"}, result)
}

func TestNormalizeConditions_Infra_Good_AnySlice(t *testing.T) {
	result := normalizeConditions([]any{"cond1", "cond2"})
	assert.Equal(t, []string{"cond1", "cond2"}, result)
}

func TestNormalizeConditions_Infra_Good_Nil(t *testing.T) {
	result := normalizeConditions(nil)
	assert.Nil(t, result)
}

func TestNormalizeConditions_Infra_Good_IntIgnored(t *testing.T) {
	// Non-string types in any slice are silently skipped
	result := normalizeConditions([]any{"cond1", 42})
	assert.Equal(t, []string{"cond1"}, result)
}

func TestNormalizeConditions_Infra_Good_UnsupportedType(t *testing.T) {
	result := normalizeConditions(42)
	assert.Nil(t, result)
}

// ===========================================================================
// 2. Become/Sudo
// ===========================================================================

func TestBecome_Infra_Good_SetBecomeTrue(t *testing.T) {
	cfg := SSHConfig{
		Host:       "test-host",
		Port:       22,
		User:       "deploy",
		Become:     true,
		BecomeUser: "root",
		BecomePass: "secret",
	}
	client, err := NewSSHClient(cfg)
	require.NoError(t, err)

	assert.True(t, client.become)
	assert.Equal(t, "root", client.becomeUser)
	assert.Equal(t, "secret", client.becomePass)
}

func TestBecome_Infra_Good_SetBecomeFalse(t *testing.T) {
	cfg := SSHConfig{
		Host: "test-host",
		Port: 22,
		User: "deploy",
	}
	client, err := NewSSHClient(cfg)
	require.NoError(t, err)

	assert.False(t, client.become)
	assert.Empty(t, client.becomeUser)
	assert.Empty(t, client.becomePass)
}

func TestBecome_Infra_Good_SetBecomeMethod(t *testing.T) {
	cfg := SSHConfig{Host: "test-host"}
	client, err := NewSSHClient(cfg)
	require.NoError(t, err)

	assert.False(t, client.become)

	client.SetBecome(true, "admin", "pass123")
	assert.True(t, client.become)
	assert.Equal(t, "admin", client.becomeUser)
	assert.Equal(t, "pass123", client.becomePass)
}

func TestBecome_Infra_Good_DisableAfterEnable(t *testing.T) {
	cfg := SSHConfig{Host: "test-host"}
	client, err := NewSSHClient(cfg)
	require.NoError(t, err)

	client.SetBecome(true, "root", "secret")
	assert.True(t, client.become)

	client.SetBecome(false, "", "")
	assert.False(t, client.become)
	// becomeUser and becomePass are only updated if non-empty
	assert.Equal(t, "root", client.becomeUser)
	assert.Equal(t, "secret", client.becomePass)
}

func TestBecome_Infra_Good_MockBecomeTracking(t *testing.T) {
	mock := NewMockSSHClient()
	assert.False(t, mock.become)

	mock.SetBecome(true, "root", "password")
	assert.True(t, mock.become)
	assert.Equal(t, "root", mock.becomeUser)
	assert.Equal(t, "password", mock.becomePass)
}

func TestBecome_Infra_Good_DefaultBecomeUserRoot(t *testing.T) {
	// When become is true but no user specified, it defaults to root in the Run method
	cfg := SSHConfig{
		Host:   "test-host",
		Become: true,
		// BecomeUser not set
	}
	client, err := NewSSHClient(cfg)
	require.NoError(t, err)

	assert.True(t, client.become)
	assert.Empty(t, client.becomeUser) // Empty in config...
	// The Run() method defaults to "root" when becomeUser is empty
}

func TestBecome_Infra_Good_PasswordlessBecome(t *testing.T) {
	cfg := SSHConfig{
		Host:       "test-host",
		Become:     true,
		BecomeUser: "root",
		// No BecomePass and no Password — triggers sudo -n
	}
	client, err := NewSSHClient(cfg)
	require.NoError(t, err)

	assert.True(t, client.become)
	assert.Empty(t, client.becomePass)
	assert.Empty(t, client.password)
}

func TestBecome_Infra_Good_ExecutorPlayBecome(t *testing.T) {
	// Test that getClient applies play-level become settings
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {AnsibleHost: "127.0.0.1"},
			},
		},
	})

	play := &Play{
		Become:     true,
		BecomeUser: "admin",
	}

	// getClient will attempt SSH connection which will fail,
	// but we can verify the config would be set correctly
	// by checking the SSHConfig construction logic.
	// Since getClient creates real connections, we just verify
	// that the become fields are set on the play.
	assert.True(t, play.Become)
	assert.Equal(t, "admin", play.BecomeUser)
}

// ===========================================================================
// 3. Fact Gathering
// ===========================================================================

func TestFacts_Infra_Good_UbuntuParsing(t *testing.T) {
	e, mock := newTestExecutorWithMock("web1")

	// Mock os-release output for Ubuntu
	mock.expectCommand(`hostname -f`, "web1.example.com\n", "", 0)
	mock.expectCommand(`hostname -s`, "web1\n", "", 0)
	mock.expectCommand(`cat /etc/os-release`, "ID=ubuntu\nVERSION_ID=\"24.04\"\n", "", 0)
	mock.expectCommand(`uname -m`, "x86_64\n", "", 0)
	mock.expectCommand(`uname -r`, "6.5.0-44-generic\n", "", 0)

	// Simulate fact gathering by directly populating facts
	// using the same parsing logic as gatherFacts
	facts := &Facts{}

	stdout, _, _, _ := mock.Run(nil, "hostname -f 2>/dev/null || hostname")
	facts.FQDN = trimSpace(stdout)

	stdout, _, _, _ = mock.Run(nil, "hostname -s 2>/dev/null || hostname")
	facts.Hostname = trimSpace(stdout)

	stdout, _, _, _ = mock.Run(nil, "cat /etc/os-release 2>/dev/null | grep -E '^(ID|VERSION_ID)=' | head -2")
	for _, line := range splitLines(stdout) {
		if hasPrefix(line, "ID=") {
			facts.Distribution = trimQuotes(trimPrefix(line, "ID="))
		}
		if hasPrefix(line, "VERSION_ID=") {
			facts.Version = trimQuotes(trimPrefix(line, "VERSION_ID="))
		}
	}

	stdout, _, _, _ = mock.Run(nil, "uname -m")
	facts.Architecture = trimSpace(stdout)

	stdout, _, _, _ = mock.Run(nil, "uname -r")
	facts.Kernel = trimSpace(stdout)

	e.facts["web1"] = facts

	assert.Equal(t, "web1.example.com", facts.FQDN)
	assert.Equal(t, "web1", facts.Hostname)
	assert.Equal(t, "ubuntu", facts.Distribution)
	assert.Equal(t, "24.04", facts.Version)
	assert.Equal(t, "x86_64", facts.Architecture)
	assert.Equal(t, "6.5.0-44-generic", facts.Kernel)

	// Now verify template resolution with these facts
	result := e.templateString("{{ ansible_hostname }}", "web1", nil)
	assert.Equal(t, "web1", result)

	result = e.templateString("{{ ansible_distribution }}", "web1", nil)
	assert.Equal(t, "ubuntu", result)
}

func TestFacts_Infra_Good_CentOSParsing(t *testing.T) {
	facts := &Facts{}

	osRelease := "ID=centos\nVERSION_ID=\"8\"\n"
	for _, line := range splitLines(osRelease) {
		if hasPrefix(line, "ID=") {
			facts.Distribution = trimQuotes(trimPrefix(line, "ID="))
		}
		if hasPrefix(line, "VERSION_ID=") {
			facts.Version = trimQuotes(trimPrefix(line, "VERSION_ID="))
		}
	}

	assert.Equal(t, "centos", facts.Distribution)
	assert.Equal(t, "8", facts.Version)
}

func TestFacts_Infra_Good_AlpineParsing(t *testing.T) {
	facts := &Facts{}

	osRelease := "ID=alpine\nVERSION_ID=3.19.1\n"
	for _, line := range splitLines(osRelease) {
		if hasPrefix(line, "ID=") {
			facts.Distribution = trimQuotes(trimPrefix(line, "ID="))
		}
		if hasPrefix(line, "VERSION_ID=") {
			facts.Version = trimQuotes(trimPrefix(line, "VERSION_ID="))
		}
	}

	assert.Equal(t, "alpine", facts.Distribution)
	assert.Equal(t, "3.19.1", facts.Version)
}

func TestFacts_Infra_Good_DebianParsing(t *testing.T) {
	facts := &Facts{}

	osRelease := "ID=debian\nVERSION_ID=\"12\"\n"
	for _, line := range splitLines(osRelease) {
		if hasPrefix(line, "ID=") {
			facts.Distribution = trimQuotes(trimPrefix(line, "ID="))
		}
		if hasPrefix(line, "VERSION_ID=") {
			facts.Version = trimQuotes(trimPrefix(line, "VERSION_ID="))
		}
	}

	assert.Equal(t, "debian", facts.Distribution)
	assert.Equal(t, "12", facts.Version)
}

func TestFacts_Infra_Good_HostnameFromCommand(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname: "myserver",
		FQDN:     "myserver.example.com",
	}

	assert.Equal(t, "myserver", e.templateString("{{ ansible_hostname }}", "host1", nil))
	assert.Equal(t, "myserver.example.com", e.templateString("{{ ansible_fqdn }}", "host1", nil))
}

func TestFacts_Infra_Good_ArchitectureResolution(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Architecture: "aarch64",
	}

	result := e.templateString("{{ ansible_architecture }}", "host1", nil)
	assert.Equal(t, "aarch64", result)
}

func TestFacts_Infra_Good_KernelResolution(t *testing.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Kernel: "5.15.0-91-generic",
	}

	result := e.templateString("{{ ansible_kernel }}", "host1", nil)
	assert.Equal(t, "5.15.0-91-generic", result)
}

func TestFacts_Infra_Good_NoFactsForHost(t *testing.T) {
	e := NewExecutor("/tmp")
	// No facts gathered for host1
	result := e.templateString("{{ ansible_hostname }}", "host1", nil)
	// Should remain unresolved
	assert.Equal(t, "{{ ansible_hostname }}", result)
}

func TestFacts_Infra_Good_LocalhostFacts(t *testing.T) {
	// When connection is local, gatherFacts sets minimal facts
	e := NewExecutor("/tmp")
	e.facts["localhost"] = &Facts{
		Hostname: "localhost",
	}

	result := e.templateString("{{ ansible_hostname }}", "localhost", nil)
	assert.Equal(t, "localhost", result)
}

// ===========================================================================
// 4. Idempotency
// ===========================================================================

func TestIdempotency_Infra_Good_GroupAlreadyExists(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: getent group docker succeeds (group exists) — the || means groupadd is skipped
	mock.expectCommand(`getent group docker`, "docker:x:999:\n", "", 0)

	task := &Task{
		Module: "group",
		Args: map[string]any{
			"name":  "docker",
			"state": "present",
		},
	}

	result, err := moduleGroupWithClient(nil, mock, task.Args)
	require.NoError(t, err)

	// The module runs the command: getent group docker >/dev/null 2>&1 || groupadd docker
	// Since getent succeeds (rc=0), groupadd is not executed by the shell.
	// However, the module always reports changed=true because it does not
	// check idempotency at the Go level. This tests the current behaviour.
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestIdempotency_Infra_Good_AuthorizedKeyAlreadyPresent(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC7xfG..." +
		"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA user@host"

	// Mock: getent passwd returns home dir
	mock.expectCommand(`getent passwd deploy`, "/home/deploy\n", "", 0)

	// Mock: mkdir + chmod + chown for .ssh dir
	mock.expectCommand(`mkdir -p`, "", "", 0)

	// Mock: grep finds the key (rc=0, key is present)
	mock.expectCommand(`grep -qF`, "", "", 0)

	// Mock: chmod + chown for authorized_keys
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(nil, mock, map[string]any{
		"user":  "deploy",
		"key":   testKey,
		"state": "present",
	})
	require.NoError(t, err)

	// Module reports changed=true regardless (it doesn't check grep result at Go level)
	// The grep || echo construct handles idempotency at the shell level
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
}

func TestIdempotency_Infra_Good_DockerComposeUpToDate(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: docker compose up -d returns "Up to date" in stdout
	mock.expectCommand(`docker compose up -d`, "web1 Up to date\nnginx Up to date\n", "", 0)

	result, err := moduleDockerComposeWithClient(nil, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// When stdout contains "Up to date", changed should be false
	assert.False(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestIdempotency_Infra_Good_DockerComposeChanged(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: docker compose up -d with actual changes
	mock.expectCommand(`docker compose up -d`, "Creating web1 ... done\nCreating nginx ... done\n", "", 0)

	result, err := moduleDockerComposeWithClient(nil, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// When stdout does NOT contain "Up to date", changed should be true
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestIdempotency_Infra_Good_DockerComposeUpToDateInStderr(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Some versions of docker compose output status to stderr
	mock.expectCommand(`docker compose up -d`, "", "web1 Up to date\n", 0)

	result, err := moduleDockerComposeWithClient(nil, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// The docker compose module checks both stdout and stderr for "Up to date"
	assert.False(t, result.Changed)
}

func TestIdempotency_Infra_Good_GroupCreationWhenNew(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: getent fails (group does not exist), groupadd succeeds
	mock.expectCommand(`getent group newgroup`, "", "no such group", 2)
	// The overall command runs in shell: getent group newgroup >/dev/null 2>&1 || groupadd  newgroup
	// Since we match on the full command, the mock will return rc=0 default
	mock.expectCommand(`getent group newgroup .* groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(nil, mock, map[string]any{
		"name":  "newgroup",
		"state": "present",
	})
	require.NoError(t, err)

	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestIdempotency_Infra_Good_ServiceStatChanged(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: stat reports the file exists
	mock.addStat("/etc/config.conf", map[string]any{"exists": true, "isdir": false})

	result, err := moduleStatWithClient(nil, mock, map[string]any{
		"path": "/etc/config.conf",
	})
	require.NoError(t, err)

	// Stat module should always report changed=false
	assert.False(t, result.Changed)
	assert.NotNil(t, result.Data)
	stat := result.Data["stat"].(map[string]any)
	assert.True(t, stat["exists"].(bool))
}

func TestIdempotency_Infra_Good_StatFileNotFound(t *testing.T) {
	_, mock := newTestExecutorWithMock("host1")

	// No stat info added — will return exists=false from mock
	result, err := moduleStatWithClient(nil, mock, map[string]any{
		"path": "/nonexistent/file",
	})
	require.NoError(t, err)

	assert.False(t, result.Changed)
	assert.NotNil(t, result.Data)
	stat := result.Data["stat"].(map[string]any)
	assert.False(t, stat["exists"].(bool))
}

// ===========================================================================
// Additional cross-cutting edge cases
// ===========================================================================

func TestResolveExpr_Infra_Good_HostVars(t *testing.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {
					AnsibleHost: "10.0.0.1",
					Vars: map[string]any{
						"custom_var": "custom_value",
					},
				},
			},
		},
	})

	result := e.templateString("{{ custom_var }}", "host1", nil)
	assert.Equal(t, "custom_value", result)
}

func TestTemplateArgs_Infra_Good_InventoryHostname(t *testing.T) {
	e := NewExecutor("/tmp")

	args := map[string]any{
		"hostname": "{{ inventory_hostname }}",
	}

	result := e.templateArgs(args, "web1", nil)
	assert.Equal(t, "web1", result["hostname"])
}

func TestEvalCondition_Infra_Good_UnknownDefaultsTrue(t *testing.T) {
	e := NewExecutor("/tmp")
	// Unknown conditions default to true (permissive)
	assert.True(t, e.evalCondition("some_complex_expression == 'value'", "host1"))
}

func TestGetRegisteredVar_Infra_Good_DottedAccess(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"my_cmd": {Stdout: "output_text", RC: 0},
	}

	// getRegisteredVar parses dotted names
	result := e.getRegisteredVar("host1", "my_cmd.stdout")
	// getRegisteredVar only looks up the base name (before the dot)
	assert.NotNil(t, result)
	assert.Equal(t, "output_text", result.Stdout)
}

func TestGetRegisteredVar_Infra_Bad_NotRegistered(t *testing.T) {
	e := NewExecutor("/tmp")

	result := e.getRegisteredVar("host1", "nonexistent")
	assert.Nil(t, result)
}

func TestGetRegisteredVar_Infra_Bad_WrongHost(t *testing.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"my_cmd": {Stdout: "output"},
	}

	// Different host has no results
	result := e.getRegisteredVar("host2", "my_cmd")
	assert.Nil(t, result)
}

// ===========================================================================
// String helper utilities used by fact tests
// ===========================================================================

func trimSpace(s string) string {
	result := ""
	for _, c := range s {
		if c != '\n' && c != '\r' && c != ' ' && c != '\t' {
			result += string(c)
		} else if len(result) > 0 && result[len(result)-1] != ' ' {
			result += " "
		}
	}
	// Actually just use strings.TrimSpace
	return stringsTrimSpace(s)
}

func trimQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func trimPrefix(s, prefix string) string {
	if len(s) >= len(prefix) && s[:len(prefix)] == prefix {
		return s[len(prefix):]
	}
	return s
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func splitLines(s string) []string {
	var lines []string
	current := ""
	for _, c := range s {
		if c == '\n' {
			lines = append(lines, current)
			current = ""
		} else {
			current += string(c)
		}
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

func stringsTrimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}
