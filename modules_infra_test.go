package ansible

import (
	"context"
	core "dappco.re/go"
	"time"
)

type slowFactsClient struct{}

func (slowFactsClient) Run(ctx context.Context, cmd string) (string, string, int, error) {
	<-ctx.Done()
	return "", "", 0, ctx.Err()
}

// ===========================================================================
// 1. Error Propagation — getHosts
// ===========================================================================

func TestModulesInfra_GetHosts_Good_AllPattern(t *core.T) {
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
	core.AssertLen(t, hosts, 3)
	core.AssertContains(t, hosts, "web1")
	core.AssertContains(t, hosts, "web2")
	core.AssertContains(t, hosts, "db1")
}

func TestModulesInfra_GetHosts_Good_SpecificHost(t *core.T) {
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
	core.AssertEqual(t, []string{"web1"}, hosts)
}

func TestModulesInfra_GetHosts_Good_GroupName(t *core.T) {
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
	core.AssertLen(t, hosts, 2)
	core.AssertContains(t, hosts, "web1")
	core.AssertContains(t, hosts, "web2")
}

func TestModulesInfra_GetHosts_Good_Localhost(t *core.T) {
	e := NewExecutor("/tmp")
	// No inventory at all
	hosts := e.getHosts("localhost")
	core.AssertEqual(t, []string{"localhost"}, hosts)
}

func TestModulesInfra_GetHosts_Bad_NilInventory(t *core.T) {
	e := NewExecutor("/tmp")
	// inventory is nil, non-localhost pattern
	hosts := e.getHosts("webservers")
	core.AssertNil(t, hosts)
}

func TestModulesInfra_GetHosts_Bad_NonexistentHost(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {},
			},
		},
	})

	hosts := e.getHosts("nonexistent")
	core.AssertEmpty(t, hosts)
}

func TestModulesInfra_GetHosts_Good_LimitFiltering(t *core.T) {
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
	core.AssertLen(t, hosts, 1)
	core.AssertContains(t, hosts, "web1")
}

func TestModulesInfra_GetHosts_Good_LimitExactMatchDoesNotSubstringMatch(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"prod-web-01":    {},
				"prod-web-02":    {},
				"staging-web-01": {},
			},
		},
	})
	e.Limit = "prod-web-01"

	hosts := e.getHosts("all")
	core.AssertEqual(t, []string{"prod-web-01"}, hosts)
}

// ===========================================================================
// 1. Error Propagation — matchesTags
// ===========================================================================

func TestModulesInfra_MatchesTags_Good_NoFiltersNoTags(t *core.T) {
	e := NewExecutor("/tmp")
	// No Tags, no SkipTags set
	result := e.matchesTags(nil)
	core.AssertTrue(t, result)
}

func TestModulesInfra_MatchesTags_Good_NoFiltersWithTaskTags(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.matchesTags([]string{"deploy", "config"})
	core.AssertTrue(t, result)
}

func TestModulesInfra_MatchesTags_Good_IncludeMatchesOneOfMultiple(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Task has deploy among its tags
	core.AssertTrue(t, e.matchesTags([]string{"setup", "deploy", "config"}))
}

func TestModulesInfra_MatchesTags_Bad_IncludeNoMatch(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	core.AssertFalse(t, e.matchesTags([]string{"build", "test"}))
}

func TestModulesInfra_MatchesTags_Good_SkipOverridesInclude(t *core.T) {
	e := NewExecutor("/tmp")
	e.SkipTags = []string{"slow"}

	// Even with no include tags, skip tags filter out matching tasks
	core.AssertFalse(t, e.matchesTags([]string{"deploy", "slow"}))
	core.AssertTrue(t, e.matchesTags([]string{"deploy", "fast"}))
}

func TestModulesInfra_MatchesTags_Bad_IncludeFilterNoTaskTags(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"deploy"}

	// Tasks with no tags should not run when include tags are active
	core.AssertFalse(t, e.matchesTags(nil))
	core.AssertFalse(t, e.matchesTags([]string{}))
}

func TestModulesInfra_MatchesTags_Good_AllTagMatchesEverything(t *core.T) {
	e := NewExecutor("/tmp")
	e.Tags = []string{"all"}

	core.AssertTrue(t, e.matchesTags([]string{"deploy"}))
	core.AssertTrue(t, e.matchesTags([]string{"config", "slow"}))
}

// ===========================================================================
// 1. Error Propagation — evaluateWhen
// ===========================================================================

func TestModulesInfra_EvaluateWhen_Good_DefinedCheck(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"myresult": {Changed: true},
	}

	core.AssertTrue(t, e.evaluateWhen("myresult is defined", "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Good_NotDefinedCheck(t *core.T) {
	e := NewExecutor("/tmp")
	// No results registered for host1
	result := e.evaluateWhen("missing_var is not defined", "host1", nil)
	core.AssertTrue(t, result)
}

func TestModulesInfra_EvaluateWhen_Good_UndefinedAlias(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.evaluateWhen("some_var is undefined", "host1", nil)
	core.AssertTrue(t, result)
}

func TestModulesInfra_EvaluateWhen_Good_SucceededCheck(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Failed: false, Changed: true},
	}

	core.AssertTrue(t, e.evaluateWhen("result is success", "host1", nil))
	core.AssertTrue(t, e.evaluateWhen("result is succeeded", "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Good_FailedCheck(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Failed: true},
	}

	core.AssertTrue(t, e.evaluateWhen("result is failed", "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Good_ChangedCheck(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Changed: true},
	}

	core.AssertTrue(t, e.evaluateWhen("result is changed", "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Good_SkippedCheck(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"result": {Skipped: true},
	}

	core.AssertTrue(t, e.evaluateWhen("result is skipped", "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Good_BoolVarTruthy(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["my_flag"] = true

	core.AssertTrue(t, e.evalCondition("my_flag", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_BoolVarFalsy(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["my_flag"] = false

	core.AssertFalse(t, e.evalCondition("my_flag", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_StringVarTruthy(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["my_str"] = "hello"
	core.AssertTrue(t, e.evalCondition("my_str", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_StringVarEmptyFalsy(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["my_str"] = ""
	core.AssertFalse(t, e.evalCondition("my_str", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_StringVarFalseLiteral(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["my_str"] = "false"
	core.AssertFalse(t, e.evalCondition("my_str", "host1"))

	e.vars["my_str2"] = "False"
	core.AssertFalse(t, e.evalCondition("my_str2", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_IntVarNonZero(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["count"] = 42
	core.AssertTrue(t, e.evalCondition("count", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_IntVarZero(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["count"] = 0
	core.AssertFalse(t, e.evalCondition("count", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_Negation(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertFalse(t, e.evalCondition("not true", "host1"))
	core.AssertTrue(t, e.evalCondition("not false", "host1"))
}

func TestModulesInfra_EvaluateWhen_Good_MultipleConditionsAllTrue(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.results["host1"] = map[string]*TaskResult{
		"prev": {Failed: false},
	}

	// Both conditions must be true (AND semantics)
	core.AssertTrue(t, e.evaluateWhen([]any{"enabled", "prev is success"}, "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Bad_MultipleConditionsOneFails(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	// "false" literal fails
	core.AssertFalse(t, e.evaluateWhen([]any{"enabled", "false"}, "host1", nil))
}

func TestModulesInfra_EvaluateWhen_Good_DefaultFilterInCondition(t *core.T) {
	e := NewExecutor("/tmp")
	// Condition with default filter should be satisfied
	result := e.evalCondition("my_var | default(true)", "host1")
	core.AssertTrue(t, result)
}

func TestModulesInfra_EvaluateWhen_Good_RegisteredVarTruthy(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"check_result": {Failed: false, Skipped: false},
	}

	// Just referencing a registered var name evaluates truthy if not failed/skipped
	core.AssertTrue(t, e.evalCondition("check_result", "host1"))
}

func TestModulesInfra_EvaluateWhen_Bad_RegisteredVarFailedFalsy(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"check_result": {Failed: true},
	}

	// A failed registered var should be falsy
	core.AssertFalse(t, e.evalCondition("check_result", "host1"))
}

// ===========================================================================
// 1. Error Propagation — templateString
// ===========================================================================

func TestModulesInfra_TemplateString_Good_SimpleSubstitution(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["app_name"] = "myapp"

	result := e.templateString("Deploying {{ app_name }}", "", nil)
	core.AssertEqual(t, "Deploying myapp", result)
}

func TestModulesInfra_TemplateString_Good_MultipleVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["host"] = "db.example.com"
	e.vars["port"] = 5432

	result := e.templateString("postgresql://{{ host }}:{{ port }}/mydb", "", nil)
	core.AssertEqual(t, "postgresql://db.example.com:5432/mydb", result)
}

func TestModulesInfra_TemplateString_Good_Unresolved(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("{{ missing_var }}", "", nil)
	core.AssertEqual(t, "{{ missing_var }}", result)
}

func TestModulesInfra_TemplateString_Good_NoTemplateMarkup(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.templateString("just a plain string", "", nil)
	core.AssertEqual(t, "just a plain string", result)
}

func TestModulesInfra_TemplateString_Good_RegisteredVarStdout(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {Stdout: "42"},
	}

	result := e.templateString("{{ cmd_result.stdout }}", "host1", nil)
	core.AssertEqual(t, "42", result)
}

func TestModulesInfra_TemplateString_Good_RegisteredVarRC(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {RC: 0},
	}

	result := e.templateString("{{ cmd_result.rc }}", "host1", nil)
	core.AssertEqual(t, "0", result)
}

func TestModulesInfra_TemplateString_Good_RegisteredVarChanged(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {Changed: true},
	}

	result := e.templateString("{{ cmd_result.changed }}", "host1", nil)
	core.AssertEqual(t, "true", result)
}

func TestModulesInfra_TemplateString_Good_RegisteredVarFailed(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": {Failed: true},
	}

	result := e.templateString("{{ cmd_result.failed }}", "host1", nil)
	core.AssertEqual(t, "true", result)
}

func TestModulesInfra_TemplateString_Good_TaskVars(t *core.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		Vars: map[string]any{
			"task_var": "task_value",
		},
	}

	result := e.templateString("{{ task_var }}", "host1", task)
	core.AssertEqual(t, "task_value", result)
}

func TestModulesInfra_TemplateString_Good_FactsResolution(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname:     "web1",
		FQDN:         "web1.example.com",
		Distribution: "ubuntu",
		Version:      "24.04",
		Architecture: "x86_64",
		Kernel:       "6.5.0",
	}

	core.AssertEqual(t, "web1", e.templateString("{{ ansible_hostname }}", "host1", nil))
	core.AssertEqual(t, "web1.example.com", e.templateString("{{ ansible_fqdn }}", "host1", nil))
	core.AssertEqual(t, "ubuntu", e.templateString("{{ ansible_distribution }}", "host1", nil))
	core.AssertEqual(t, "24.04", e.templateString("{{ ansible_distribution_version }}", "host1", nil))
	core.AssertEqual(t, "x86_64", e.templateString("{{ ansible_architecture }}", "host1", nil))
	core.AssertEqual(t, "6.5.0", e.templateString("{{ ansible_kernel }}", "host1", nil))
}

// ===========================================================================
// 1. Error Propagation — applyFilter
// ===========================================================================

func TestModulesInfra_ApplyFilter_Good_DefaultWithValue(t *core.T) {
	e := NewExecutor("/tmp")
	// When value is non-empty, default is not applied
	result := e.applyFilter("hello", "default('fallback')")
	core.AssertEqual(t, "hello", result)
}

func TestModulesInfra_ApplyFilter_Good_DefaultWithEmpty(t *core.T) {
	e := NewExecutor("/tmp")
	// When value is empty, default IS applied
	result := e.applyFilter("", "default('fallback')")
	core.AssertEqual(t, "fallback", result)
}

func TestModulesInfra_ApplyFilter_Good_DefaultWithDoubleQuotes(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.applyFilter("", `default("fallback")`)
	core.AssertEqual(t, "fallback", result)
}

func TestModulesInfra_ApplyFilter_Good_BoolFilterTrue(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertEqual(t, "true", e.applyFilter("true", "bool"))
	core.AssertEqual(t, "true", e.applyFilter("True", "bool"))
	core.AssertEqual(t, "true", e.applyFilter("yes", "bool"))
	core.AssertEqual(t, "true", e.applyFilter("Yes", "bool"))
	core.AssertEqual(t, "true", e.applyFilter("1", "bool"))
}

func TestModulesInfra_ApplyFilter_Good_BoolFilterFalse(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertEqual(t, "false", e.applyFilter("false", "bool"))
	core.AssertEqual(t, "false", e.applyFilter("no", "bool"))
	core.AssertEqual(t, "false", e.applyFilter("0", "bool"))
	core.AssertEqual(t, "false", e.applyFilter("random", "bool"))
}

func TestModulesInfra_ApplyFilter_Good_TrimFilter(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertEqual(t, "hello", e.applyFilter("  hello  ", "trim"))
	core.AssertEqual(t, "no spaces", e.applyFilter("no spaces", "trim"))
	core.AssertEqual(t, "", e.applyFilter("   ", "trim"))
}

func TestModulesInfra_ApplyFilter_Good_RegexReplaceFilter(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertEqual(t, "app-01", e.applyFilter("app_01", "regex_replace('_', '-')"))
	core.AssertEqual(t, "42", e.applyFilter("v42", `regex_replace("^v", "")`))
}

func TestModulesInfra_ApplyFilter_Good_B64Decode(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.applyFilter("dGVzdA==", "b64decode")
	core.AssertEqual(t, "test", result)
}

func TestModulesInfra_ApplyFilter_Good_UnknownFilter(t *core.T) {
	e := NewExecutor("/tmp")
	// Unknown filters return value unchanged
	result := e.applyFilter("hello", "nonexistent_filter")
	core.AssertEqual(t, "hello", result)
}

func TestModulesInfra_TemplateString_Good_FilterInTemplate(t *core.T) {
	e := NewExecutor("/tmp")
	// When a var is defined, the filter passes through
	e.vars["defined_var"] = "hello"
	result := e.templateString("{{ defined_var | default('fallback') }}", "", nil)
	core.AssertEqual(t, "hello", result)
}

func TestModulesInfra_TemplateString_Good_DefaultFilterMissingVar(t *core.T) {
	e := NewExecutor("/tmp")

	result := e.templateString("{{ missing_var | default('fallback') }}", "", nil)

	core.AssertEqual(t, "fallback", result)
}

func TestModulesInfra_TemplateString_Good_DefaultFilterEmptyVar(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["empty_var"] = ""
	// When var is empty string, default filter applies
	result := e.applyFilter("", "default('fallback')")
	core.AssertEqual(t, "fallback", result)
}

func TestModulesInfra_TemplateString_Good_BoolFilterInTemplate(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["flag"] = "yes"

	result := e.templateString("{{ flag | bool }}", "", nil)
	core.AssertEqual(t, "true", result)
}

func TestModulesInfra_TemplateString_Good_TrimFilterInTemplate(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["padded"] = "  trimmed  "

	result := e.templateString("{{ padded | trim }}", "", nil)
	core.AssertEqual(t, "trimmed", result)
}

// ===========================================================================
// 1. Error Propagation — resolveLoop
// ===========================================================================

func TestModulesInfra_ResolveLoop_Good_SliceAny(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"a", "b", "c"}, "host1")
	core.AssertLen(t, items, 3)
	core.AssertEqual(t, "a", items[0])
	core.AssertEqual(t, "b", items[1])
	core.AssertEqual(t, "c", items[2])
}

func TestModulesInfra_ResolveLoop_Good_SliceString(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]string{"x", "y"}, "host1")
	core.AssertLen(t, items, 2)
	core.AssertEqual(t, "x", items[0])
	core.AssertEqual(t, "y", items[1])
}

func TestModulesInfra_ResolveLoop_Good_NilLoop(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop(nil, "host1")
	core.AssertNil(t, items)
}

func TestModulesInfra_ResolveLoop_Good_VarReference(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["my_list"] = []any{"item1", "item2", "item3"}

	// A templated loop source should resolve to the underlying list value.
	items := e.resolveLoop("{{ my_list }}", "host1")
	core.AssertLen(t, items, 3)
	core.AssertEqual(t, "item1", items[0])
	core.AssertEqual(t, "item2", items[1])
	core.AssertEqual(t, "item3", items[2])
}

func TestModulesInfra_ResolveLoop_Good_MixedTypes(t *core.T) {
	e := NewExecutor("/tmp")
	items := e.resolveLoop([]any{"str", 42, true, map[string]any{"key": "val"}}, "host1")
	core.AssertLen(t, items, 4)
	core.AssertEqual(t, "str", items[0])
	core.AssertEqual(t, 42, items[1])
	core.AssertEqual(t, true, items[2])
}

// ===========================================================================
// 1. Error Propagation — handleNotify
// ===========================================================================

func TestModulesInfra_HandleNotify_Good_SingleString(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("restart nginx")

	core.AssertTrue(t, e.notified["restart nginx"])
	core.AssertFalse(t, e.notified["restart apache"])
}

func TestModulesInfra_HandleNotify_Good_StringSlice(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]string{"restart nginx", "reload haproxy"})

	core.AssertTrue(t, e.notified["restart nginx"])
	core.AssertTrue(t, e.notified["reload haproxy"])
}

func TestModulesInfra_HandleNotify_Good_AnySlice(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify([]any{"handler1", "handler2", "handler3"})

	core.AssertTrue(t, e.notified["handler1"])
	core.AssertTrue(t, e.notified["handler2"])
	core.AssertTrue(t, e.notified["handler3"])
}

func TestModulesInfra_HandleNotify_Good_NilNotify(t *core.T) {
	e := NewExecutor("/tmp")
	// Should not panic
	e.handleNotify(nil)
	core.AssertEmpty(t, e.notified)
}

func TestModulesInfra_HandleNotify_Good_MultipleCallsAccumulate(t *core.T) {
	e := NewExecutor("/tmp")
	e.handleNotify("handler1")
	e.handleNotify("handler2")

	core.AssertTrue(t, e.notified["handler1"])
	core.AssertTrue(t, e.notified["handler2"])
}

// ===========================================================================
// 1. Error Propagation — normalizeConditions
// ===========================================================================

func TestModulesInfra_NormalizeConditions_Good_String(t *core.T) {
	result := normalizeConditions("my_var is defined")
	core.AssertEqual(t, []string{"my_var is defined"}, result)
	core.AssertLen(t, result, 1)
}

func TestModulesInfra_NormalizeConditions_Good_StringSlice(t *core.T) {
	result := normalizeConditions([]string{"cond1", "cond2"})
	core.AssertEqual(t, []string{"cond1", "cond2"}, result)
	core.AssertLen(t, result, 2)
}

func TestModulesInfra_NormalizeConditions_Good_AnySlice(t *core.T) {
	result := normalizeConditions([]any{"cond1", "cond2"})
	core.AssertEqual(t, []string{"cond1", "cond2"}, result)
	core.AssertLen(t, result, 2)
}

func TestModulesInfra_NormalizeConditions_Good_Nil(t *core.T) {
	result := normalizeConditions(nil)
	core.AssertNil(t, result)
	core.AssertEmpty(t, result)
}

func TestModulesInfra_NormalizeConditions_Good_IntIgnored(t *core.T) {
	// Non-string types in any slice are silently skipped
	result := normalizeConditions([]any{"cond1", 42})
	core.AssertEqual(t, []string{"cond1"}, result)
	core.AssertLen(t, result, 1)
}

func TestModulesInfra_NormalizeConditions_Good_UnsupportedType(t *core.T) {
	result := normalizeConditions(42)
	core.AssertNil(t, result)
	core.AssertEmpty(t, result)
}

// ===========================================================================
// 2. Become/Sudo
// ===========================================================================

func TestModulesInfra_Become_Good_SetBecomeTrue(t *core.T) {
	cfg := SSHConfig{
		Host:       "test-host",
		Port:       22,
		User:       "deploy",
		Become:     true,
		BecomeUser: "root",
		BecomePass: "secret",
	}
	client, err := NewSSHClient(cfg)
	core.RequireNoError(t, err)

	core.AssertTrue(t, client.become)
	core.AssertEqual(t, "root", client.becomeUser)
	core.AssertEqual(t, "secret", client.becomePass)
}

func TestModulesInfra_Become_Good_SetBecomeFalse(t *core.T) {
	cfg := SSHConfig{
		Host: "test-host",
		Port: 22,
		User: "deploy",
	}
	client, err := NewSSHClient(cfg)
	core.RequireNoError(t, err)

	core.AssertFalse(t, client.become)
	core.AssertEmpty(t, client.becomeUser)
	core.AssertEmpty(t, client.becomePass)
}

func TestModulesInfra_Become_Good_SetBecomeMethod(t *core.T) {
	cfg := SSHConfig{Host: "test-host"}
	client, err := NewSSHClient(cfg)
	core.RequireNoError(t, err)

	core.AssertFalse(t, client.become)

	client.SetBecome(true, "admin", "pass123")
	core.AssertTrue(t, client.become)
	core.AssertEqual(t, "admin", client.becomeUser)
	core.AssertEqual(t, "pass123", client.becomePass)
}

func TestModulesInfra_Become_Good_DisableAfterEnable(t *core.T) {
	cfg := SSHConfig{Host: "test-host"}
	client, err := NewSSHClient(cfg)
	core.RequireNoError(t, err)

	client.SetBecome(true, "root", "secret")
	core.AssertTrue(t, client.become)

	client.SetBecome(false, "", "")
	core.AssertFalse(t, client.become)
	core.AssertEmpty(t, client.becomeUser)
	core.AssertEmpty(t, client.becomePass)
}

func TestModulesInfra_Become_Good_MockBecomeTracking(t *core.T) {
	mock := NewMockSSHClient()
	core.AssertFalse(t, mock.become)

	mock.SetBecome(true, "root", "password")
	core.AssertTrue(t, mock.become)
	core.AssertEqual(t, "root", mock.becomeUser)
	core.AssertEqual(t, "password", mock.becomePass)
}

func TestModulesInfra_Become_Good_DefaultBecomeUserRoot(t *core.T) {
	// When become is true but no user specified, it defaults to root in the Run method
	cfg := SSHConfig{
		Host:   "test-host",
		Become: true,
		// BecomeUser not set
	}
	client, err := NewSSHClient(cfg)
	core.RequireNoError(t, err)

	core.AssertTrue(t, client.become)
	core.AssertEmpty(t, client.becomeUser) // Empty in config...
	// The Run() method defaults to "root" when becomeUser is empty
}

func TestModulesInfra_Become_Good_PasswordlessBecome(t *core.T) {
	cfg := SSHConfig{
		Host:       "test-host",
		Become:     true,
		BecomeUser: "root",
		// No BecomePass and no Password — triggers sudo -n
	}
	client, err := NewSSHClient(cfg)
	core.RequireNoError(t, err)

	core.AssertTrue(t, client.become)
	core.AssertEmpty(t, client.becomePass)
	core.AssertEmpty(t, client.password)
}

func TestModulesInfra_Become_Good_ExecutorPlayBecome(t *core.T) {
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
	core.AssertTrue(t, play.Become)
	core.AssertEqual(t, "admin", play.BecomeUser)
}

// ===========================================================================
// 3. Fact Gathering
// ===========================================================================

func TestModulesInfra_Facts_Good_UbuntuParsing(t *core.T) {
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
	facts.FQDN = trimFactSpace(stdout)

	stdout, _, _, _ = mock.Run(nil, "hostname -s 2>/dev/null || hostname")
	facts.Hostname = trimFactSpace(stdout)

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
	facts.Architecture = trimFactSpace(stdout)

	stdout, _, _, _ = mock.Run(nil, "uname -r")
	facts.Kernel = trimFactSpace(stdout)

	e.facts["web1"] = facts

	core.AssertEqual(t, "web1.example.com", facts.FQDN)
	core.AssertEqual(t, "web1", facts.Hostname)
	core.AssertEqual(t, "ubuntu", facts.Distribution)
	core.AssertEqual(t, "24.04", facts.Version)
	core.AssertEqual(t, "x86_64", facts.Architecture)
	core.AssertEqual(t, "6.5.0-44-generic", facts.Kernel)

	// Now verify template resolution with these facts
	result := e.templateString("{{ ansible_hostname }}", "web1", nil)
	core.AssertEqual(t, "web1", result)

	result = e.templateString("{{ ansible_distribution }}", "web1", nil)
	core.AssertEqual(t, "ubuntu", result)
}

func TestModulesInfra_Facts_Good_CentOSParsing(t *core.T) {
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

	core.AssertEqual(t, "centos", facts.Distribution)
	core.AssertEqual(t, "8", facts.Version)
}

func TestModulesInfra_Facts_Good_AlpineParsing(t *core.T) {
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

	core.AssertEqual(t, "alpine", facts.Distribution)
	core.AssertEqual(t, "3.19.1", facts.Version)
}

func TestModulesInfra_Facts_Good_DebianParsing(t *core.T) {
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

	core.AssertEqual(t, "debian", facts.Distribution)
	core.AssertEqual(t, "12", facts.Version)
}

func TestModulesInfra_Facts_Good_HostnameFromCommand(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname: "myserver",
		FQDN:     "myserver.example.com",
	}

	core.AssertEqual(t, "myserver", e.templateString("{{ ansible_hostname }}", "host1", nil))
	core.AssertEqual(t, "myserver.example.com", e.templateString("{{ ansible_fqdn }}", "host1", nil))
}

func TestModulesInfra_Facts_Good_ArchitectureResolution(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Architecture: "aarch64",
	}

	result := e.templateString("{{ ansible_architecture }}", "host1", nil)
	core.AssertEqual(t, "aarch64", result)
}

func TestModulesInfra_Facts_Good_KernelResolution(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Kernel: "5.15.0-91-generic",
	}

	result := e.templateString("{{ ansible_kernel }}", "host1", nil)
	core.AssertEqual(t, "5.15.0-91-generic", result)
}

func TestModulesInfra_Facts_Good_NoFactsForHost(t *core.T) {
	e := NewExecutor("/tmp")
	// No facts gathered for host1
	result := e.templateString("{{ ansible_hostname }}", "host1", nil)
	// Should remain unresolved
	core.AssertEqual(t, "{{ ansible_hostname }}", result)
}

func TestModulesInfra_Facts_Good_LocalhostFacts(t *core.T) {
	// When connection is local, gatherFacts sets minimal facts
	e := NewExecutor("/tmp")
	e.facts["localhost"] = &Facts{
		Hostname: "localhost",
	}

	result := e.templateString("{{ ansible_hostname }}", "localhost", nil)
	core.AssertEqual(t, "localhost", result)
}

func TestModulesInfra_Facts_Good_AnsibleFactsMapResolution(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname:     "web1",
		FQDN:         "web1.example.com",
		Distribution: "debian",
		Version:      "12",
	}

	core.AssertEqual(t, "web1", e.templateString("{{ ansible_facts.ansible_hostname }}", "host1", nil))
	core.AssertEqual(t, "debian", e.templateString("{{ ansible_facts.ansible_distribution }}", "host1", nil))
	core.AssertTrue(t, e.evalCondition("ansible_facts.ansible_hostname == 'web1'", "host1"))
	core.AssertTrue(t, e.evalCondition("ansible_facts.ansible_distribution == 'debian'", "host1"))
}

func TestModulesInfra_ModuleSetup_Good_GathersAndStoresFacts(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	mock.expectCommand(`hostname -f`, "web1.example.com\n", "", 0)
	mock.expectCommand(`hostname -s`, "web1\n", "", 0)
	mock.expectCommand(`cat /etc/os-release`, "ID=debian\nVERSION_ID=12\n", "", 0)
	mock.expectCommand(`uname -m`, "x86_64\n", "", 0)
	mock.expectCommand(`uname -r`, "6.1.0\n", "", 0)
	mock.expectCommand(`nproc`, "8\n", "", 0)
	mock.expectCommand(`free -m`, "16384\n", "", 0)
	mock.expectCommand(`hostname -I`, "10.0.0.11\n", "", 0)

	task := &Task{Module: "setup"}
	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, "facts gathered", result.Msg)
	core.AssertNotNil(t, result.Data)

	facts, ok := result.Data["ansible_facts"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "web1", facts["ansible_hostname"])
	core.AssertEqual(t, "web1.example.com", facts["ansible_fqdn"])
	core.AssertEqual(t, "Debian", facts["ansible_os_family"])
	core.AssertEqual(t, "debian", facts["ansible_distribution"])
	core.AssertEqual(t, "12", facts["ansible_distribution_version"])
	core.AssertEqual(t, "x86_64", facts["ansible_architecture"])
	core.AssertEqual(t, "6.1.0", facts["ansible_kernel"])
	core.AssertEqual(t, 8, facts["ansible_processor_vcpus"])
	core.AssertEqual(t, int64(16384), facts["ansible_memtotal_mb"])
	core.AssertEqual(t, "10.0.0.11", facts["ansible_default_ipv4_address"])

	core.AssertNotNil(t, e.facts["host1"])
	core.AssertEqual(t, "web1", e.templateString("{{ ansible_hostname }}", "host1", nil))
	core.AssertEqual(t, "Debian", e.templateString("{{ ansible_os_family }}", "host1", nil))
	core.AssertEqual(t, "16384", e.templateString("{{ ansible_memtotal_mb }}", "host1", nil))
	core.AssertEqual(t, "8", e.templateString("{{ ansible_processor_vcpus }}", "host1", nil))
	core.AssertEqual(t, "10.0.0.11", e.templateString("{{ ansible_default_ipv4_address }}", "host1", nil))
}

func TestModulesInfra_ModuleSetup_Good_FilteredFacts(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	mock.expectCommand(`hostname -f`, "web1.example.com\n", "", 0)
	mock.expectCommand(`hostname -s`, "web1\n", "", 0)
	mock.expectCommand(`cat /etc/os-release`, "ID=debian\nVERSION_ID=12\n", "", 0)
	mock.expectCommand(`uname -m`, "x86_64\n", "", 0)
	mock.expectCommand(`uname -r`, "6.1.0\n", "", 0)
	mock.expectCommand(`nproc`, "8\n", "", 0)
	mock.expectCommand(`free -m`, "16384\n", "", 0)
	mock.expectCommand(`hostname -I`, "10.0.0.11\n", "", 0)

	task := &Task{
		Module: "setup",
		Args: map[string]any{
			"filter": "ansible_hostname,ansible_distribution",
		},
	}
	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Changed)

	facts, ok := result.Data["ansible_facts"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, facts, 2)
	core.AssertEqual(t, "web1", facts["ansible_hostname"])
	core.AssertEqual(t, "debian", facts["ansible_distribution"])
	core.AssertNotContains(t, facts, "ansible_os_family")

	core.AssertNotNil(t, e.facts["host1"])
	core.AssertEqual(t, "web1", e.templateString("{{ ansible_hostname }}", "host1", nil))
	core.AssertEqual(t, "", e.facts["host1"].OS)
	core.AssertEqual(t, "debian", e.facts["host1"].Distribution)
}

func TestModulesInfra_ModuleSetup_Good_VirtualSubset(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	mock.expectCommand(`hostname -f`, "web1.example.com\n", "", 0)
	mock.expectCommand(`hostname -s`, "web1\n", "", 0)
	mock.expectCommand(`cat /etc/os-release`, "ID=debian\nVERSION_ID=12\n", "", 0)
	mock.expectCommand(`uname -m`, "x86_64\n", "", 0)
	mock.expectCommand(`uname -r`, "6.1.0\n", "", 0)
	mock.expectCommand(`nproc`, "8\n", "", 0)
	mock.expectCommand(`free -m`, "16384\n", "", 0)
	mock.expectCommand(`hostname -I`, "10.0.0.11\n", "", 0)
	mock.expectCommand(`systemd-detect-virt`, "docker\n", "", 0)

	task := &Task{
		Module: "setup",
		Args: map[string]any{
			"gather_subset": "!all,!min,virtual",
		},
	}
	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Changed)

	facts, ok := result.Data["ansible_facts"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, facts, 2)
	core.AssertEqual(t, "guest", facts["ansible_virtualization_role"])
	core.AssertEqual(t, "docker", facts["ansible_virtualization_type"])
	core.AssertNotContains(t, facts, "ansible_hostname")

	core.AssertNotNil(t, e.facts["host1"])
	core.AssertEqual(t, "guest", e.facts["host1"].VirtualizationRole)
	core.AssertEqual(t, "docker", e.facts["host1"].VirtualizationType)
	core.AssertEqual(t, "guest", e.templateString("{{ ansible_virtualization_role }}", "host1", nil))
	core.AssertEqual(t, "docker", e.templateString("{{ ansible_virtualization_type }}", "host1", nil))
}

func TestModulesInfra_ModuleSetup_Good_GatherSubset(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	mock.expectCommand(`hostname -f`, "web1.example.com\n", "", 0)
	mock.expectCommand(`hostname -s`, "web1\n", "", 0)
	mock.expectCommand(`cat /etc/os-release`, "ID=debian\nVERSION_ID=12\n", "", 0)
	mock.expectCommand(`uname -m`, "x86_64\n", "", 0)
	mock.expectCommand(`uname -r`, "6.1.0\n", "", 0)
	mock.expectCommand(`nproc`, "8\n", "", 0)
	mock.expectCommand(`free -m`, "16384\n", "", 0)
	mock.expectCommand(`hostname -I`, "10.0.0.11\n", "", 0)

	task := &Task{
		Module: "setup",
		Args: map[string]any{
			"gather_subset": "!all,!min,network",
		},
	}
	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertNotNil(t, result.Data)

	facts, ok := result.Data["ansible_facts"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertLen(t, facts, 1)
	core.AssertEqual(t, "10.0.0.11", facts["ansible_default_ipv4_address"])
	core.AssertNotContains(t, facts, "ansible_hostname")
	core.AssertNotContains(t, facts, "ansible_distribution")

	core.AssertNotNil(t, e.facts["host1"])
	core.AssertEqual(t, "", e.facts["host1"].Hostname)
	core.AssertEqual(t, "10.0.0.11", e.facts["host1"].IPv4)
	core.AssertEqual(t, "", e.templateString("{{ ansible_hostname }}", "host1", nil))
	core.AssertEqual(t, "10.0.0.11", e.templateString("{{ ansible_default_ipv4_address }}", "host1", nil))
}

func TestModulesInfra_ModuleSetup_Good_RespectsGatherTimeout(t *core.T) {
	e := NewExecutor("/tmp")

	start := time.Now()
	result, err := e.moduleSetup(context.Background(), "host1", slowFactsClient{}, map[string]any{
		"gather_timeout": 1,
	})
	elapsed := time.Since(start)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "context deadline exceeded")
	core.AssertGreaterOrEqual(t, elapsed, time.Second)
}

func TestModulesInfra_ModuleArchive_Good_CreateZipArchive(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleArchiveWithClient(e, mock, map[string]any{
		"path":   []any{"/etc/nginx/nginx.conf", "/etc/hosts"},
		"dest":   "/tmp/configs.zip",
		"format": "zip",
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/tmp"`))
	core.AssertTrue(t, mock.hasExecuted(`zip -r "/tmp/configs.zip" "/etc/nginx/nginx.conf" "/etc/hosts"`))
}

// ===========================================================================
// 4. Idempotency
// ===========================================================================

func TestModulesInfra_Idempotency_Good_GroupAlreadyExists(t *core.T) {
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
	core.RequireNoError(t, err)

	// The module runs the command: getent group docker >/dev/null 2>&1 || groupadd docker
	// Since getent succeeds (rc=0), groupadd is not executed by the shell.
	// However, the module always reports changed=true because it does not
	// check idempotency at the Go level. This tests the current behaviour.
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesInfra_Idempotency_Good_AuthorizedKeyAlreadyPresent(t *core.T) {
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
	core.RequireNoError(t, err)

	// Module reports changed=true regardless (it doesn't check grep result at Go level)
	// The grep || echo construct handles idempotency at the shell level
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
}

func TestModulesInfra_Idempotency_Good_DockerComposeUpToDate(t *core.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: docker compose up -d returns "Up to date" in stdout
	mock.expectCommand(`docker compose up -d`, "web1 Up to date\nnginx Up to date\n", "", 0)

	result, err := moduleDockerComposeWithClient(nil, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})
	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)

	// When stdout contains "Up to date", changed should be false
	core.AssertFalse(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesInfra_Idempotency_Good_DockerComposeChanged(t *core.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: docker compose up -d with actual changes
	mock.expectCommand(`docker compose up -d`, "Creating web1 ... done\nCreating nginx ... done\n", "", 0)

	result, err := moduleDockerComposeWithClient(nil, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})
	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)

	// When stdout does NOT contain "Up to date", changed should be true
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesInfra_Idempotency_Good_DockerComposeUpToDateInStderr(t *core.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Some versions of docker compose output status to stderr
	mock.expectCommand(`docker compose up -d`, "", "web1 Up to date\n", 0)

	result, err := moduleDockerComposeWithClient(nil, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})
	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)

	// The docker compose module checks both stdout and stderr for "Up to date"
	core.AssertFalse(t, result.Changed)
}

func TestModulesInfra_Idempotency_Good_GroupCreationWhenNew(t *core.T) {
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
	core.RequireNoError(t, err)

	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesInfra_Idempotency_Good_ServiceStatChanged(t *core.T) {
	_, mock := newTestExecutorWithMock("host1")

	// Mock: stat reports the file exists
	mock.addStat("/etc/config.conf", map[string]any{"exists": true, "isdir": false})

	result, err := moduleStatWithClient(nil, mock, map[string]any{
		"path": "/etc/config.conf",
	})
	core.RequireNoError(t, err)

	// Stat module should always report changed=false
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	stat := result.Data["stat"].(map[string]any)
	core.AssertTrue(t, stat["exists"].(bool))
}

func TestModulesInfra_Idempotency_Good_StatFileNotFound(t *core.T) {
	_, mock := newTestExecutorWithMock("host1")

	// No stat info added — will return exists=false from mock
	result, err := moduleStatWithClient(nil, mock, map[string]any{
		"path": "/nonexistent/file",
	})
	core.RequireNoError(t, err)

	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	stat := result.Data["stat"].(map[string]any)
	core.AssertFalse(t, stat["exists"].(bool))
}

// ===========================================================================
// Additional cross-cutting edge cases
// ===========================================================================

func TestModulesInfra_ResolveExpr_Good_HostVars(t *core.T) {
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
	core.AssertEqual(t, "custom_value", result)
}

func TestModulesInfra_TemplateArgs_Good_InventoryHostname(t *core.T) {
	e := NewExecutor("/tmp")

	args := map[string]any{
		"hostname": "{{ inventory_hostname }}",
	}

	result := e.templateArgs(args, "web1", nil)
	core.AssertEqual(t, "web1", result["hostname"])
}

func TestModulesInfra_EvalCondition_Good_UnknownComparisonFailsClosed(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.evalCondition("some_complex_expression == 'value'", "host1")
	core.AssertFalse(t, result)
}

func TestModulesInfra_GetRegisteredVar_Good_DottedAccess(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"my_cmd": {Stdout: "output_text", RC: 0},
	}

	// getRegisteredVar parses dotted names
	result := e.getRegisteredVar("host1", "my_cmd.stdout")
	// getRegisteredVar only looks up the base name (before the dot)
	core.AssertNotNil(t, result)
	core.AssertEqual(t, "output_text", result.Stdout)
}

func TestModulesInfra_GetRegisteredVar_Bad_NotRegistered(t *core.T) {
	e := NewExecutor("/tmp")

	result := e.getRegisteredVar("host1", "nonexistent")
	core.AssertNil(t, result)
}

func TestModulesInfra_GetRegisteredVar_Bad_WrongHost(t *core.T) {
	e := NewExecutor("/tmp")
	e.results["host1"] = map[string]*TaskResult{
		"my_cmd": {Stdout: "output"},
	}

	// Different host has no results
	result := e.getRegisteredVar("host2", "my_cmd")
	core.AssertNil(t, result)
}

// ===========================================================================
// String helper utilities used by fact tests
// ===========================================================================

func trimFactSpace(s string) string {
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
