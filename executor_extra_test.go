package ansible

import (
	"context"
	core "dappco.re/go"
)

// ============================================================
// Tests for non-SSH module handlers (0% coverage)
// ============================================================

// --- moduleDebug ---

func TestExecutorExtra_ModuleDebug_Good_Message(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleDebug("host1", nil, map[string]any{"msg": "Hello world"})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, "Hello world", result.Msg)
}

func TestExecutorExtra_ModuleDebug_Good_Var(t *core.T) {
	e := NewExecutor("/tmp")
	e.setHostVars("host1", map[string]any{"my_version": "1.2.3"})

	result, err := e.moduleDebug("host1", nil, map[string]any{"var": "my_version"})

	core.RequireNoError(t, err)
	core.AssertContains(t, result.Msg, "1.2.3")
}

func TestExecutorExtra_ModuleDebug_Good_EmptyArgs(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleDebug("host1", nil, map[string]any{})

	core.RequireNoError(t, err)
	core.AssertEqual(t, "", result.Msg)
}

// --- moduleFail ---

func TestExecutorExtra_ModuleFail_Good_DefaultMessage(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleFail(map[string]any{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, "Failed as requested", result.Msg)
}

func TestExecutorExtra_ModuleFail_Good_CustomMessage(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleFail(map[string]any{"msg": "deployment blocked"})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, "deployment blocked", result.Msg)
}

// --- modulePing ---

func TestExecutorExtra_ModulePing_Good_DefaultPong(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "ping",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, "pong", result.Msg)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "pong", result.Data["ping"])
	core.AssertTrue(t, mock.hasExecuted(`^true$`))
}

func TestExecutorExtra_ModulePing_Good_CustomData(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "ansible.builtin.ping",
		Args: map[string]any{
			"data": "hello",
		},
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "hello", result.Msg)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "hello", result.Data["ping"])
}

func TestExecutorExtra_moduleSetFact_Good_ReturnsStructuredFacts(t *core.T) {
	e := NewExecutor("/tmp")

	result, err := e.moduleSetFact("host1", map[string]any{
		"app_version": "1.2.3",
		"cacheable":   true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	facts, ok := result.Data["ansible_facts"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, "1.2.3", facts["app_version"])
	_, cached := facts["cacheable"]
	core.AssertFalse(t, cached)
	core.AssertEqual(t, "1.2.3", e.hostScopedVars("host1")["app_version"])
	_, cached = e.hostScopedVars("host1")["cacheable"]
	core.AssertFalse(t, cached)
}

func TestExecutorExtra_ExecuteModule_Good_DelegateFactsStoresOnDelegateHost(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()

	task := &Task{
		Module:        "set_fact",
		Args:          map[string]any{"app_version": "2.0.0"},
		Delegate:      "delegate1",
		DelegateFacts: true,
	}

	result, err := e.executeModule(context.Background(), "host1", mock, task, &Play{})
	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "2.0.0", e.hostScopedVars("delegate1")["app_version"])
	core.AssertNil(t, e.hostScopedVars("host1"))
	core.AssertEqual(t, "2.0.0", e.hostFactsMap("delegate1")["app_version"])
	core.AssertNil(t, e.hostFactsMap("host1"))
}

func TestExecutorExtra_ExecuteModule_Good_LegacyNamespaceCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo hello", "hello\n", "", 0)

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "ansible.legacy.command",
		Args:   map[string]any{"_raw_params": "echo hello"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "hello\n", result.Stdout)
	core.AssertTrue(t, mock.hasExecuted(`echo hello`))
}

// --- moduleAssert ---

func TestExecutorExtra_ModuleAssert_Good_PassingAssertion(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true

	result, err := e.moduleAssert(map[string]any{"that": "enabled"}, "host1")

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "All assertions passed", result.Msg)
}

func TestExecutorExtra_ModuleAssert_Bad_FailingAssertion(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = false

	result, err := e.moduleAssert(map[string]any{"that": "enabled"}, "host1")

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "Assertion failed")
}

func TestExecutorExtra_ModuleAssert_Bad_MissingThat(t *core.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleAssert(map[string]any{}, "host1")
	core.AssertError(t, err)
}

func TestExecutorExtra_ModuleAssert_Good_CustomFailMsg(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["ready"] = false

	result, err := e.moduleAssert(map[string]any{
		"that":     "ready",
		"fail_msg": "Service not ready",
	}, "host1")

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, "Service not ready", result.Msg)
}

func TestExecutorExtra_ModuleAssert_Good_MultipleConditions(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["enabled"] = true
	e.vars["count"] = 5

	result, err := e.moduleAssert(map[string]any{
		"that": []any{"enabled", "count"},
	}, "host1")

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
}

// --- moduleSetFact ---

func TestExecutorExtra_moduleSetFact_Good(t *core.T) {
	e := NewExecutor("/tmp")

	result, err := e.moduleSetFact("host1", map[string]any{
		"app_version": "2.0.0",
		"deploy_env":  "production",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertContains(t, e.hostVars, "host1")
	core.AssertEqual(t, "2.0.0", e.hostVars["host1"]["app_version"])
	core.AssertEqual(t, "production", e.hostVars["host1"]["deploy_env"])
}

func TestExecutorExtra_moduleSetFact_Good_SkipsCacheable(t *core.T) {
	e := NewExecutor("/tmp")

	e.moduleSetFact("host1", map[string]any{
		"my_fact":   "value",
		"cacheable": true,
	})

	core.AssertContains(t, e.hostVars, "host1")
	core.AssertEqual(t, "value", e.hostVars["host1"]["my_fact"])
	_, hasCacheable := e.hostVars["host1"]["cacheable"]
	core.AssertFalse(t, hasCacheable)
}

func TestExecutorExtra_moduleSetFact_Good_HostScopedLookup(t *core.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleSetFact("host1", map[string]any{
		"build_id": "2026.04.02",
	})
	core.RequireNoError(t, err)

	core.AssertEqual(t, "2026.04.02", e.templateString("{{ build_id }}", "host1", nil))
	core.AssertEqual(t, "{{ build_id }}", e.templateString("{{ build_id }}", "host2", nil))
}

func TestExecutorExtra_moduleSetFact_Good_ExposesAnsibleFacts(t *core.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleSetFact("host1", map[string]any{
		"app_version": "2.0.0",
	})
	core.RequireNoError(t, err)

	core.AssertEqual(t, "2.0.0", e.templateString("{{ ansible_facts.app_version }}", "host1", nil))
	core.AssertTrue(t, e.evalCondition("ansible_facts.app_version == '2.0.0'", "host1"))
}

func TestExecutorExtra_ClearFacts_Good_RemovesSetFacts(t *core.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleSetFact("host1", map[string]any{
		"app_version": "2.0.0",
	})
	core.RequireNoError(t, err)

	e.clearFacts([]string{"host1"})

	core.AssertEqual(t, "{{ ansible_facts.app_version }}", e.templateString("{{ ansible_facts.app_version }}", "host1", nil))
}

// --- moduleAddHost ---

func TestExecutorExtra_ModuleAddHost_Good_AddsHostAndGroups(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "db1", result.Data["host"])
	core.AssertContains(t, result.Msg, "db1")

	core.AssertNotNil(t, e.inventory)
	core.AssertNotNil(t, e.inventory.All)
	core.AssertNotNil(t, e.inventory.All.Hosts["db1"])

	host := e.inventory.All.Hosts["db1"]
	core.AssertEqual(t, "10.0.0.5", host.AnsibleHost)
	core.AssertEqual(t, 2222, host.AnsiblePort)
	core.AssertEqual(t, "deploy", host.AnsibleUser)
	core.AssertEqual(t, "ssh", host.AnsibleConnection)
	core.AssertEqual(t, "secret", host.AnsibleBecomePassword)
	core.AssertEqual(t, "custom-value", host.Vars["custom_var"])

	core.AssertNotNil(t, e.inventory.All.Children["databases"])
	core.AssertNotNil(t, e.inventory.All.Children["production"])
	core.AssertSame(t, host, e.inventory.All.Children["databases"].Hosts["db1"])
	core.AssertSame(t, host, e.inventory.All.Children["production"].Hosts["db1"])

	core.AssertEqual(t, []string{"db1"}, GetHosts(e.inventory, "all"))
	core.AssertEqual(t, []string{"db1"}, GetHosts(e.inventory, "databases"))
	core.AssertEqual(t, []string{"db1"}, GetHosts(e.inventory, "production"))
}

func TestExecutorExtra_ModuleAddHost_Good_IdempotentRepeat(t *core.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleAddHost(map[string]any{
		"name":   "cache1",
		"groups": []any{"caches"},
		"role":   "redis",
	})
	core.RequireNoError(t, err)

	result, err := e.moduleAddHost(map[string]any{
		"name":   "cache1",
		"groups": []any{"caches"},
		"role":   "redis",
	})
	core.RequireNoError(t, err)

	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, []string{"cache1"}, GetHosts(e.inventory, "caches"))
}

func TestExecutorExtra_ModuleAddHost_Good_ThroughDispatcher(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "cache1", result.Data["host"])
	core.AssertEqual(t, []string{"caches"}, result.Data["groups"])
	core.AssertEqual(t, []string{"cache1"}, GetHosts(e.inventory, "all"))
	core.AssertEqual(t, []string{"cache1"}, GetHosts(e.inventory, "caches"))
	core.AssertEqual(t, "redis", e.inventory.All.Hosts["cache1"].Vars["role"])
}

// --- moduleGroupBy ---

func TestExecutorExtra_moduleGroupBy_Good(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": &Host{AnsibleHost: "10.0.0.10"},
			},
		},
	})

	result, err := e.moduleGroupBy("web1", map[string]any{"key": "debian"})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "web1", result.Data["host"])
	core.AssertEqual(t, "debian", result.Data["group"])
	core.AssertContains(t, result.Msg, "web1")
	core.AssertContains(t, result.Msg, "debian")
	core.AssertEqual(t, []string{"web1"}, GetHosts(e.inventory, "debian"))
}

func TestExecutorExtra_moduleGroupBy_Good_ThroughDispatcher(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	task := &Task{
		Module: "group_by",
		Args: map[string]any{
			"key": "linux",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, []string{"host1"}, GetHosts(e.inventory, "linux"))
}

func TestExecutorExtra_ModuleGroupBy_Bad_MissingKey(t *core.T) {
	e := NewExecutor("/tmp")

	_, err := e.moduleGroupBy("host1", map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "key required")
}

// --- moduleIncludeVars ---

func TestExecutorExtra_ModuleIncludeVars_Good_WithFile(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "main.yml")
	core.RequireNoError(t, writeTestFile(path, []byte("app_name: demo\n"), 0644))

	e := NewExecutor("/tmp")
	result, err := e.moduleIncludeVars(map[string]any{"file": path})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertContains(t, result.Msg, path)
	core.AssertEqual(t, "demo", e.vars["app_name"])
}

func TestExecutorExtra_ModuleIncludeVars_Good_WithRawParams(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "defaults.yml")
	core.RequireNoError(t, writeTestFile(path, []byte("app_port: 8080\n"), 0644))

	e := NewExecutor("/tmp")
	result, err := e.moduleIncludeVars(map[string]any{"_raw_params": path})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertContains(t, result.Msg, path)
	core.AssertEqual(t, 8080, e.vars["app_port"])
}

func TestExecutorExtra_ModuleIncludeVars_Good_Empty(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleIncludeVars(map[string]any{})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
}

// --- moduleMeta ---

func TestExecutorExtra_moduleMeta_Good(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "flush_handlers"})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "flush_handlers", result.Data["action"])
}

func TestExecutorExtra_moduleMeta_Good_ExplicitActionField(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"action": "refresh_inventory"})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "refresh_inventory", result.Data["action"])
}

func TestExecutorExtra_moduleMeta_Good_ClearFacts(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{Hostname: "web01"}

	result, err := e.moduleMeta(map[string]any{"_raw_params": "clear_facts"})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "clear_facts", result.Data["action"])
}

func TestExecutorExtra_moduleMeta_Good_ResetConnection(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "reset_connection"})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "reset_connection", result.Data["action"])
}

func TestExecutorExtra_moduleMeta_Good_EndHost(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "end_host"})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "end_host", result.Data["action"])
}

func TestExecutorExtra_moduleMeta_Good_EndBatch(t *core.T) {
	e := NewExecutor("/tmp")
	result, err := e.moduleMeta(map[string]any{"_raw_params": "end_batch"})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "end_batch", result.Data["action"])
}

func TestExecutorExtra_HandleMetaAction_Good_ClearFacts(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{Hostname: "web01"}
	e.facts["host2"] = &Facts{Hostname: "web02"}

	result := &TaskResult{Data: map[string]any{"action": "clear_facts"}}
	core.RequireNoError(t, e.handleMetaAction(context.Background(), "host1", []string{"host1"}, nil, result))

	_, ok := e.facts["host1"]
	core.AssertFalse(t, ok)
	core.AssertNotNil(t, e.facts["host2"])
	core.AssertEqual(t, "web02", e.facts["host2"].Hostname)
}

func TestExecutorExtra_HandleMetaAction_Good_EndHost(t *core.T) {
	e := NewExecutor("/tmp")

	result := &TaskResult{Data: map[string]any{"action": "end_host"}}
	err := e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, nil, result)

	core.AssertErrorIs(t, err, errEndHost)
	core.AssertTrue(t, e.isHostEnded("host1"))
	core.AssertFalse(t, e.isHostEnded("host2"))
}

func TestExecutorExtra_HandleMetaAction_Good_EndBatch(t *core.T) {
	e := NewExecutor("/tmp")

	result := &TaskResult{Data: map[string]any{"action": "end_batch"}}
	err := e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, nil, result)

	core.AssertErrorIs(t, err, errEndBatch)
	core.AssertFalse(t, e.isHostEnded("host1"))
	core.AssertFalse(t, e.isHostEnded("host2"))
}

func TestExecutorExtra_HandleMetaAction_Good_ResetConnection(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	e.clients["host1"] = mock
	e.clients["host2"] = NewMockSSHClient()

	result := &TaskResult{Data: map[string]any{"action": "reset_connection"}}
	core.RequireNoError(t, e.handleMetaAction(context.Background(), "host1", []string{"host1", "host2"}, nil, result))

	_, ok := e.clients["host1"]
	core.AssertFalse(t, ok)
	_, ok = e.clients["host2"]
	core.AssertTrue(t, ok)
	core.AssertTrue(t, mock.closed)
}

func TestExecutorExtra_RunBlock_Bad_RescueFailurePropagates(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")

	task := &Task{
		Block: []Task{
			{
				Name:   "primary failure",
				Module: "fail",
				Args:   map[string]any{"msg": "block failed"},
			},
		},
		Rescue: []Task{
			{
				Name:   "rescue failure",
				Module: "fail",
				Args:   map[string]any{"msg": "rescue failed"},
			},
		},
	}

	err := e.runBlock(context.Background(), []string{"host1"}, task, &Play{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "rescue failed")
}

func TestExecutorExtra_RunBlock_Good_RescueSuccessClearsBlockFailure(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")

	task := &Task{
		Block: []Task{
			{
				Name:   "primary failure",
				Module: "fail",
				Args:   map[string]any{"msg": "block failed"},
			},
		},
		Rescue: []Task{
			{
				Name:   "rescue success",
				Module: "debug",
				Args:   map[string]any{"msg": "recovered"},
			},
		},
	}

	err := e.runBlock(context.Background(), []string{"host1"}, task, &Play{})

	core.RequireNoError(t, err)
}

func TestExecutorExtra_RunBlock_Good_InheritsBlockVars(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand("echo inherited", "inherited\n", "", 0)

	task := &Task{
		Vars: map[string]any{
			"message": "inherited",
		},
		Block: []Task{
			{
				Name:   "use inherited vars",
				Module: "command",
				Args: map[string]any{
					"_raw_params": "echo {{ message }}",
				},
			},
		},
	}

	err := e.runBlock(context.Background(), []string{"host1"}, task, &Play{})

	core.RequireNoError(t, err)
	core.AssertTrue(t, mock.hasExecuted("echo inherited"))
}

func TestExecutorExtra_RunBlock_Good_InheritsBlockWhen(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")

	task := &Task{
		When: "false",
		Block: []Task{
			{
				Name:   "should be skipped",
				Module: "command",
				Args: map[string]any{
					"_raw_params": "echo blocked",
				},
			},
		},
	}

	err := e.runBlock(context.Background(), []string{"host1"}, task, &Play{})

	core.RequireNoError(t, err)
	core.AssertEqual(t, 0, mock.commandCount())
}

func TestExecutorExtra_RunTaskOnHosts_Good_EndHostSkipsFutureTasks(t *core.T) {
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
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1", "host2"}, first, play))
	core.AssertTrue(t, e.isHostEnded("host1"))
	core.AssertFalse(t, e.isHostEnded("host2"))

	second := &Task{
		Name:   "Follow-up",
		Module: "debug",
		Args:   map[string]any{"msg": "still running"},
	}
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1", "host2"}, second, play))

	core.AssertContains(t, started, "host1:Retire host")
	core.AssertContains(t, started, "host2:Follow-up")
	core.AssertNotContains(t, started, "host1:Follow-up")
}

func TestExecutorExtra_RunPlay_Good_MetaEndBatchAdvancesToNextSerialBatch(t *core.T) {
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

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertContains(t, started, "host1:end current batch")
	core.AssertNotContains(t, started, "host1:follow-up")
	core.AssertContains(t, started, "host2:follow-up")
	core.AssertContains(t, started, "host3:follow-up")
}

func TestExecutorExtra_SplitSerialHosts_Good_ListValues(t *core.T) {
	batches := splitSerialHosts([]string{"host1", "host2", "host3", "host4"}, []any{1, "50%"})

	core.AssertLen(t, batches, 3)
	core.AssertEqual(t, []string{"host1"}, batches[0])
	core.AssertEqual(t, []string{"host2", "host3"}, batches[1])
	core.AssertEqual(t, []string{"host4"}, batches[2])
}

func TestExecutorExtra_SplitSerialHosts_Good_ListRepeatsLastValue(t *core.T) {
	batches := splitSerialHosts([]string{"host1", "host2", "host3", "host4", "host5"}, []any{2, 1})

	core.AssertLen(t, batches, 4)
	core.AssertEqual(t, []string{"host1", "host2"}, batches[0])
	core.AssertEqual(t, []string{"host3"}, batches[1])
	core.AssertEqual(t, []string{"host4"}, batches[2])
	core.AssertEqual(t, []string{"host5"}, batches[3])
}

func TestExecutorExtra_RunPlay_Good_ExposesPlayMagicVars(t *core.T) {
	e := NewExecutor("/tmp")
	gatherFacts := false
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()
	e.clients["host2"] = NewMockSSHClient()

	play := &Play{
		Name:        "Inspect play magic vars",
		Hosts:       "all",
		Serial:      1,
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:   "Inspect play magic vars",
				Module: "debug",
				Args: map[string]any{
					"msg": "{{ ansible_play_name }}|{{ ansible_play_hosts_all }}|{{ ansible_play_hosts }}|{{ ansible_play_batch }}",
				},
				Register: "magic_vars",
			},
		},
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertNotNil(t, e.results["host1"]["magic_vars"])
	core.AssertEqual(t, "Inspect play magic vars|[host1 host2]|[host1 host2]|[host1]", e.results["host1"]["magic_vars"].Msg)
	core.AssertNotNil(t, e.results["host2"]["magic_vars"])
	core.AssertEqual(t, "Inspect play magic vars|[host1 host2]|[host1 host2]|[host2]", e.results["host2"]["magic_vars"].Msg)
}

func TestExecutorExtra_RunPlay_Good_ExposesLimitMagicVar(t *core.T) {
	gatherFacts := false
	e := NewExecutor(t.TempDir())
	e.Limit = "host1"
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {},
				"host2": {},
			},
		},
	})
	e.clients["host1"] = NewMockSSHClient()

	play := &Play{
		Name:        "Inspect limit magic var",
		Hosts:       "all",
		GatherFacts: &gatherFacts,
		Tasks: []Task{
			{
				Name:   "Inspect limit magic var",
				Module: "debug",
				Args: map[string]any{
					"msg": "{{ ansible_limit }}",
				},
				Register: "limit_var",
			},
		},
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertNotNil(t, e.results["host1"]["limit_var"])
	core.AssertEqual(t, "host1", e.results["host1"]["limit_var"].Msg)
	core.AssertNil(t, e.results["host2"])
}

// ============================================================
// Tests for handleLookup (0% coverage)
// ============================================================

func TestExecutorExtra_HandleLookup_Good_EnvVar(t *core.T) {
	e := NewExecutor("/tmp")
	t.Setenv("TEST_ANSIBLE_LOOKUP", "found_it")

	result := e.handleLookup("lookup('env', 'TEST_ANSIBLE_LOOKUP')", "", nil)
	core.AssertEqual(t, "found_it", result)
}

func TestExecutorExtra_HandleLookup_Good_EnvVarMissing(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.handleLookup("lookup('env', 'NONEXISTENT_VAR_12345')", "", nil)
	core.AssertEqual(t, "", result)
}

func TestExecutorExtra_HandleLookup_Good_FileLookupResolvesBasePath(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars.txt"), []byte("from base path"), 0644))

	e := NewExecutor(dir)
	result := e.handleLookup("lookup('file', 'vars.txt')", "", nil)

	core.AssertEqual(t, "from base path", result)
}

func TestExecutorExtra_HandleLookup_Good_TemplateLookupResolvesBasePathAndVars(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "templates", "message.j2"), []byte("Hello {{ name }}"), 0644))

	e := NewExecutor(dir)
	e.SetVar("name", "world")

	result := e.handleLookup("lookup('template', 'templates/message.j2')", "host1", nil)

	core.AssertEqual(t, "Hello world", result)
}

func TestExecutorExtra_HandleLookup_Good_VarsLookup(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetVar("lookup_value", "resolved from vars")

	result := e.handleLookup("lookup('vars', 'lookup_value')", "", nil)

	core.AssertEqual(t, "resolved from vars", result)
}

func TestExecutorExtra_HandleLookup_Good_PipeLookup(t *core.T) {
	e := NewExecutor("/tmp")

	result := e.handleLookup("lookup('pipe', 'printf pipe-value')", "", nil)

	core.AssertEqual(t, "pipe-value", result)
}

func TestExecutorExtra_HandleLookup_Good_FileGlobLookup(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "files", "a.txt"), []byte("alpha"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "files", "b.txt"), []byte("bravo"), 0644))

	e := NewExecutor(dir)
	result := e.handleLookup("lookup('fileglob', 'files/*.txt')", "", nil)

	core.AssertEqual(t, join(",", []string{
		joinPath(dir, "files", "a.txt"),
		joinPath(dir, "files", "b.txt"),
	}), result)
}

func TestExecutorExtra_HandleLookup_Good_PasswordLookupReadsExistingFile(t *core.T) {
	dir := t.TempDir()
	passPath := joinPath(dir, "secrets", "app.pass")
	core.RequireNoError(t, writeTestFile(passPath, []byte("s3cret\n"), 0600))

	e := NewExecutor(dir)
	result := e.handleLookup("lookup('password', 'secrets/app.pass')", "", nil)

	core.AssertEqual(t, "s3cret", result)
}

func TestExecutorExtra_HandleLookup_Good_PasswordLookupCreatesFile(t *core.T) {
	dir := t.TempDir()
	passPath := joinPath(dir, "secrets", "generated.pass")

	e := NewExecutor(dir)
	result := e.handleLookup("lookup('password', 'secrets/generated.pass length=12 chars=digits')", "", nil)

	core.AssertLen(t, result, 12)
	content, err := readTestFile(passPath)
	core.RequireNoError(t, err)
	core.AssertEqual(t, result, string(content))
	core.AssertLen(t, content, 12)
}

func TestExecutorExtra_HandleLookup_Good_PasswordLookupHonoursSeed(t *core.T) {
	dir := t.TempDir()
	e := NewExecutor(dir)

	first := e.handleLookup("lookup('password', '/dev/null length=16 chars=ascii_lowercase seed=inventory_hostname')", "host1", nil)
	second := e.handleLookup("lookup('password', '/dev/null length=16 chars=ascii_lowercase seed=inventory_hostname')", "host1", nil)
	other := e.handleLookup("lookup('password', '/dev/null length=16 chars=ascii_lowercase seed=inventory_hostname')", "host2", nil)

	core.AssertLen(t, first, 16)
	core.AssertEqual(t, first, second)
	core.AssertNotEqual(t, first, other)
}

func TestExecutorExtra_HandleLookup_Good_FirstFoundLookupReturnsFirstExistingPath(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "defaults", "common.yml"), []byte("common: true\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "defaults", "production.yml"), []byte("env: prod\n"), 0644))

	e := NewExecutor(dir)
	e.SetVar("findme", map[string]any{
		"files": []any{"missing.yml", "production.yml", "common.yml"},
		"paths": []any{"defaults"},
	})

	result := e.handleLookup("lookup('first_found', findme)", "", nil)

	core.AssertEqual(t, joinPath(dir, "defaults", "production.yml"), result)
}

func TestExecutorExtra_HandleLookup_Good_FirstFoundLookupAcceptsFQCN(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars", "selected.yml"), []byte("selected: true\n"), 0644))

	e := NewExecutor(dir)
	e.SetVar("findme", map[string]any{
		"files": []any{"missing.yml", "selected.yml"},
		"paths": []any{"vars"},
	})

	result := e.handleLookup("lookup('ansible.builtin.first_found', findme)", "", nil)

	core.AssertEqual(t, joinPath(dir, "vars", "selected.yml"), result)
}

func TestExecutorExtra_RunTaskOnHost_Good_LoopFromFileGlobLookup(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "files", "a.txt"), []byte("alpha"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "files", "b.txt"), []byte("bravo"), 0644))

	e := NewExecutor(dir)
	task := &Task{
		Module: "debug",
		Args: map[string]any{
			"msg": "{{ item }}",
		},
		Loop:     "{{ lookup('fileglob', 'files/*.txt') }}",
		Register: "glob_lookup",
	}

	core.RequireNoError(t, e.runTaskOnHost(context.Background(), "host1", []string{"host1"}, task, &Play{}))

	result := e.results["host1"]["glob_lookup"]
	core.AssertNotNil(t, result)
	core.AssertLen(t, result.Results, 2)
	core.AssertEqual(t, joinPath(dir, "files", "a.txt"), result.Results[0].Msg)
	core.AssertEqual(t, joinPath(dir, "files", "b.txt"), result.Results[1].Msg)
}

func TestExecutorExtra_HandleLookup_Bad_InvalidSyntax(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.handleLookup("lookup(invalid)", "", nil)
	core.AssertEqual(t, "", result)
}

// ============================================================
// Tests for SetInventory (0% coverage)
// ============================================================

func TestExecutorExtra_SetInventory_Good(t *core.T) {
	dir := t.TempDir()
	invPath := joinPath(dir, "inventory.yml")
	yaml := `all:
  hosts:
    web1:
      ansible_host: 10.0.0.1
    web2:
      ansible_host: 10.0.0.2
`
	core.RequireNoError(t, writeTestFile(invPath, []byte(yaml), 0644))

	e := NewExecutor(dir)
	err := e.SetInventory(invPath)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, e.inventory)
	core.AssertLen(t, e.inventory.All.Hosts, 2)
}

func TestExecutorExtra_SetInventory_Good_Directory(t *core.T) {
	dir := t.TempDir()
	inventoryDir := joinPath(dir, "inventory")
	core.RequireTrue(t, core.MkdirAll(inventoryDir, 0755).OK)

	invPath := joinPath(inventoryDir, "inventory.yml")
	yaml := `all:
  hosts:
    web1:
      ansible_host: 10.0.0.1
`
	core.RequireNoError(t, writeTestFile(invPath, []byte(yaml), 0644))

	e := NewExecutor(dir)
	err := e.SetInventory(inventoryDir)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, e.inventory)
	core.AssertContains(t, e.inventory.All.Hosts, "web1")
}

func TestExecutorExtra_SetInventory_Bad_FileNotFound(t *core.T) {
	e := NewExecutor("/tmp")
	err := e.SetInventory("/nonexistent/inventory.yml")
	core.AssertError(t, err)
}

// ============================================================
// Tests for iterator functions (0% coverage)
// ============================================================

func TestExecutorExtra_ParsePlaybookIter_Good(t *core.T) {
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
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	iter, err := p.ParsePlaybookIter(path)
	core.RequireNoError(t, err)

	var plays []Play
	for play := range iter {
		plays = append(plays, play)
	}
	core.AssertLen(t, plays, 2)
	core.AssertEqual(t, "First play", plays[0].Name)
	core.AssertEqual(t, "Second play", plays[1].Name)
}

func TestExecutorExtra_ParsePlaybookIter_Bad_InvalidFile(t *core.T) {
	parser := NewParser("/tmp")
	seq, err := parser.ParsePlaybookIter("/nonexistent.yml")
	core.AssertError(t, err)
	core.AssertNil(t, seq)
}

func TestExecutorExtra_ParseTasksIter_Good(t *core.T) {
	dir := t.TempDir()
	path := joinPath(dir, "tasks.yml")
	yaml := `- name: Task one
  debug:
    msg: first

- name: Task two
  debug:
    msg: second
`
	core.RequireNoError(t, writeTestFile(path, []byte(yaml), 0644))

	p := NewParser(dir)
	iter, err := p.ParseTasksIter(path)
	core.RequireNoError(t, err)

	var tasks []Task
	for task := range iter {
		tasks = append(tasks, task)
	}
	core.AssertLen(t, tasks, 2)
	core.AssertEqual(t, "Task one", tasks[0].Name)
}

func TestExecutorExtra_ParseTasksIter_Bad_InvalidFile(t *core.T) {
	parser := NewParser("/tmp")
	seq, err := parser.ParseTasksIter("/nonexistent.yml")
	core.AssertError(t, err)
	core.AssertNil(t, seq)
}

func TestExecutorExtra_RunIncludeTasks_Good_RelativePath(t *core.T) {
	dir := t.TempDir()
	includedPath := joinPath(dir, "included.yml")
	yaml := `- name: Included first task
  debug:
    msg: first

- name: Included second task
  debug:
    msg: second
`
	core.RequireNoError(t, writeTestFile(includedPath, []byte(yaml), 0644))

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

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertContains(t, started, "localhost:Included first task")
	core.AssertContains(t, started, "localhost:Included second task")
}

func TestExecutorExtra_RunIncludeTasks_Good_InheritsTaskVars(t *core.T) {
	dir := t.TempDir()
	includedPath := joinPath(dir, "included-vars.yml")
	yaml := `- name: Included var task
  debug:
    msg: "{{ include_message }}"
  register: included_result
`
	core.RequireNoError(t, writeTestFile(includedPath, []byte(yaml), 0644))

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
	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertNotNil(t, e.results["localhost"]["included_result"])
	core.AssertEqual(t, "hello from include", e.results["localhost"]["included_result"].Msg)
}

func TestExecutorExtra_RunIncludeTasks_Good_HostSpecificTemplate(t *core.T) {
	dir := t.TempDir()

	core.RequireNoError(t, writeTestFile(joinPath(dir, "web.yml"), []byte(`- name: Web included task
  debug:
    msg: web
`), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "db.yml"), []byte(`- name: DB included task
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

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"web1", "db1"}, &Task{
		Name:         "Load host-specific tasks",
		IncludeTasks: "{{ include_file }}",
	}, play))

	core.AssertContains(t, started, "web1:Web included task")
	core.AssertContains(t, started, "db1:DB included task")
}

func TestExecutorExtra_RunIncludeTasks_Good_HonoursWhen(t *core.T) {
	dir := t.TempDir()
	includedPath := joinPath(dir, "conditional.yml")
	yaml := `- name: Conditional included task
  debug:
    msg: should not run
`
	core.RequireNoError(t, writeTestFile(includedPath, []byte(yaml), 0644))

	gatherFacts := false
	play := &Play{
		Name:        "Conditional include",
		Hosts:       "localhost",
		GatherFacts: &gatherFacts,
	}

	e := NewExecutor(dir)
	e.SetVar("include_enabled", false)

	var started []string
	e.OnTaskStart = func(host string, task *Task) {
		started = append(started, host+":"+task.Name)
	}

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name:         "Load conditional tasks",
		IncludeTasks: "conditional.yml",
		When:         "include_enabled",
	}, play))

	core.AssertEmpty(t, started)
}

func TestExecutorExtra_RunIncludeTasks_Good_TemplatesVarsAndInheritsTags(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "tasks", "common.yml"), []byte(`---
- name: Included tagged task
  debug:
    msg: "{{ include_message }}"
  register: include_result
  tags:
    - child-tag
`), 0644))

	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {},
			},
		},
	})
	e.SetVar("env_name", "production")
	e.Tags = []string{"include-tag"}

	gatherFacts := false
	play := &Play{
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
	}

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name:         "Load included tasks",
		IncludeTasks: "tasks/common.yml",
		Tags:         []string{"include-tag"},
		Vars: map[string]any{
			"include_message": "{{ env_name }}",
		},
	}, play))

	core.AssertNotNil(t, e.results["localhost"]["include_result"])
	core.AssertEqual(t, "production", e.results["localhost"]["include_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_InheritsTaskVars(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "demo", "tasks", "main.yml"), []byte(`---
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

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name: "Load role",
		IncludeRole: &RoleRef{
			Role: "demo",
		},
		Vars: map[string]any{"role_message": "hello from role"},
	}, play))

	core.AssertNotNil(t, e.results["localhost"]["role_result"])
	core.AssertEqual(t, "hello from role", e.results["localhost"]["role_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_AppliesRoleDefaults(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "app", "tasks", "main.yml"), []byte(`---
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
	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
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

	core.AssertNotNil(t, started)
	core.AssertContains(t, started.Tags, "role-apply")
	core.AssertEqual(t, "production", started.Environment["APP_ENV"])
	core.AssertEqual(t, "from-apply", started.Vars["apply_message"])
	core.AssertEqual(t, "from-task", started.Vars["role_message"])
	core.AssertNotNil(t, e.results["localhost"]["role_result"])
	core.AssertEqual(t, "production|from-apply|from-task", e.results["localhost"]["role_result"].Stdout)
}

func TestExecutorExtra_RunIncludeRole_Good_AppliesRoleWhen(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "app", "tasks", "main.yml"), []byte(`---
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

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"localhost"}, &Task{
		Name: "Load role with conditional apply",
		IncludeRole: &RoleRef{
			Role: "app",
			Apply: &TaskApply{
				When: "apply_enabled",
			},
		},
	}, play))

	core.AssertNotNil(t, e.results["localhost"]["role_result"])
	core.AssertTrue(t, e.results["localhost"]["role_result"].Skipped)
	core.AssertEqual(t, "Skipped due to when condition", e.results["localhost"]["role_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_UsesRoleRefTagsForSelection(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "tagged", "tasks", "main.yml"), []byte(`---
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

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, &Task{
		Name: "Load tagged role",
		IncludeRole: &RoleRef{
			Role: "tagged",
			Tags: []string{"role-tag"},
		},
	}, play))

	core.AssertNotNil(t, e.results["host1"]["role_result"])
	core.AssertEqual(t, "role task ran", e.results["host1"]["role_result"].Msg)
}

func TestExecutorExtra_RunIncludeRole_Good_HonoursRoleRefWhen(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "conditional", "tasks", "main.yml"), []byte(`---
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

	core.RequireNoError(t, e.runTaskOnHosts(context.Background(), []string{"host1"}, &Task{
		Name: "Load conditional role",
		IncludeRole: &RoleRef{
			Role: "conditional",
			When: "role_enabled",
		},
	}, play))

	core.AssertEmpty(t, started)
	if results := e.results["host1"]; results != nil {
		_, ok := results["role_result"]
		core.AssertFalse(t, ok)
	}
}

func TestExecutorExtra_RunIncludeRole_Good_PublicVarsPersist(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "roles", "shared", "tasks", "main.yml"), []byte(`---
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

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertNotNil(t, e.results["localhost"]["shared_role_result"])
	core.AssertEqual(t, "hello from public role", e.results["localhost"]["shared_role_result"].Msg)
	core.AssertNotNil(t, e.results["localhost"]["after_public_role"])
	core.AssertEqual(t, "hello from public role", e.results["localhost"]["after_public_role"].Msg)
}

func TestExecutorExtra_RunPlay_Good_ModuleDefaultsApplyToTasks(t *core.T) {
	dir := t.TempDir()
	e := NewExecutor(dir)
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"localhost": {},
			},
		},
	})

	mock := newTrackingMockClient()
	e.clients["localhost"] = mock
	mock.expectCommand(`cd "/tmp/module-defaults" && pwd`, "/tmp/module-defaults\n", "", 0)

	gatherFacts := false
	play := &Play{
		Name:        "Module defaults",
		Hosts:       "localhost",
		Connection:  "local",
		GatherFacts: &gatherFacts,
		ModuleDefaults: map[string]map[string]any{
			"command": {
				"chdir": "/tmp/module-defaults",
			},
		},
		Tasks: []Task{
			{
				Name:     "Run command with defaults",
				Module:   "command",
				Args:     map[string]any{"cmd": "pwd"},
				Register: "command_result",
			},
		},
	}

	core.RequireNoError(t, e.runPlay(context.Background(), play))

	core.AssertNotNil(t, e.results["localhost"]["command_result"])
	core.AssertEqual(t, "/tmp/module-defaults\n", e.results["localhost"]["command_result"].Stdout)
	core.AssertTrue(t, mock.hasExecuted(`cd "/tmp/module-defaults" && pwd`))
}

func TestExecutorExtra_GetHostsIter_Good(t *core.T) {
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
	core.AssertLen(t, hosts, 3)
}

func TestExecutorExtra_AllHostsIter_Good(t *core.T) {
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
	core.AssertLen(t, hosts, 3)
	// AllHostsIter sorts keys
	core.AssertEqual(t, "alpha", hosts[0])
	core.AssertEqual(t, "beta", hosts[1])
	core.AssertEqual(t, "gamma", hosts[2])
}

func TestExecutorExtra_AllHostsIter_Good_NilGroup(t *core.T) {
	var count int
	for range AllHostsIter(nil) {
		count++
	}
	core.AssertEqual(t, 0, count)
}

// ============================================================
// Tests for resolveExpr with registered vars (additional coverage)
// ============================================================

func TestExecutorExtra_ResolveExpr_Good_RegisteredVarFields(t *core.T) {
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

	core.AssertEqual(t, "output text", e.resolveExpr("cmd_result.stdout", "host1", nil))
	core.AssertEqual(t, "error text", e.resolveExpr("cmd_result.stderr", "host1", nil))
	core.AssertEqual(t, "0", e.resolveExpr("cmd_result.rc", "host1", nil))
	core.AssertEqual(t, "true", e.resolveExpr("cmd_result.changed", "host1", nil))
	core.AssertEqual(t, "false", e.resolveExpr("cmd_result.failed", "host1", nil))
}

func TestExecutorExtra_ResolveExpr_Good_BareRegisteredVar(t *core.T) {
	e := NewExecutor("/tmp")
	result := &TaskResult{Stdout: "output text", RC: 0, Changed: true}
	e.results["host1"] = map[string]*TaskResult{
		"cmd_result": result,
	}

	value, ok := e.resolveExprValue("cmd_result", "host1", nil)

	core.RequireTrue(t, ok)
	core.AssertSame(t, result, value)
}

func TestExecutorExtra_ResolveExpr_Good_TaskVars(t *core.T) {
	e := NewExecutor("/tmp")
	task := &Task{
		Vars: map[string]any{"local_var": "local_value"},
	}

	result := e.resolveExpr("local_var", "host1", task)
	core.AssertEqual(t, "local_value", result)
}

func TestExecutorExtra_ResolveExpr_Good_HostVars(t *core.T) {
	e := NewExecutor("/tmp")
	e.SetInventoryDirect(&Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"host1": {AnsibleHost: "10.0.0.1"},
			},
		},
	})

	result := e.resolveExpr("ansible_host", "host1", nil)
	core.AssertEqual(t, "10.0.0.1", result)
}

func TestExecutorExtra_ResolveExpr_Good_Facts(t *core.T) {
	e := NewExecutor("/tmp")
	e.facts["host1"] = &Facts{
		Hostname:     "web01",
		FQDN:         "web01.example.com",
		Distribution: "ubuntu",
		Version:      "22.04",
		Architecture: "x86_64",
		Kernel:       "5.15.0",
	}

	core.AssertEqual(t, "web01", e.resolveExpr("ansible_hostname", "host1", nil))
	core.AssertEqual(t, "web01.example.com", e.resolveExpr("ansible_fqdn", "host1", nil))
	core.AssertEqual(t, "ubuntu", e.resolveExpr("ansible_distribution", "host1", nil))
	core.AssertEqual(t, "22.04", e.resolveExpr("ansible_distribution_version", "host1", nil))
	core.AssertEqual(t, "x86_64", e.resolveExpr("ansible_architecture", "host1", nil))
	core.AssertEqual(t, "5.15.0", e.resolveExpr("ansible_kernel", "host1", nil))
}

// --- applyFilter additional coverage ---

func TestExecutorExtra_ApplyFilter_Good_B64Decode(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.applyFilter("aGVsbG8=", "b64decode")
	core.AssertEqual(t, "hello", result)
}

func TestExecutorExtra_ApplyFilter_Good_B64Encode(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.applyFilter("hello", "b64encode")
	core.AssertEqual(t, "aGVsbG8=", result)
}

func TestExecutorExtra_ApplyFilter_Good_UnknownFilter(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.applyFilter("value", "unknown_filter")
	core.AssertEqual(t, "value", result)
}

// --- evalCondition with default filter ---

func TestExecutorExtra_EvalCondition_Good_DefaultFilter(t *core.T) {
	e := NewExecutor("/tmp")
	result := e.evalCondition("myvar | default('fallback')", "host1")
	core.AssertTrue(t, result)
}

func TestExecutorExtra_EvalCondition_Good_UndefinedCheck(t *core.T) {
	e := NewExecutor("/tmp")
	core.AssertTrue(t, e.evalCondition("missing_var is not defined", "host1"))
	core.AssertTrue(t, e.evalCondition("missing_var is undefined", "host1"))
}

func TestExecutorExtra_EvalCondition_Good_BinaryOperators(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["count"] = 2
	e.vars["limit"] = 5
	e.vars["roles"] = []string{"web", "api"}

	core.AssertTrue(t, e.evalCondition("count < limit", "host1"))
	core.AssertTrue(t, e.evalCondition("count <= 2", "host1"))
	core.AssertTrue(t, e.evalCondition("count<=2", "host1"))
	core.AssertTrue(t, e.evalCondition("count!=limit", "host1"))
	core.AssertTrue(t, e.evalCondition("limit >= count", "host1"))
	core.AssertTrue(t, e.evalCondition("'web' in roles", "host1"))
	core.AssertTrue(t, e.evalCondition("roles contains 'api'", "host1"))
	core.AssertTrue(t, e.evalCondition("'db' not in roles", "host1"))
	core.AssertFalse(t, e.evalCondition("count > limit", "host1"))
	core.AssertFalse(t, e.evalCondition("count < missing_limit", "host1"))
}

// --- resolveExpr with filter pipe ---

func TestExecutorExtra_ResolveExpr_Good_WithFilter(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["raw_value"] = "  trimmed  "

	result := e.resolveExpr("raw_value | trim", "host1", nil)
	core.AssertEqual(t, "trimmed", result)
}

func TestExecutorExtra_ResolveExpr_Good_CommonFilters(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["name"] = "Hello"
	e.vars[pathArgKey] = "/tmp/example/nginx.conf"
	e.vars["parts"] = []string{"a", "b"}
	e.vars["csv"] = "a,b,c"
	e.vars["count"] = "42"
	e.vars["negative"] = -7
	e.vars["values"] = []any{3, 1, 2}

	core.AssertEqual(t, "HELLO", e.resolveExpr("name | upper", "host1", nil))
	core.AssertEqual(t, "hello", e.resolveExpr("name | lower", "host1", nil))
	core.AssertEqual(t, "nginx.conf", e.resolveExpr("path | basename", "host1", nil))
	core.AssertEqual(t, "/tmp/example", e.resolveExpr("path | dirname", "host1", nil))
	core.AssertEqual(t, "a,b", e.resolveExpr("parts | join(',')", "host1", nil))
	core.AssertEqual(t, "[a b c]", e.resolveExpr("csv | split(',')", "host1", nil))
	core.AssertEqual(t, "42", e.resolveExpr("count | int", "host1", nil))
	core.AssertEqual(t, "7", e.resolveExpr("negative | abs", "host1", nil))
	core.AssertEqual(t, "1", e.resolveExpr("values | min", "host1", nil))
	core.AssertEqual(t, "3", e.resolveExpr("values | max", "host1", nil))
	core.AssertEqual(t, "3", e.resolveExpr("values | length", "host1", nil))
}

func TestExecutorExtra_ResolveExpr_Good_WithB64Encode(t *core.T) {
	e := NewExecutor("/tmp")
	e.vars["raw_value"] = "hello"

	result := e.resolveExpr("raw_value | b64encode", "host1", nil)
	core.AssertEqual(t, "aGVsbG8=", result)
}
