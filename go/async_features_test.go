package ansible

import (
	core "dappco.re/go"
)

// --- newAsyncJobID ---

func TestAsyncFeatures_NewAsyncJobID_Good_UniqueHex(t *core.T) {
	a := newAsyncJobID()
	b := newAsyncJobID()
	core.AssertNotEmpty(t, a)
	core.AssertNotEmpty(t, b)
	// Two consecutive ids should not collide.
	core.AssertNotEqual(t, a, b)
}

// --- cloneAnyValue ---

func TestAsyncFeatures_CloneAnyValue_Good_Scalars(t *core.T) {
	core.AssertNil(t, cloneAnyValue(nil))
	core.AssertEqual(t, "x", cloneAnyValue("x"))
	core.AssertEqual(t, 7, cloneAnyValue(7))
	core.AssertEqual(t, true, cloneAnyValue(true))
	core.AssertEqual(t, 1.5, cloneAnyValue(1.5))
}

func TestAsyncFeatures_CloneAnyValue_Ugly_NestedCollectionsAreIndependent(t *core.T) {
	src := map[string]any{
		"list": []any{1, map[string]any{"deep": "v"}},
		"strs": []string{"a", "b"},
		"anys": map[any]any{1: "one"},
	}
	clone, ok := cloneAnyValue(src).(map[string]any)
	core.AssertTrue(t, ok)

	// Mutate the clone's nested structures.
	clone["list"].([]any)[0] = 99
	clone["strs"].([]string)[0] = "z"

	// Source remains untouched.
	core.AssertEqual(t, 1, src["list"].([]any)[0])
	core.AssertEqual(t, "a", src["strs"].([]string)[0])
}

func TestAsyncFeatures_CloneAnyValue_Bad_UnknownTypePassthrough(t *core.T) {
	type custom struct{ n int }
	in := custom{n: 3}
	out := cloneAnyValue(in)
	core.AssertEqual(t, in, out)
}

// --- cloneAnyMap ---

func TestAsyncFeatures_CloneAnyMap_Good_IndependentCopy(t *core.T) {
	src := map[string]any{"k": "v", "nested": map[string]any{"x": 1}}
	clone := cloneAnyMap(src)
	core.AssertEqual(t, "v", clone["k"])

	clone["nested"].(map[string]any)["x"] = 42
	core.AssertEqual(t, 1, src["nested"].(map[string]any)["x"])
}

func TestAsyncFeatures_CloneAnyMap_Bad_EmptyReturnsNil(t *core.T) {
	core.AssertNil(t, cloneAnyMap(nil))
	core.AssertNil(t, cloneAnyMap(map[string]any{}))
}

// --- cloneStringMap / cloneBoolMap ---

func TestAsyncFeatures_CloneStringMap_Good_IndependentCopy(t *core.T) {
	src := map[string]string{"PATH": "/usr/bin"}
	clone := cloneStringMap(src)
	clone["PATH"] = "/bin"
	core.AssertEqual(t, "/usr/bin", src["PATH"])
}

func TestAsyncFeatures_CloneStringMap_Bad_EmptyReturnsNil(t *core.T) {
	core.AssertNil(t, cloneStringMap(nil))
	core.AssertNil(t, cloneStringMap(map[string]string{}))
}

func TestAsyncFeatures_CloneBoolMap_Good_IndependentCopy(t *core.T) {
	src := map[string]bool{"web1": true}
	clone := cloneBoolMap(src)
	clone["web1"] = false
	core.AssertTrue(t, src["web1"])
}

func TestAsyncFeatures_CloneBoolMap_Bad_EmptyReturnsNil(t *core.T) {
	core.AssertNil(t, cloneBoolMap(nil))
	core.AssertNil(t, cloneBoolMap(map[string]bool{}))
}

// --- cloneTaskForAsync / cloneTaskSlice ---

func TestAsyncFeatures_CloneTaskForAsync_Good_DeepCopyMutableFields(t *core.T) {
	src := &Task{
		Name:        "install",
		Module:      "apt",
		Args:        map[string]any{"name": "nginx"},
		Vars:        map[string]any{"v": 1},
		Environment: map[string]string{"E": "1"},
		Tags:        []string{"deploy"},
		Block:       []Task{{Name: "b1"}},
		Loop:        []any{"a", "b"},
		IncludeRole: &RoleRef{Role: "web"},
		Apply:       &TaskApply{Tags: []string{"t"}},
		LoopControl: &LoopControl{LoopVar: "item"},
	}
	clone := cloneTaskForAsync(src)
	core.AssertEqual(t, "install", clone.Name)
	core.AssertEqual(t, "apt", clone.Module)

	// Mutate clone collections; source unchanged.
	clone.Args["name"] = "apache"
	clone.Tags[0] = "x"
	clone.IncludeRole.Role = "db"
	core.AssertEqual(t, "nginx", src.Args["name"])
	core.AssertEqual(t, "deploy", src.Tags[0])
	core.AssertEqual(t, "web", src.IncludeRole.Role)
}

func TestAsyncFeatures_CloneTaskForAsync_Bad_NilReturnsZero(t *core.T) {
	clone := cloneTaskForAsync(nil)
	core.AssertEqual(t, "", clone.Name)
	core.AssertNil(t, clone.Args)
}

func TestAsyncFeatures_CloneTaskSlice_Ugly_EmptyAndPopulated(t *core.T) {
	core.AssertNil(t, cloneTaskSlice(nil))
	core.AssertNil(t, cloneTaskSlice([]Task{}))

	src := []Task{{Name: "one", Args: map[string]any{"k": "v"}}, {Name: "two"}}
	clone := cloneTaskSlice(src)
	core.AssertLen(t, clone, 2)
	clone[0].Args["k"] = "changed"
	core.AssertEqual(t, "v", src[0].Args["k"])
}

// --- cloneRoleRef family ---

func TestAsyncFeatures_CloneRoleRef_Good_DeepCopy(t *core.T) {
	src := RoleRef{
		Role:  "web",
		Vars:  map[string]any{"port": 80},
		Tags:  []string{"deploy"},
		Apply: &TaskApply{Tags: []string{"t"}},
		When:  []any{"cond"},
	}
	clone := cloneRoleRef(src)
	clone.Vars["port"] = 8080
	clone.Tags[0] = "x"
	core.AssertEqual(t, 80, src.Vars["port"])
	core.AssertEqual(t, "deploy", src.Tags[0])
}

func TestAsyncFeatures_CloneRoleRefPtr_Bad_NilAndPopulated(t *core.T) {
	core.AssertNil(t, cloneRoleRefPtr(nil))

	src := &RoleRef{Role: "web", Vars: map[string]any{"a": 1}}
	clone := cloneRoleRefPtr(src)
	core.AssertNotNil(t, clone)
	clone.Vars["a"] = 2
	core.AssertEqual(t, 1, src.Vars["a"])
}

func TestAsyncFeatures_CloneRoleRefSlice_Ugly_EmptyAndPopulated(t *core.T) {
	core.AssertNil(t, cloneRoleRefSlice(nil))
	clone := cloneRoleRefSlice([]RoleRef{{Role: "a"}, {Role: "b"}})
	core.AssertLen(t, clone, 2)
	core.AssertEqual(t, "a", clone[0].Role)
}

// --- cloneTaskApplyPtr / cloneLoopControlPtr ---

func TestAsyncFeatures_CloneTaskApplyPtr_Good_DeepCopy(t *core.T) {
	src := &TaskApply{
		Tags:        []string{"deploy"},
		Vars:        map[string]any{"v": 1},
		Environment: map[string]string{"E": "1"},
		When:        []any{"cond"},
	}
	clone := cloneTaskApplyPtr(src)
	clone.Tags[0] = "x"
	clone.Vars["v"] = 2
	core.AssertEqual(t, "deploy", src.Tags[0])
	core.AssertEqual(t, 1, src.Vars["v"])
}

func TestAsyncFeatures_CloneTaskApplyPtr_Bad_Nil(t *core.T) {
	core.AssertNil(t, cloneTaskApplyPtr(nil))
}

func TestAsyncFeatures_CloneLoopControlPtr_Good_ValueCopy(t *core.T) {
	src := &LoopControl{LoopVar: "item", IndexVar: "idx"}
	clone := cloneLoopControlPtr(src)
	core.AssertNotNil(t, clone)
	clone.LoopVar = "changed"
	core.AssertEqual(t, "item", src.LoopVar)
}

func TestAsyncFeatures_CloneLoopControlPtr_Bad_Nil(t *core.T) {
	core.AssertNil(t, cloneLoopControlPtr(nil))
}

// --- cloneModuleDefaults / cloneHostVarsMap ---

func TestAsyncFeatures_CloneModuleDefaults_Good_IndependentNested(t *core.T) {
	src := map[string]map[string]any{"apt": {"update_cache": true}}
	clone := cloneModuleDefaults(src)
	clone["apt"]["update_cache"] = false
	core.AssertEqual(t, true, src["apt"]["update_cache"])
}

func TestAsyncFeatures_CloneModuleDefaults_Bad_EmptyNil(t *core.T) {
	core.AssertNil(t, cloneModuleDefaults(nil))
	core.AssertNil(t, cloneModuleDefaults(map[string]map[string]any{}))
}

func TestAsyncFeatures_CloneHostVarsMap_Ugly_IndependentNested(t *core.T) {
	core.AssertNil(t, cloneHostVarsMap(nil))
	src := map[string]map[string]any{"web1": {"port": 80}}
	clone := cloneHostVarsMap(src)
	clone["web1"]["port"] = 443
	core.AssertEqual(t, 80, src["web1"]["port"])
}

// --- cloneInventory / cloneInventoryGroup / cloneHost ---

func TestAsyncFeatures_CloneInventory_Good_DeepCopyTree(t *core.T) {
	src := &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{"web1": {AnsibleHost: "10.0.0.1", Vars: map[string]any{"role": "web"}}},
			Children: map[string]*InventoryGroup{
				"db": {Hosts: map[string]*Host{"db1": {AnsibleHost: "10.0.0.2"}}},
			},
			Vars: map[string]any{"env": "prod"},
		},
		HostVars: map[string]map[string]any{"web1": {"x": 1}},
	}
	clone := cloneInventory(src)
	core.AssertNotNil(t, clone)
	core.AssertEqual(t, "10.0.0.1", clone.All.Hosts["web1"].AnsibleHost)

	// Mutate clone tree; source unchanged.
	clone.All.Hosts["web1"].Vars["role"] = "db"
	clone.All.Vars["env"] = "dev"
	clone.All.Children["db"].Hosts["db1"].AnsibleHost = "9.9.9.9"
	core.AssertEqual(t, "web", src.All.Hosts["web1"].Vars["role"])
	core.AssertEqual(t, "prod", src.All.Vars["env"])
	core.AssertEqual(t, "10.0.0.2", src.All.Children["db"].Hosts["db1"].AnsibleHost)
}

func TestAsyncFeatures_CloneInventory_Bad_Nil(t *core.T) {
	core.AssertNil(t, cloneInventory(nil))
	core.AssertNil(t, cloneInventoryGroup(nil))
	core.AssertNil(t, cloneHost(nil))
}

func TestAsyncFeatures_CloneHost_Ugly_VarsIndependent(t *core.T) {
	src := &Host{AnsibleHost: "1.1.1.1", Vars: map[string]any{"k": "v"}}
	clone := cloneHost(src)
	clone.Vars["k"] = "changed"
	clone.AnsibleHost = "2.2.2.2"
	core.AssertEqual(t, "v", src.Vars["k"])
	core.AssertEqual(t, "1.1.1.1", src.AnsibleHost)
}

// --- cloneFactsMap ---

func TestAsyncFeatures_CloneFactsMap_Good_IndependentSnapshots(t *core.T) {
	src := map[string]*Facts{"web1": {Hostname: "web1", CPUs: 4}}
	clone := cloneFactsMap(src)
	clone["web1"].CPUs = 8
	core.AssertEqual(t, 4, src["web1"].CPUs)
}

func TestAsyncFeatures_CloneFactsMap_Ugly_NilEntryPreserved(t *core.T) {
	core.AssertNil(t, cloneFactsMap(nil))
	src := map[string]*Facts{"web1": nil, "web2": {Hostname: "web2"}}
	clone := cloneFactsMap(src)
	core.AssertNil(t, clone["web1"])
	core.AssertEqual(t, "web2", clone["web2"].Hostname)
}

// --- cloneResultsMap / cloneTaskResult / cloneTaskHandlersMap ---

func TestAsyncFeatures_CloneTaskResult_Good_NestedLoopResults(t *core.T) {
	src := &TaskResult{
		Changed: true,
		Data:    map[string]any{"k": "v"},
		Results: []TaskResult{{Changed: true, Data: map[string]any{"inner": 1}}},
	}
	clone := cloneTaskResult(src)
	core.AssertNotNil(t, clone)
	clone.Data["k"] = "changed"
	clone.Results[0].Data["inner"] = 99
	core.AssertEqual(t, "v", src.Data["k"])
	core.AssertEqual(t, 1, src.Results[0].Data["inner"])
}

func TestAsyncFeatures_CloneTaskResult_Bad_Nil(t *core.T) {
	core.AssertNil(t, cloneTaskResult(nil))
}

func TestAsyncFeatures_CloneResultsMap_Ugly_EmptyHostSkipped(t *core.T) {
	core.AssertNil(t, cloneResultsMap(nil))
	src := map[string]map[string]*TaskResult{
		"web1":  {"r1": {Changed: true}},
		"empty": {},
	}
	clone := cloneResultsMap(src)
	core.AssertNotNil(t, clone["web1"])
	_, hasEmpty := clone["empty"]
	core.AssertFalse(t, hasEmpty)
}

func TestAsyncFeatures_CloneTaskHandlersMap_Good_IndependentSlices(t *core.T) {
	core.AssertNil(t, cloneTaskHandlersMap(nil))
	src := map[string][]Task{"restart nginx": {{Name: "h1", Args: map[string]any{"k": "v"}}}}
	clone := cloneTaskHandlersMap(src)
	clone["restart nginx"][0].Args["k"] = "changed"
	core.AssertEqual(t, "v", src["restart nginx"][0].Args["k"])
}

// --- clonePlayForAsync ---

func TestAsyncFeatures_ClonePlayForAsync_Good_DeepCopyFields(t *core.T) {
	gather := true
	src := &Play{
		Name:           "deploy",
		Hosts:          "web",
		GatherFacts:    &gather,
		Vars:           map[string]any{"v": 1},
		ModuleDefaults: map[string]map[string]any{"apt": {"x": 1}},
		PreTasks:       []Task{{Name: "pre"}},
		Tasks:          []Task{{Name: "t", Args: map[string]any{"k": "v"}}},
		PostTasks:      []Task{{Name: "post"}},
		Roles:          []RoleRef{{Role: "web"}},
		Handlers:       []Task{{Name: "h"}},
		Tags:           []string{"deploy"},
		Environment:    map[string]string{"E": "1"},
		Serial:         []any{1, 2},
		VarsFiles:      []any{"vars.yml"},
	}
	clone := clonePlayForAsync(src)
	core.AssertNotNil(t, clone)
	core.AssertEqual(t, "deploy", clone.Name)

	// GatherFacts pointer is a distinct copy.
	*clone.GatherFacts = false
	core.AssertTrue(t, *src.GatherFacts)

	clone.Vars["v"] = 2
	clone.Tasks[0].Args["k"] = "changed"
	clone.Tags[0] = "x"
	core.AssertEqual(t, 1, src.Vars["v"])
	core.AssertEqual(t, "v", src.Tasks[0].Args["k"])
	core.AssertEqual(t, "deploy", src.Tags[0])
}

func TestAsyncFeatures_ClonePlayForAsync_Bad_Nil(t *core.T) {
	core.AssertNil(t, clonePlayForAsync(nil))
}

func TestAsyncFeatures_ClonePlayForAsync_Ugly_NilOptionalPointers(t *core.T) {
	src := &Play{Name: "minimal", Hosts: "all"}
	clone := clonePlayForAsync(src)
	core.AssertNotNil(t, clone)
	core.AssertNil(t, clone.GatherFacts)
	core.AssertNil(t, clone.VarsFiles)
	core.AssertEqual(t, "minimal", clone.Name)
}

// --- cloneParser / cloneAsyncExecutor ---

func TestAsyncFeatures_CloneParser_Good_IndependentVars(t *core.T) {
	src := NewParser("/workspace")
	src.vars = map[string]any{"k": "v"}
	clone := cloneParser(src)
	core.AssertNotNil(t, clone)
	clone.vars["k"] = "changed"
	core.AssertEqual(t, "v", src.vars["k"])
}

func TestAsyncFeatures_CloneParser_Bad_Nil(t *core.T) {
	core.AssertNil(t, cloneParser(nil))
}

func TestAsyncFeatures_CloneAsyncExecutor_Good_IndependentMaps(t *core.T) {
	e := &Executor{
		vars:      map[string]any{"v": 1},
		notified:  map[string]bool{"h": true},
		Tags:      []string{"deploy"},
		Limit:     "web",
		CheckMode: true,
	}
	clone := e.cloneAsyncExecutor()
	core.AssertNotNil(t, clone)
	core.AssertEqual(t, "web", clone.Limit)
	core.AssertTrue(t, clone.CheckMode)

	clone.vars["v"] = 2
	clone.Tags[0] = "x"
	core.AssertEqual(t, 1, e.vars["v"])
	core.AssertEqual(t, "deploy", e.Tags[0])
}

func TestAsyncFeatures_CloneAsyncExecutor_Bad_Nil(t *core.T) {
	var e *Executor
	core.AssertNil(t, e.cloneAsyncExecutor())
}
