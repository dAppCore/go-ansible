package ansible

import (
	core "dappco.re/go"
)

// --- parseActionSpec / parseActionSpecString ---

func TestParserHelpers_ParseActionSpec_Good_StringWithArgs(t *core.T) {
	module, args := parseActionSpec("apt name=nginx state=present")
	core.AssertEqual(t, "apt", module)
	core.AssertEqual(t, "nginx", args["name"])
	core.AssertEqual(t, "present", args["state"])
}

func TestParserHelpers_ParseActionSpec_Good_MapForm(t *core.T) {
	module, args := parseActionSpec(map[string]any{
		"module": "service",
		"name":   "nginx",
		"state":  "started",
	})
	core.AssertEqual(t, "service", module)
	core.AssertEqual(t, "nginx", args["name"])
	core.AssertEqual(t, "started", args["state"])
}

func TestParserHelpers_ParseActionSpec_Bad_EmptyAndNoModule(t *core.T) {
	module, args := parseActionSpec("")
	core.AssertEqual(t, "", module)
	core.AssertNil(t, args)

	module, _ = parseActionSpec(map[string]any{"name": "nginx"})
	core.AssertEqual(t, "", module)
}

func TestParserHelpers_ParseActionSpec_Ugly_FreeFormRawParams(t *core.T) {
	module, args := parseActionSpec("command echo hello world")
	core.AssertEqual(t, "command", module)
	core.AssertEqual(t, "echo hello world", args["_raw_params"])
}

func TestParserHelpers_ParseActionSpec_Ugly_ModuleOnly(t *core.T) {
	module, args := parseActionSpec("ping")
	core.AssertEqual(t, "ping", module)
	core.AssertNil(t, args)
}

// --- nestedLoopGroups / nestedLoopItems ---

func TestParserHelpers_NestedLoopGroups_Good_NestedSlices(t *core.T) {
	groups, ok := nestedLoopGroups([]any{[]any{"a", "b"}, []any{1, 2}})
	core.AssertTrue(t, ok)
	core.AssertLen(t, groups, 2)
	core.AssertLen(t, groups[0], 2)
}

func TestParserHelpers_NestedLoopGroups_Bad_EmptyGroupFails(t *core.T) {
	_, ok := nestedLoopGroups([]any{[]any{}})
	core.AssertFalse(t, ok)
	_, ok = nestedLoopGroups("")
	core.AssertFalse(t, ok)
}

func TestParserHelpers_NestedLoopGroups_Ugly_StringAndScalar(t *core.T) {
	groups, ok := nestedLoopGroups("solo")
	core.AssertTrue(t, ok)
	core.AssertLen(t, groups, 1)
	core.AssertEqual(t, "solo", groups[0][0])

	groups, ok = nestedLoopGroups([]string{"x", "y"})
	core.AssertTrue(t, ok)
	core.AssertLen(t, groups, 1)
	core.AssertLen(t, groups[0], 2)
}

func TestParserHelpers_NestedLoopItems_Ugly_KindCoverage(t *core.T) {
	core.AssertNil(t, nestedLoopItems(nil))
	core.AssertLen(t, nestedLoopItems([]any{1, 2}), 2)
	core.AssertLen(t, nestedLoopItems([]string{"a"}), 1)
	core.AssertLen(t, nestedLoopItems(42), 1)
}

// --- togetherLoopGroups ---

func TestParserHelpers_TogetherLoopGroups_Good_ParallelSlices(t *core.T) {
	groups, ok := togetherLoopGroups([]any{[]any{"a", "b"}, []any{"1", "2"}})
	core.AssertTrue(t, ok)
	core.AssertLen(t, groups, 2)
}

func TestParserHelpers_TogetherLoopGroups_Bad_EmptyAndBlankString(t *core.T) {
	_, ok := togetherLoopGroups([]any{[]any{}})
	core.AssertFalse(t, ok)
	_, ok = togetherLoopGroups("")
	core.AssertFalse(t, ok)
}

func TestParserHelpers_TogetherLoopGroups_Ugly_StringSliceAndScalar(t *core.T) {
	groups, ok := togetherLoopGroups([]string{"x", "y"})
	core.AssertTrue(t, ok)
	core.AssertLen(t, groups[0], 2)

	groups, ok = togetherLoopGroups(99)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 99, groups[0][0])
}
