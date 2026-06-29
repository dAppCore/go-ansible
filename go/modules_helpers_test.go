package ansible

import (
	core "dappco.re/go"
)

// --- getIntArg ---

func TestModulesHelpers_GetIntArg_Good_NumericKinds(t *core.T) {
	core.AssertEqual(t, 5, getIntArg(map[string]any{"n": 5}, "n", 0))
	core.AssertEqual(t, 9, getIntArg(map[string]any{"n": int64(9)}, "n", 0))
	core.AssertEqual(t, 3, getIntArg(map[string]any{"n": uint8(3)}, "n", 0))
}

func TestModulesHelpers_GetIntArg_Bad_MissingUsesDefault(t *core.T) {
	core.AssertEqual(t, 42, getIntArg(map[string]any{}, "n", 42))
	core.AssertEqual(t, 7, getIntArg(map[string]any{"n": "not-int"}, "n", 7))
}

func TestModulesHelpers_GetIntArg_Ugly_StringParsed(t *core.T) {
	core.AssertEqual(t, 12, getIntArg(map[string]any{"n": "12"}, "n", 0))
}

// --- normalizeStatusCodes ---

func TestModulesHelpers_NormalizeStatusCodes_Good_ScalarAndSlice(t *core.T) {
	core.AssertElementsMatch(t, []int{200}, normalizeStatusCodes(200, 0))
	core.AssertElementsMatch(t, []int{200, 201}, normalizeStatusCodes([]int{200, 201}, 0))
}

func TestModulesHelpers_NormalizeStatusCodes_Bad_NilUsesDefault(t *core.T) {
	core.AssertElementsMatch(t, []int{200}, normalizeStatusCodes(nil, 200))
}

func TestModulesHelpers_NormalizeStatusCodes_Ugly_MixedAnySliceAndStrings(t *core.T) {
	core.AssertElementsMatch(t, []int{200, 404}, normalizeStatusCodes([]any{200, "404"}, 0))
	core.AssertElementsMatch(t, []int{301, 302}, normalizeStatusCodes([]string{"301", "302"}, 0))
	core.AssertElementsMatch(t, []int{500}, normalizeStatusCodes("500", 0))
}

// --- normalizeStringList ---

func TestModulesHelpers_NormalizeStringList_Good_CommaSplit(t *core.T) {
	core.AssertElementsMatch(t, []string{"a", "b", "c"}, normalizeStringList("a, b ,c"))
}

func TestModulesHelpers_NormalizeStringList_Bad_NilAndEmpty(t *core.T) {
	core.AssertNil(t, normalizeStringList(nil))
	core.AssertNil(t, normalizeStringList(""))
}

func TestModulesHelpers_NormalizeStringList_Ugly_SliceAndDefault(t *core.T) {
	core.AssertElementsMatch(t, []string{"x", "y"}, normalizeStringList([]string{"x", " y ", ""}))
	// A non-string scalar is stringified into a single-element list.
	core.AssertElementsMatch(t, []string{"7"}, normalizeStringList(7))
}

// --- normalizeStringArgs ---

func TestModulesHelpers_NormalizeStringArgs_Good_NoSplitOnComma(t *core.T) {
	// Unlike normalizeStringList, a comma string is kept whole.
	core.AssertElementsMatch(t, []string{"a,b"}, normalizeStringArgs("a,b"))
}

func TestModulesHelpers_NormalizeStringArgs_Bad_NilAndBlank(t *core.T) {
	core.AssertNil(t, normalizeStringArgs(nil))
	core.AssertNil(t, normalizeStringArgs("   "))
}

func TestModulesHelpers_NormalizeStringArgs_Ugly_AnySliceMixedTypes(t *core.T) {
	core.AssertElementsMatch(t, []string{"a", "3"}, normalizeStringArgs([]any{"a", 3, nil}))
}

// --- osFamilyFromReleaseID ---

func TestModulesHelpers_OSFamilyFromReleaseID_Good_KnownFamilies(t *core.T) {
	core.AssertEqual(t, "Debian", osFamilyFromReleaseID("ubuntu"))
	core.AssertEqual(t, "RedHat", osFamilyFromReleaseID("CentOS"))
	core.AssertEqual(t, "Archlinux", osFamilyFromReleaseID(" arch "))
	core.AssertEqual(t, "Alpine", osFamilyFromReleaseID("alpine"))
}

func TestModulesHelpers_OSFamilyFromReleaseID_Bad_UnknownEmpty(t *core.T) {
	core.AssertEqual(t, "", osFamilyFromReleaseID("plan9"))
	core.AssertEqual(t, "", osFamilyFromReleaseID(""))
}

// --- templateConditionEqual / Compare / Contains ---

func TestModulesHelpers_TemplateConditionEqual_Good_NumericAndString(t *core.T) {
	core.AssertTrue(t, templateConditionEqual(1, 1.0))
	core.AssertTrue(t, templateConditionEqual("x", "x"))
	core.AssertFalse(t, templateConditionEqual("x", "y"))
}

func TestModulesHelpers_TemplateConditionCompare_Good_NumericOrder(t *core.T) {
	core.AssertEqual(t, -1, templateConditionCompare(1, 2))
	core.AssertEqual(t, 1, templateConditionCompare(3, 2))
	core.AssertEqual(t, 0, templateConditionCompare(2, 2))
}

func TestModulesHelpers_TemplateConditionCompare_Ugly_LexicalFallback(t *core.T) {
	core.AssertEqual(t, -1, templateConditionCompare("alpha", "beta"))
	core.AssertEqual(t, 1, templateConditionCompare("beta", "alpha"))
	core.AssertEqual(t, 0, templateConditionCompare("same", "same"))
}

func TestModulesHelpers_TemplateConditionContains_Good_StringAndSlice(t *core.T) {
	core.AssertTrue(t, templateConditionContains("hello world", "world"))
	core.AssertTrue(t, templateConditionContains([]any{"a", "b"}, "b"))
	core.AssertFalse(t, templateConditionContains([]any{"a"}, "z"))
}

func TestModulesHelpers_TemplateConditionContains_Bad_NilContainer(t *core.T) {
	core.AssertFalse(t, templateConditionContains(nil, "x"))
}

func TestModulesHelpers_TemplateConditionContains_Ugly_MapKeys(t *core.T) {
	core.AssertTrue(t, templateConditionContains(map[string]any{"key": 1}, "key"))
	core.AssertFalse(t, templateConditionContains(map[string]any{"key": 1}, "missing"))
}
