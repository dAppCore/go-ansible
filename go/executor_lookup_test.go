package ansible

import (
	core "dappco.re/go"
)

// --- isEmptyLoopValue ---

func TestExecutorLookup_IsEmptyLoopValue_Good_EmptyKinds(t *core.T) {
	core.AssertTrue(t, isEmptyLoopValue(nil))
	core.AssertTrue(t, isEmptyLoopValue("  "))
	core.AssertTrue(t, isEmptyLoopValue([]any{}))
	core.AssertTrue(t, isEmptyLoopValue([]string{}))
	core.AssertTrue(t, isEmptyLoopValue(map[string]any{}))
}

func TestExecutorLookup_IsEmptyLoopValue_Bad_NonEmpty(t *core.T) {
	core.AssertFalse(t, isEmptyLoopValue("x"))
	core.AssertFalse(t, isEmptyLoopValue([]any{1}))
	core.AssertFalse(t, isEmptyLoopValue(42))
}

func TestExecutorLookup_IsEmptyLoopValue_Ugly_ReflectKinds(t *core.T) {
	core.AssertTrue(t, isEmptyLoopValue([]int{}))
	core.AssertFalse(t, isEmptyLoopValue([]int{1}))
	core.AssertTrue(t, isEmptyLoopValue(map[int]int{}))
}

// --- firstFoundTerms ---

func TestExecutorLookup_FirstFoundTerms_Good_StringAndSlice(t *core.T) {
	files, paths := firstFoundTerms("a,b")
	core.AssertElementsMatch(t, []string{"a", "b"}, files)
	core.AssertNil(t, paths)

	files, _ = firstFoundTerms([]string{"x", "y"})
	core.AssertElementsMatch(t, []string{"x", "y"}, files)
}

func TestExecutorLookup_FirstFoundTerms_Bad_Nil(t *core.T) {
	files, paths := firstFoundTerms(nil)
	core.AssertNil(t, files)
	core.AssertNil(t, paths)
}

func TestExecutorLookup_FirstFoundTerms_Ugly_MapFilesAndPaths(t *core.T) {
	files, paths := firstFoundTerms(map[string]any{
		"files": []any{"a.conf", "b.conf"},
		"paths": []any{"/etc", "/opt"},
	})
	core.AssertElementsMatch(t, []string{"a.conf", "b.conf"}, files)
	core.AssertElementsMatch(t, []string{"/etc", "/opt"}, paths)

	// Falls back to "terms" when "files" is absent.
	files, _ = firstFoundTerms(map[any]any{"terms": "only.conf"})
	core.AssertElementsMatch(t, []string{"only.conf"}, files)
}

// --- commandArgv ---

func TestExecutorLookup_CommandArgv_Good_StringSlice(t *core.T) {
	out := commandArgv(map[string]any{"argv": []string{"ls", "-l", ""}})
	core.AssertElementsMatch(t, []string{"ls", "-l"}, out)
}

func TestExecutorLookup_CommandArgv_Bad_MissingKey(t *core.T) {
	core.AssertNil(t, commandArgv(map[string]any{}))
	core.AssertNil(t, commandArgv(map[string]any{"argv": ""}))
}

func TestExecutorLookup_CommandArgv_Ugly_AnySliceAndScalar(t *core.T) {
	out := commandArgv(map[string]any{"argv": []any{"echo", 1, nil}})
	core.AssertElementsMatch(t, []string{"echo", "1"}, out)

	out = commandArgv(map[string]any{"argv": "whoami"})
	core.AssertElementsMatch(t, []string{"whoami"}, out)
}

// --- gatherSubsetKeys ---

func TestExecutorLookup_GatherSubsetKeys_Good_KnownSubsets(t *core.T) {
	core.AssertLen(t, gatherSubsetKeys("all"), 10)
	core.AssertLen(t, gatherSubsetKeys("hardware"), 4)
	core.AssertElementsMatch(t, []string{"ansible_default_ipv4_address"}, gatherSubsetKeys("network"))
}

func TestExecutorLookup_GatherSubsetKeys_Bad_Unknown(t *core.T) {
	core.AssertNil(t, gatherSubsetKeys("bogus"))
}

// --- passwordLookupCharset ---

func TestExecutorLookup_PasswordLookupCharset_Good_NamedSets(t *core.T) {
	core.AssertEqual(t, "0123456789", passwordLookupCharset("digits"))
	core.AssertEqual(t, "abcdefghijklmnopqrstuvwxyz", passwordLookupCharset("ascii_lowercase"))
}

func TestExecutorLookup_PasswordLookupCharset_Bad_Empty(t *core.T) {
	core.AssertEqual(t, "", passwordLookupCharset(""))
}

func TestExecutorLookup_PasswordLookupCharset_Ugly_DedupAcrossSets(t *core.T) {
	// hexdigits + digits share 0-9; duplicates are collapsed.
	got := passwordLookupCharset("digits,hexdigits")
	core.AssertContains(t, got, "abcdef")
	core.AssertContains(t, got, "0123456789")
	// Each rune appears once: count '0'.
	count := 0
	for _, r := range got {
		if r == '0' {
			count++
		}
	}
	core.AssertEqual(t, 1, count)
}

// --- parsePasswordLookupSpec ---

func TestExecutorLookup_ParsePasswordLookupSpec_Good_PathAndOptions(t *core.T) {
	spec := parsePasswordLookupSpec("/tmp/secret length=30 chars=digits seed=abc")
	core.AssertEqual(t, "/tmp/secret", spec.path)
	core.AssertEqual(t, 30, spec.length)
	core.AssertEqual(t, "0123456789", spec.chars)
	core.AssertEqual(t, "abc", spec.seed)
}

func TestExecutorLookup_ParsePasswordLookupSpec_Bad_InvalidLengthIgnored(t *core.T) {
	spec := parsePasswordLookupSpec("/tmp/secret length=notnum")
	// Invalid length leaves the default in place.
	core.AssertEqual(t, 20, spec.length)
	core.AssertEqual(t, "/tmp/secret", spec.path)
}

func TestExecutorLookup_ParsePasswordLookupSpec_Ugly_DefaultsWhenBare(t *core.T) {
	spec := parsePasswordLookupSpec("")
	core.AssertEqual(t, 20, spec.length)
	core.AssertEqual(t, "", spec.path)
	core.AssertNotEmpty(t, spec.chars)
}

// --- generatePassword ---

func TestExecutorLookup_GeneratePassword_Good_SeededDeterministic(t *core.T) {
	r1 := generatePassword(12, "abcdef", "fixed-seed")
	r2 := generatePassword(12, "abcdef", "fixed-seed")
	core.RequireTrue(t, r1.OK)
	core.RequireTrue(t, r2.OK)
	core.AssertEqual(t, r1.Value.(string), r2.Value.(string))
	core.AssertLen(t, r1.Value.(string), 12)
}

func TestExecutorLookup_GeneratePassword_Ugly_DefaultsApplied(t *core.T) {
	// Non-positive length and empty charset both fall back to defaults.
	r := generatePassword(0, "", "seed")
	core.RequireTrue(t, r.OK)
	core.AssertLen(t, r.Value.(string), 20)
}

func TestExecutorLookup_GeneratePassword_Good_UnseededRandom(t *core.T) {
	r := generatePassword(16, "abcdef", "")
	core.RequireTrue(t, r.OK)
	core.AssertLen(t, r.Value.(string), 16)
}
