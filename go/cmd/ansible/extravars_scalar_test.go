package ansiblecmd

import (
	core "dappco.re/go"
)

// --- parseExtraVarsScalar ---

func TestExtraVarsScalar_ParseExtraVarsScalar_Good_TypedScalars(t *core.T) {
	core.AssertEqual(t, 42, parseExtraVarsScalar("42"))
	core.AssertEqual(t, true, parseExtraVarsScalar("true"))
	core.AssertEqual(t, "plain", parseExtraVarsScalar("plain"))
}

func TestExtraVarsScalar_ParseExtraVarsScalar_Bad_EmptyStaysEmpty(t *core.T) {
	core.AssertEqual(t, "", parseExtraVarsScalar(""))
}

func TestExtraVarsScalar_ParseExtraVarsScalar_Ugly_StructuredKeptAsRawString(t *core.T) {
	// A YAML mapping or sequence is preserved as the raw string, not decoded,
	// so structured extra-vars flow through the dedicated structured path.
	core.AssertEqual(t, "{a: 1}", parseExtraVarsScalar("{a: 1}"))
	core.AssertEqual(t, "[1, 2]", parseExtraVarsScalar("[1, 2]"))
}

// --- trimCutset / containsRune / dirSep ---

func TestExtraVarsScalar_TrimCutset_Good_BothEnds(t *core.T) {
	core.AssertEqual(t, "core", trimCutset("''core''", "'"))
	core.AssertEqual(t, "abc", trimCutset("xxabcxx", "x"))
}

func TestExtraVarsScalar_TrimCutset_Bad_NoCutsetMatch(t *core.T) {
	core.AssertEqual(t, "abc", trimCutset("abc", "z"))
}

func TestExtraVarsScalar_ContainsRune_Good_PresentAndAbsent(t *core.T) {
	core.AssertTrue(t, containsRune("abc", 'b'))
	core.AssertFalse(t, containsRune("abc", 'z'))
}

func TestExtraVarsScalar_DirSep_Good_DefaultsToSlash(t *core.T) {
	// With no DS override the separator defaults to "/".
	core.AssertEqual(t, "/", dirSep())
}

// --- print (comment-as-usage Example) ---

func ExamplePrint() {
	print("%s=%d\n", "n", 3)
	// Output: n=3
}
