package ansiblecmd

import (
	core "dappco.re/go"
)

// --- absPath / cleanPath ---

func TestCorePrimitives_AbsPath_Good_AbsoluteCleaned(t *core.T) {
	core.AssertEqual(t, "/srv/app", absPath("/srv/app"))
	core.AssertEqual(t, "/srv/app", absPath("/srv/./app"))
}

func TestCorePrimitives_AbsPath_Ugly_RelativeJoinsCwd(t *core.T) {
	got := absPath("playbooks/site.yml")
	core.AssertTrue(t, hasPrefix(got, "/") || contains(got, "playbooks/site.yml"))
	core.AssertTrue(t, contains(got, "playbooks/site.yml"))
}

func TestCorePrimitives_AbsPath_Bad_EmptyMatchesCwd(t *core.T) {
	core.AssertEqual(t, core.Env("DIR_CWD"), absPath(""))
}

func TestCorePrimitives_CleanPath_Good_NormalisesDots(t *core.T) {
	core.AssertEqual(t, "a/b", cleanPath("a/./b"))
	core.AssertEqual(t, ".", cleanPath(""))
}

// --- repeat ---

func TestCorePrimitives_Repeat_Good_BuildsString(t *core.T) {
	core.AssertEqual(t, "==", repeat("=", 2))
}

func TestCorePrimitives_Repeat_Bad_NonPositive(t *core.T) {
	core.AssertEqual(t, "", repeat("=", 0))
	core.AssertEqual(t, "", repeat("=", -1))
}

// --- fields ---

func TestCorePrimitives_Fields_Good_SplitsOnWhitespace(t *core.T) {
	core.AssertElementsMatch(t, []string{"a", "b", "c"}, fields("  a b\tc  "))
}

func TestCorePrimitives_Fields_Bad_EmptyAndBlank(t *core.T) {
	core.AssertLen(t, fields(""), 0)
	core.AssertLen(t, fields("   \t  "), 0)
}

// --- println (comment-as-usage Example) ---

func ExamplePrintln() {
	println("ready", true)
	// Output: ready true
}
