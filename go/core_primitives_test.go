package ansible

import (
	core "dappco.re/go"
)

// --- corexAbsPath / absPath ---

func TestCorePrimitives_AbsPath_Good_AbsoluteStaysClean(t *core.T) {
	// An already-absolute path is returned cleaned, independent of cwd.
	core.AssertEqual(t, "/etc/hosts", absPath("/etc/hosts"))
	core.AssertEqual(t, "/etc/hosts", absPath("/etc/./hosts"))
}

func TestCorePrimitives_AbsPath_Ugly_RelativeJoinsCwd(t *core.T) {
	// A relative path is joined onto the current working directory, so the
	// result must end with the relative tail and be absolute.
	got := corexAbsPath("sub/file.txt")
	core.AssertTrue(t, hasSuffix(got, "sub/file.txt"))
}

func TestCorePrimitives_AbsPath_Bad_EmptyReturnsCwd(t *core.T) {
	// Empty input falls back to the configured cwd; both helper and alias agree.
	core.AssertEqual(t, corexAbsPath(""), absPath(""))
}

// --- corexRepeat / repeat ---

func TestCorePrimitives_Repeat_Good_BuildsString(t *core.T) {
	core.AssertEqual(t, "ababab", repeat("ab", 3))
	core.AssertEqual(t, "xxx", corexRepeat("x", 3))
}

func TestCorePrimitives_Repeat_Bad_NonPositiveCountEmpty(t *core.T) {
	core.AssertEqual(t, "", repeat("ab", 0))
	core.AssertEqual(t, "", repeat("ab", -2))
}

func TestCorePrimitives_Repeat_Ugly_EmptySourceStillEmpty(t *core.T) {
	core.AssertEqual(t, "", repeat("", 5))
}

// --- corexSprint / sprint ---

func TestCorePrimitives_Sprint_Good_ConcatenatesArgs(t *core.T) {
	core.AssertEqual(t, sprint("a", "b"), corexSprint("a", "b"))
	core.AssertNotEmpty(t, sprint(1, 2, 3))
}

// --- corexWriteString / writeString ---

func TestCorePrimitives_WriteString_Good_WritesToBuilder(t *core.T) {
	buf := newBuilder()
	writeString(buf, "hello ")
	writeString(buf, "world")
	core.AssertEqual(t, "hello world", buf.String())
}

// --- corexReplaceN / replaceN ---

func TestCorePrimitives_ReplaceN_Good_LimitedReplacements(t *core.T) {
	core.AssertEqual(t, "X-X-a", replaceN("a-a-a", "a", "X", 2))
}

func TestCorePrimitives_ReplaceN_Bad_ZeroOrEmptyOldIsNoop(t *core.T) {
	core.AssertEqual(t, "a-a", replaceN("a-a", "a", "X", 0))
	core.AssertEqual(t, "a-a", replaceN("a-a", "", "X", 3))
}

func TestCorePrimitives_ReplaceN_Ugly_NegativeReplacesAll(t *core.T) {
	core.AssertEqual(t, "X-X-X", replaceN("a-a-a", "a", "X", -1))
	// Needle not present leaves the string untouched even with a limit.
	core.AssertEqual(t, "abc", replaceN("abc", "z", "Q", 2))
}

// --- corexStringIndex / corexStringLastIndex ---

func TestCorePrimitives_StringIndex_Good_FindsFirst(t *core.T) {
	core.AssertEqual(t, 2, stringIndex("aabaa", "b"))
	core.AssertEqual(t, 0, stringIndex("abc", ""))
}

func TestCorePrimitives_StringIndex_Bad_NotFoundAndTooLong(t *core.T) {
	core.AssertEqual(t, -1, stringIndex("abc", "z"))
	core.AssertEqual(t, -1, stringIndex("ab", "abcdef"))
}

func TestCorePrimitives_StringLastIndex_Ugly_FindsLastAndEdges(t *core.T) {
	core.AssertEqual(t, 3, stringLastIndex("aabaa", "a"+"a"))
	core.AssertEqual(t, len("abc"), stringLastIndex("abc", ""))
	core.AssertEqual(t, -1, stringLastIndex("ab", "zzz"))
	core.AssertEqual(t, -1, stringLastIndex("abc", "z"))
}

// --- corexCut / cut ---

func TestCorePrimitives_Cut_Good_SplitsOnSep(t *core.T) {
	before, after, found := cut("key=value", "=")
	core.AssertTrue(t, found)
	core.AssertEqual(t, "key", before)
	core.AssertEqual(t, "value", after)
}

func TestCorePrimitives_Cut_Bad_SepAbsent(t *core.T) {
	before, after, found := cut("plain", "=")
	core.AssertFalse(t, found)
	core.AssertEqual(t, "plain", before)
	core.AssertEqual(t, "", after)
}

func TestCorePrimitives_Cut_Ugly_EmptySep(t *core.T) {
	before, after, found := cut("plain", "")
	core.AssertTrue(t, found)
	core.AssertEqual(t, "", before)
	core.AssertEqual(t, "plain", after)
}
