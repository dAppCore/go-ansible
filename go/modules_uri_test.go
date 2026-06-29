package ansible

import (
	core "dappco.re/go"
)

// --- multipartFieldValues ---

func TestModulesURI_MultipartFieldValues_Good_ScalarAndSlice(t *core.T) {
	core.AssertElementsMatch(t, []string{"k=v"}, multipartFieldValues("k", "v"))
	core.AssertElementsMatch(t, []string{"k="}, multipartFieldValues("k", nil))
	core.AssertElementsMatch(t, []string{"k=a", "k=b"}, multipartFieldValues("k", []string{"a", "b"}))
}

func TestModulesURI_MultipartFieldValues_Ugly_AnySliceAndOther(t *core.T) {
	core.AssertElementsMatch(t, []string{"n=1", "n=2"}, multipartFieldValues("n", []any{1, 2}))
	core.AssertElementsMatch(t, []string{"flag=true"}, multipartFieldValues("flag", true))
}

// --- multipartBodyFields ---

func TestModulesURI_MultipartBodyFields_Good_MapSorted(t *core.T) {
	// Keys are emitted in sorted order.
	got := multipartBodyFields(map[string]any{"b": "2", "a": "1"})
	core.AssertEqual(t, 2, len(got))
	core.AssertEqual(t, "a=1", got[0])
	core.AssertEqual(t, "b=2", got[1])
}

func TestModulesURI_MultipartBodyFields_Bad_EmptyStringNil(t *core.T) {
	core.AssertNil(t, multipartBodyFields(""))
}

func TestModulesURI_MultipartBodyFields_Ugly_AnySliceOfPairs(t *core.T) {
	got := multipartBodyFields([]any{
		map[string]any{"key": "name", "value": "nginx"},
		map[string]any{"name": "state", "value": "present"},
	})
	core.AssertElementsMatch(t, []string{"name=nginx", "state=present"}, got)
}

// --- appendFormValue / renderURIBodyFormEncoded ---

func TestModulesURI_RenderURIBodyFormEncoded_Good_SortedMap(t *core.T) {
	// url.Values.Encode sorts keys, so output is deterministic.
	got := renderURIBodyFormEncoded(map[string]any{"name": "nginx", "state": "present"})
	core.AssertEqual(t, "name=nginx&state=present", got)
}

func TestModulesURI_RenderURIBodyFormEncoded_Bad_StringPassthrough(t *core.T) {
	core.AssertEqual(t, "already=encoded", renderURIBodyFormEncoded("already=encoded"))
}

func TestModulesURI_RenderURIBodyFormEncoded_Ugly_SliceValuesAndNil(t *core.T) {
	got := renderURIBodyFormEncoded(map[string]any{"tag": []string{"a", "b"}, "empty": nil})
	core.AssertContains(t, got, "tag=a")
	core.AssertContains(t, got, "tag=b")
	core.AssertContains(t, got, "empty=")
}

// --- isHexDigest ---

func TestModulesURI_IsHexDigest_Good_LowerHex(t *core.T) {
	core.AssertTrue(t, isHexDigest("deadbeef0123"))
}

func TestModulesURI_IsHexDigest_Bad_EmptyAndNonHex(t *core.T) {
	core.AssertFalse(t, isHexDigest(""))
	core.AssertFalse(t, isHexDigest("xyz"))
	core.AssertFalse(t, isHexDigest("DEADBEEF")) // uppercase not accepted
}

// --- parseGetURLChecksumFile ---

func TestModulesURI_ParseGetURLChecksumFile_Good_MatchesFilename(t *core.T) {
	content := "abc123  other.tar.gz\ndef456  app.tar.gz\n"
	r := parseGetURLChecksumFile(content, "/tmp/app.tar.gz", "sha256")
	core.RequireTrue(t, r.OK)
	core.AssertEqual(t, "def456", r.Value.(string))
}

func TestModulesURI_ParseGetURLChecksumFile_Good_SingleFieldDigest(t *core.T) {
	r := parseGetURLChecksumFile("abc123\n", "/tmp/app.tar.gz", "sha256")
	core.RequireTrue(t, r.OK)
	core.AssertEqual(t, "abc123", r.Value.(string))
}

func TestModulesURI_ParseGetURLChecksumFile_Bad_NoDigest(t *core.T) {
	r := parseGetURLChecksumFile("not a checksum file\n", "/tmp/app.tar.gz", "sha256")
	core.AssertFalse(t, r.OK)
	core.AssertContains(t, r.Error(), "could not parse checksum file")
}

func TestModulesURI_ParseGetURLChecksumFile_Ugly_FallsBackToFirstDigest(t *core.T) {
	// No filename match, but a bare digest line exists -> first digest wins.
	content := "deadbeef  unrelated.bin\n"
	r := parseGetURLChecksumFile(content, "/tmp/app.tar.gz", "sha256")
	core.RequireTrue(t, r.OK)
	core.AssertEqual(t, "deadbeef", r.Value.(string))
}
