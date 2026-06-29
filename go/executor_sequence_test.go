package ansible

import (
	core "dappco.re/go"
)

// --- sequenceSpecInt ---

func TestExecutorSequence_SequenceSpecInt_Good_NumericKinds(t *core.T) {
	n, ok := sequenceSpecInt(5)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 5, n)

	n, ok = sequenceSpecInt(int64(9))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 9, n)

	n, ok = sequenceSpecInt("12")
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 12, n)
}

func TestExecutorSequence_SequenceSpecInt_Bad_NonNumeric(t *core.T) {
	_, ok := sequenceSpecInt("abc")
	core.AssertFalse(t, ok)
	_, ok = sequenceSpecInt(1.5)
	core.AssertFalse(t, ok)
	_, ok = sequenceSpecInt(nil)
	core.AssertFalse(t, ok)
}

func TestExecutorSequence_SequenceSpecInt_Ugly_UnsignedKinds(t *core.T) {
	n, ok := sequenceSpecInt(uint16(7))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 7, n)

	n, ok = sequenceSpecInt(uint8(255))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 255, n)
}

// --- parseSequenceSpec ---

func TestExecutorSequence_ParseSequenceSpec_Good_StringStartEnd(t *core.T) {
	r := parseSequenceSpec("start=1 end=3")
	core.RequireTrue(t, r.OK)
	spec := r.Value.(*sequenceSpec)
	core.AssertEqual(t, 1, spec.start)
	core.AssertEqual(t, 3, spec.end)
	core.AssertTrue(t, spec.hasEnd)
}

func TestExecutorSequence_ParseSequenceSpec_Good_CountDerivesEnd(t *core.T) {
	r := parseSequenceSpec(map[string]any{"start": 2, "count": 3, "stride": 2})
	core.RequireTrue(t, r.OK)
	spec := r.Value.(*sequenceSpec)
	// end = start + (count-1)*step = 2 + 2*2 = 6
	core.AssertEqual(t, 6, spec.end)
	core.AssertTrue(t, spec.hasEnd)
}

func TestExecutorSequence_ParseSequenceSpec_Bad_MissingEndAndCount(t *core.T) {
	r := parseSequenceSpec("start=1")
	core.AssertFalse(t, r.OK)
	core.AssertContains(t, r.Error(), "end or count")
}

func TestExecutorSequence_ParseSequenceSpec_Bad_ZeroStride(t *core.T) {
	r := parseSequenceSpec(map[string]any{"start": 1, "end": 5, "stride": 0})
	core.AssertFalse(t, r.OK)
	core.AssertContains(t, r.Error(), "stride must not be zero")
}

func TestExecutorSequence_ParseSequenceSpec_Bad_NonNumericStart(t *core.T) {
	r := parseSequenceSpec(map[string]any{"start": "x", "end": 5})
	core.AssertFalse(t, r.OK)
	core.AssertContains(t, r.Error(), "start must be numeric")
}

func TestExecutorSequence_ParseSequenceSpec_Ugly_UnsupportedType(t *core.T) {
	r := parseSequenceSpec(42)
	core.AssertFalse(t, r.OK)
	core.AssertContains(t, r.Error(), "string or mapping")
}

func TestExecutorSequence_ParseSequenceSpec_Ugly_DescendingFlipsStep(t *core.T) {
	// end < start with positive step -> parser flips the step negative.
	r := parseSequenceSpec(map[string]any{"start": 5, "end": 1, "stride": 1})
	core.RequireTrue(t, r.OK)
	spec := r.Value.(*sequenceSpec)
	core.AssertLess(t, spec.step, 0)
}

// --- buildSequenceValues / formatSequenceValue ---

func TestExecutorSequence_BuildSequenceValues_Good_Ascending(t *core.T) {
	spec := &sequenceSpec{start: 1, end: 3, step: 1, format: "%d", hasEnd: true}
	r := buildSequenceValues(spec)
	core.RequireTrue(t, r.OK)
	core.AssertElementsMatch(t, []string{"1", "2", "3"}, r.Value.([]string))
}

func TestExecutorSequence_BuildSequenceValues_Ugly_DescendingAndFormat(t *core.T) {
	spec := &sequenceSpec{start: 3, end: 1, step: -1, format: "host%02d", hasEnd: true}
	r := buildSequenceValues(spec)
	core.RequireTrue(t, r.OK)
	core.AssertElementsMatch(t, []string{"host03", "host02", "host01"}, r.Value.([]string))
}

func TestExecutorSequence_FormatSequenceValue_Bad_EmptyOrBadFormatFallsBack(t *core.T) {
	core.AssertEqual(t, "5", formatSequenceValue("", 5))
	// A format producing nothing usable falls back to plain decimal.
	core.AssertEqual(t, "7", formatSequenceValue("%d", 7))
}

// --- parseSkipMissingValue ---

func TestExecutorSequence_ParseSkipMissingValue_Good_BoolAndKeyword(t *core.T) {
	core.AssertTrue(t, parseSkipMissingValue(true))
	core.AssertTrue(t, parseSkipMissingValue("skip_missing"))
	core.AssertTrue(t, parseSkipMissingValue("skip_missing=true"))
}

func TestExecutorSequence_ParseSkipMissingValue_Bad_FalseAndEmpty(t *core.T) {
	core.AssertFalse(t, parseSkipMissingValue(false))
	core.AssertFalse(t, parseSkipMissingValue("  "))
	core.AssertFalse(t, parseSkipMissingValue(42))
}

func TestExecutorSequence_ParseSkipMissingValue_Ugly_Maps(t *core.T) {
	core.AssertTrue(t, parseSkipMissingValue(map[string]any{"skip_missing": true}))
	core.AssertTrue(t, parseSkipMissingValue(map[any]any{"skip_missing": true}))
	core.AssertFalse(t, parseSkipMissingValue(map[any]any{42: true}))
}

// --- parseSubelementsSpec ---

func TestExecutorSequence_ParseSubelementsSpec_Good_AnySlice(t *core.T) {
	parent, path, skip, ok := parseSubelementsSpec([]any{"users", "groups", true})
	core.AssertTrue(t, ok)
	core.AssertEqual(t, "users", parent)
	core.AssertEqual(t, "groups", path)
	core.AssertTrue(t, skip)
}

func TestExecutorSequence_ParseSubelementsSpec_Good_String(t *core.T) {
	parent, path, skip, ok := parseSubelementsSpec("users groups")
	core.AssertTrue(t, ok)
	core.AssertEqual(t, "users", parent)
	core.AssertEqual(t, "groups", path)
	core.AssertFalse(t, skip)
}

func TestExecutorSequence_ParseSubelementsSpec_Bad_TooShort(t *core.T) {
	_, _, _, ok := parseSubelementsSpec([]any{"users"})
	core.AssertFalse(t, ok)
	_, _, _, ok = parseSubelementsSpec("users")
	core.AssertFalse(t, ok)
	_, _, _, ok = parseSubelementsSpec(42)
	core.AssertFalse(t, ok)
}

func TestExecutorSequence_ParseSubelementsSpec_Ugly_StringSlice(t *core.T) {
	parent, path, skip, ok := parseSubelementsSpec([]string{"users", "groups", "skip_missing"})
	core.AssertTrue(t, ok)
	core.AssertEqual(t, "users", parent)
	core.AssertEqual(t, "groups", path)
	core.AssertTrue(t, skip)
}
