package ansible

import (
	core "dappco.re/go"
)

// --- isEmptyTemplateValue ---

func TestTemplateFeatures_IsEmptyTemplateValue_Good_EmptyKinds(t *core.T) {
	core.AssertTrue(t, isEmptyTemplateValue(nil))
	core.AssertTrue(t, isEmptyTemplateValue(""))
	core.AssertTrue(t, isEmptyTemplateValue([]any{}))
	core.AssertTrue(t, isEmptyTemplateValue([]string{}))
	core.AssertTrue(t, isEmptyTemplateValue(map[string]any{}))
	core.AssertTrue(t, isEmptyTemplateValue(map[any]any{}))
}

func TestTemplateFeatures_IsEmptyTemplateValue_Bad_NonEmptyKinds(t *core.T) {
	core.AssertFalse(t, isEmptyTemplateValue("x"))
	core.AssertFalse(t, isEmptyTemplateValue([]any{1}))
	core.AssertFalse(t, isEmptyTemplateValue([]string{"a"}))
	core.AssertFalse(t, isEmptyTemplateValue(map[string]any{"k": 1}))
	core.AssertFalse(t, isEmptyTemplateValue(42))
}

func TestTemplateFeatures_IsEmptyTemplateValue_Ugly_UnresolvedAndReflectKinds(t *core.T) {
	// An unresolved {{ ... }} placeholder counts as empty so default filters fire.
	core.AssertTrue(t, isEmptyTemplateValue("{{ undefined_var }}"))
	// Reflect path: an empty typed slice.
	core.AssertTrue(t, isEmptyTemplateValue([]int{}))
	core.AssertFalse(t, isEmptyTemplateValue([]int{1, 2}))
	// Reflect path: an empty typed map.
	core.AssertTrue(t, isEmptyTemplateValue(map[int]int{}))
}

// --- templateBool ---

func TestTemplateFeatures_TemplateBool_Good_TruthyStrings(t *core.T) {
	core.AssertTrue(t, templateBool("true"))
	core.AssertTrue(t, templateBool("YES"))
	core.AssertTrue(t, templateBool(" 1 "))
	core.AssertTrue(t, templateBool(true))
}

func TestTemplateFeatures_TemplateBool_Bad_FalsyValues(t *core.T) {
	core.AssertFalse(t, templateBool("false"))
	core.AssertFalse(t, templateBool("no"))
	core.AssertFalse(t, templateBool(""))
	core.AssertFalse(t, templateBool(false))
	core.AssertFalse(t, templateBool(0))
}

func TestTemplateFeatures_TemplateBool_Ugly_NumericKindsAndFallback(t *core.T) {
	core.AssertTrue(t, templateBool(int8(1)))
	core.AssertTrue(t, templateBool(int64(-1)))
	core.AssertTrue(t, templateBool(uint16(3)))
	core.AssertTrue(t, templateBool(float32(0.5)))
	core.AssertFalse(t, templateBool(float64(0)))
	// Unknown type falls through to stringify-non-empty.
	core.AssertTrue(t, templateBool([]any{1}))
	core.AssertFalse(t, templateBool(nil))
}

// --- templateInt ---

func TestTemplateFeatures_TemplateInt_Good_NumericAndString(t *core.T) {
	n, ok := templateInt(7)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 7, n)

	n, ok = templateInt("42")
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 42, n)

	n, ok = templateInt(int64(9))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 9, n)
}

func TestTemplateFeatures_TemplateInt_Bad_NonNumericString(t *core.T) {
	n, ok := templateInt("not-a-number")
	core.AssertFalse(t, ok)
	core.AssertEqual(t, 0, n)

	n, ok = templateInt(nil)
	core.AssertFalse(t, ok)
	core.AssertEqual(t, 0, n)
}

func TestTemplateFeatures_TemplateInt_Ugly_FloatStringTruncation(t *core.T) {
	// A float-looking string flows through templateFloat and truncates.
	n, ok := templateInt("3.9")
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 3, n)

	n, ok = templateInt(float64(5.7))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 5, n)

	n, ok = templateInt(uint8(255))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 255, n)
}

// --- templateFloat ---

func TestTemplateFeatures_TemplateFloat_Good_NumericAndString(t *core.T) {
	f, ok := templateFloat(2.5)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 2.5, f)

	f, ok = templateFloat("1.25")
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 1.25, f)

	f, ok = templateFloat(int32(4))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, float64(4), f)
}

func TestTemplateFeatures_TemplateFloat_Bad_NonNumeric(t *core.T) {
	f, ok := templateFloat("xyz")
	core.AssertFalse(t, ok)
	core.AssertEqual(t, float64(0), f)

	f, ok = templateFloat([]any{1})
	core.AssertFalse(t, ok)
	core.AssertEqual(t, float64(0), f)
}

func TestTemplateFeatures_TemplateFloat_Ugly_UnsignedAndWhitespaceString(t *core.T) {
	f, ok := templateFloat(uint64(8))
	core.AssertTrue(t, ok)
	core.AssertEqual(t, float64(8), f)

	f, ok = templateFloat("  6.0 ")
	core.AssertTrue(t, ok)
	core.AssertEqual(t, float64(6), f)
}

// --- templateLength ---

func TestTemplateFeatures_TemplateLength_Good_StringAndSlices(t *core.T) {
	core.AssertEqual(t, 3, templateLength("abc"))
	core.AssertEqual(t, 2, templateLength([]any{1, 2}))
	core.AssertEqual(t, 1, templateLength([]string{"x"}))
	core.AssertEqual(t, 2, templateLength(map[string]any{"a": 1, "b": 2}))
}

func TestTemplateFeatures_TemplateLength_Bad_NilAndUnsupported(t *core.T) {
	core.AssertEqual(t, 0, templateLength(nil))
	// A bare int has no native length and stringifies to "5" -> length 1.
	core.AssertEqual(t, 1, templateLength(5))
}

func TestTemplateFeatures_TemplateLength_Ugly_ReflectKindsAndMapAny(t *core.T) {
	core.AssertEqual(t, 1, templateLength(map[any]any{"k": "v"}))
	// Reflect path: a typed slice.
	core.AssertEqual(t, 3, templateLength([]int{1, 2, 3}))
	// Reflect path: a typed array.
	core.AssertEqual(t, 2, templateLength([2]int{0, 0}))
}

// --- templateValueGreater ---

func TestTemplateFeatures_TemplateValueGreater_Good_NumericCompare(t *core.T) {
	core.AssertTrue(t, templateValueGreater(5, 3, true))
	core.AssertFalse(t, templateValueGreater(5, 3, false))
	core.AssertTrue(t, templateValueGreater(2, 9, false))
}

func TestTemplateFeatures_TemplateValueGreater_Bad_StringFallback(t *core.T) {
	// Non-numeric values fall back to lexical comparison.
	core.AssertTrue(t, templateValueGreater("beta", "alpha", true))
	core.AssertFalse(t, templateValueGreater("beta", "alpha", false))
}

func TestTemplateFeatures_TemplateValueGreater_Ugly_MixedNumericString(t *core.T) {
	// One side non-numeric -> lexical path on both stringified forms.
	core.AssertTrue(t, templateValueGreater("9", "x", false))
}

// --- templateMinMax ---

func TestTemplateFeatures_TemplateMinMax_Good_MinAndMax(t *core.T) {
	got, ok := templateMinMax([]any{3, 1, 2}, false)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 1, got)

	got, ok = templateMinMax([]any{3, 1, 2}, true)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, 3, got)
}

func TestTemplateFeatures_TemplateMinMax_Bad_EmptyOrNonSlice(t *core.T) {
	_, ok := templateMinMax([]any{}, true)
	core.AssertFalse(t, ok)

	_, ok = templateMinMax(42, true)
	core.AssertFalse(t, ok)
}

func TestTemplateFeatures_TemplateMinMax_Ugly_StringElements(t *core.T) {
	got, ok := templateMinMax([]any{"banana", "apple", "cherry"}, false)
	core.AssertTrue(t, ok)
	core.AssertEqual(t, "apple", got)
}

// --- templateStringify ---

func TestTemplateFeatures_TemplateStringify_Good_Primitives(t *core.T) {
	core.AssertEqual(t, "", templateStringify(nil))
	core.AssertEqual(t, "hello", templateStringify("hello"))
}

func TestTemplateFeatures_TemplateStringify_Ugly_CollectionsAndOther(t *core.T) {
	core.AssertNotEmpty(t, templateStringify([]string{"a", "b"}))
	core.AssertNotEmpty(t, templateStringify([]any{1, 2}))
	core.AssertNotEmpty(t, templateStringify(map[string]any{"k": 1}))
	core.AssertNotEmpty(t, templateStringify(3.14))
}
