package ansible

import (
	"encoding/base64"
	"reflect"
	"regexp"
	"strconv"

	"gopkg.in/yaml.v3"
)

// resolveExprValue evaluates a template expression and preserves native values.
func (e *Executor) resolveExprValue(expr string, host string, task *Task) (any, bool) {
	parts := splitTemplatePipeline(expr)
	if len(parts) == 0 {
		return nil, false
	}

	value, ok := e.resolveExprBaseValue(parts[0], host, task)
	if !ok {
		value = "{{ " + corexTrimSpace(parts[0]) + " }}"
	}

	for _, filter := range parts[1:] {
		value = e.applyFilterValue(value, filter)
	}

	return value, true
}

// resolveExprBaseValue resolves the first expression segment before filters run.
func (e *Executor) resolveExprBaseValue(expr string, host string, task *Task) (any, bool) {
	expr = corexTrimSpace(expr)
	if expr == "" {
		return nil, false
	}

	if corexHasPrefix(expr, "lookup(") {
		if value, ok := e.lookupValue(expr, host, task); ok {
			return value, true
		}
	}

	if contains(expr, ".") {
		parts := splitN(expr, ".", 2)
		if result := e.getRegisteredVar(host, parts[0]); result != nil {
			if value, ok := taskResultField(result, parts[1]); ok {
				return value, true
			}
		}
	}

	if result := e.getRegisteredVar(host, expr); result != nil {
		return result, true
	}

	if value, ok := e.lookupExprValue(expr, host, task); ok {
		return value, true
	}

	return nil, false
}

// applyFilterValue applies a supported Jinja-style filter to a native value.
func (e *Executor) applyFilterValue(value any, filter string) any {
	filter = corexTrimSpace(filter)

	if corexHasPrefix(filter, "default(") {
		if isEmptyTemplateValue(value) {
			if raw, ok := parseTemplateFilterLiteral(filter, "default"); ok {
				return raw
			}
		}
		return value
	}

	if filter == "upper" {
		return upper(templateStringify(value))
	}
	if filter == "lower" {
		return lower(templateStringify(value))
	}
	if filter == "trim" {
		return corexTrimSpace(templateStringify(value))
	}
	if corexHasPrefix(filter, "basename") {
		return pathBase(templateStringify(value))
	}
	if corexHasPrefix(filter, "dirname") {
		return pathDir(templateStringify(value))
	}
	if corexHasPrefix(filter, "join(") {
		separator := ""
		if raw, ok := parseTemplateFilterLiteral(filter, "join"); ok {
			separator = templateStringify(raw)
		}

		items, ok := anySliceFromValue(value)
		if !ok {
			return templateStringify(value)
		}

		parts := make([]string, 0, len(items))
		for _, item := range items {
			parts = append(parts, templateStringify(item))
		}
		return join(separator, parts)
	}
	if filter == "split" || corexHasPrefix(filter, "split(") {
		separator := ""
		if raw, ok := parseTemplateFilterLiteral(filter, "split"); ok {
			separator = templateStringify(raw)
		}

		text := templateStringify(value)
		if separator == "" {
			parts := fields(text)
			items := make([]any, len(parts))
			for i, part := range parts {
				items[i] = part
			}
			return items
		}

		parts := split(text, separator)
		items := make([]any, len(parts))
		for i, part := range parts {
			items[i] = part
		}
		return items
	}
	if filter == "bool" {
		return templateBool(value)
	}
	if filter == "int" {
		if n, ok := templateInt(value); ok {
			return n
		}
		return 0
	}
	if filter == "abs" {
		if n, ok := templateFloat(value); ok {
			if n < 0 {
				n = -n
			}
			if float64(int64(n)) == n {
				return int(n)
			}
			return n
		}
		return value
	}
	if corexHasPrefix(filter, "min") || corexHasPrefix(filter, "max") {
		wantMax := corexHasPrefix(filter, "max")
		if result, ok := templateMinMax(value, wantMax); ok {
			return result
		}
		return value
	}
	if filter == "length" {
		return templateLength(value)
	}
	if corexHasPrefix(filter, "regex_replace(") {
		pattern, replacement, ok := parseRegexReplaceFilter(filter)
		if !ok {
			return templateStringify(value)
		}

		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return templateStringify(value)
		}
		return compiled.ReplaceAllString(templateStringify(value), replacement)
	}
	if filter == "b64decode" {
		valueStr := templateStringify(value)
		decoded, err := base64.StdEncoding.DecodeString(valueStr)
		if err == nil {
			return string(decoded)
		}
		if decoded, err := base64.RawStdEncoding.DecodeString(valueStr); err == nil {
			return string(decoded)
		}
		return valueStr
	}
	if filter == "b64encode" {
		return base64.StdEncoding.EncodeToString([]byte(templateStringify(value)))
	}

	return value
}

// parseTemplateFilterLiteral parses a single literal argument from a filter call.
func parseTemplateFilterLiteral(filter, name string) (any, bool) {
	if !corexHasPrefix(filter, name+"(") || !corexHasSuffix(filter, ")") {
		return nil, false
	}

	raw := trimSpace(filter[len(name)+1 : len(filter)-1])
	if raw == "" {
		return "", true
	}

	var value any
	if err := yaml.Unmarshal([]byte(raw), &value); err != nil {
		return trimCutset(raw, "'\""), true
	}
	return value, true
}

// templateStringify converts template values to Ansible-style strings.
func templateStringify(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []string:
		return sprintf("%v", v)
	case []any:
		return sprintf("%v", v)
	case map[string]any:
		return sprintf("%v", v)
	case map[any]any:
		return sprintf("%v", v)
	default:
		return sprintf("%v", value)
	}
}

// isEmptyTemplateValue reports whether a value should trigger default filters.
func isEmptyTemplateValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == "" || isUnresolvedTemplateValue(v)
	case []any:
		return len(v) == 0
	case []string:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	case map[any]any:
		return len(v) == 0
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return true
	}

	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return rv.Len() == 0
	}

	return false
}

// templateBool coerces common Ansible truthy values to bool.
func templateBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		lowered := lower(trimSpace(v))
		return lowered == "true" || lowered == "yes" || lowered == "1"
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	default:
		return templateStringify(value) != ""
	}
}

// templateInt coerces numeric template values to int.
func templateInt(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case uint:
		return int(v), true
	case uint8:
		return int(v), true
	case uint16:
		return int(v), true
	case uint32:
		return int(v), true
	case uint64:
		return int(v), true
	case float32:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(trimSpace(v))
		if err == nil {
			return n, true
		}
	}

	if n, ok := templateFloat(value); ok {
		return int(n), true
	}

	return 0, false
}

// templateFloat coerces numeric template values to float64.
func templateFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		n, err := strconv.ParseFloat(trimSpace(v), 64)
		if err == nil {
			return n, true
		}
	}

	return 0, false
}

// templateLength returns the length used by the template length filter.
func templateLength(value any) int {
	switch v := value.(type) {
	case nil:
		return 0
	case string:
		return len(v)
	case []any:
		return len(v)
	case []string:
		return len(v)
	case map[string]any:
		return len(v)
	case map[any]any:
		return len(v)
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return 0
	}

	switch rv.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map, reflect.String:
		return rv.Len()
	default:
		return len(templateStringify(value))
	}
}

// templateMinMax returns the smallest or largest item in a slice-like value.
func templateMinMax(value any, wantMax bool) (any, bool) {
	items, ok := anySliceFromValue(value)
	if !ok || len(items) == 0 {
		return nil, false
	}

	best := items[0]
	for _, item := range items[1:] {
		if templateValueGreater(item, best, wantMax) {
			best = item
		}
	}

	return best, true
}

// templateValueGreater compares values for min and max filters.
func templateValueGreater(candidate, current any, wantMax bool) bool {
	candidateFloat, candidateOK := templateFloat(candidate)
	currentFloat, currentOK := templateFloat(current)
	if candidateOK && currentOK {
		if wantMax {
			return candidateFloat > currentFloat
		}
		return candidateFloat < currentFloat
	}

	candidateStr := templateStringify(candidate)
	currentStr := templateStringify(current)
	if wantMax {
		return candidateStr > currentStr
	}
	return candidateStr < currentStr
}
