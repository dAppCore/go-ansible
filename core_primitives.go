package ansible

import (
	"unicode"
	"unicode/utf8"

	core "dappco.re/go/core"
)

type stringBuffer interface {
	Write([]byte) (int, error)
	WriteString(string) (int, error)
	String() string
}

func dirSep() string {
	ds := core.Env("DS")
	if ds == "" {
		return "/"
	}
	return ds
}

func corexAbsPath(path string) string {
	if path == "" {
		return core.Env("DIR_CWD")
	}
	if core.PathIsAbs(path) {
		return corexCleanPath(path)
	}

	cwd := core.Env("DIR_CWD")
	if cwd == "" {
		cwd = "."
	}
	return corexJoinPath(cwd, path)
}

func corexJoinPath(parts ...string) string {
	ds := dirSep()
	path := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		if path == "" {
			path = part
			continue
		}

		path = core.TrimSuffix(path, ds)
		part = core.TrimPrefix(part, ds)
		path = core.Concat(path, ds, part)
	}

	if path == "" {
		return "."
	}
	return core.CleanPath(path, ds)
}

func corexCleanPath(path string) string {
	if path == "" {
		return "."
	}
	return core.CleanPath(path, dirSep())
}

func corexPathDir(path string) string {
	return core.PathDir(path)
}

func corexPathBase(path string) string {
	return core.PathBase(path)
}

func corexPathIsAbs(path string) bool {
	return core.PathIsAbs(path)
}

func corexEnv(key string) string {
	return core.Env(key)
}

func corexSprintf(format string, args ...any) string {
	return core.Sprintf(format, args...)
}

func corexSprint(args ...any) string {
	return core.Sprint(args...)
}

func corexContains(s, substr string) bool {
	return core.Contains(s, substr)
}

func corexHasPrefix(s, prefix string) bool {
	return core.HasPrefix(s, prefix)
}

func corexHasSuffix(s, suffix string) bool {
	return core.HasSuffix(s, suffix)
}

func corexSplit(s, sep string) []string {
	return core.Split(s, sep)
}

func corexSplitN(s, sep string, n int) []string {
	return core.SplitN(s, sep, n)
}

func corexJoin(sep string, parts []string) string {
	return core.Join(sep, parts...)
}

func corexLower(s string) string {
	return core.Lower(s)
}

func corexReplaceAll(s, old, new string) string {
	return core.Replace(s, old, new)
}

func corexReplaceN(s, old, new string, n int) string {
	if n == 0 || old == "" {
		return s
	}
	if n < 0 {
		return corexReplaceAll(s, old, new)
	}

	result := s
	for i := 0; i < n; i++ {
		index := corexStringIndex(result, old)
		if index < 0 {
			break
		}
		result = core.Concat(result[:index], new, result[index+len(old):])
	}
	return result
}

func corexTrimSpace(s string) string {
	return core.Trim(s)
}

func corexTrimPrefix(s, prefix string) string {
	return core.TrimPrefix(s, prefix)
}

func corexTrimCutset(s, cutset string) string {
	start := 0
	end := len(s)

	for start < end {
		r, size := utf8.DecodeRuneInString(s[start:end])
		if !corexContainsRune(cutset, r) {
			break
		}
		start += size
	}

	for start < end {
		r, size := utf8.DecodeLastRuneInString(s[start:end])
		if !corexContainsRune(cutset, r) {
			break
		}
		end -= size
	}

	return s[start:end]
}

func corexRepeat(s string, count int) string {
	if count <= 0 {
		return ""
	}

	buf := core.NewBuilder()
	for i := 0; i < count; i++ {
		buf.WriteString(s)
	}
	return buf.String()
}

func corexFields(s string) []string {
	var out []string
	start := -1

	for i, r := range s {
		if unicode.IsSpace(r) {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
			continue
		}
		if start < 0 {
			start = i
		}
	}

	if start >= 0 {
		out = append(out, s[start:])
	}
	return out
}

func corexNewBuilder() stringBuffer {
	return core.NewBuilder()
}

func corexNewReader(s string) interface {
	Read([]byte) (int, error)
} {
	return core.NewReader(s)
}

func corexReadAllString(reader any) (string, error) {
	result := core.ReadAll(reader)
	if !result.OK {
		if err, ok := result.Value.(error); ok {
			return "", err
		}
		return "", core.NewError("read content")
	}

	if data, ok := result.Value.(string); ok {
		return data, nil
	}
	return corexSprint(result.Value), nil
}

func corexWriteString(writer interface {
	Write([]byte) (int, error)
}, value string) {
	_, _ = writer.Write([]byte(value))
}

func corexContainsRune(cutset string, target rune) bool {
	for _, candidate := range cutset {
		if candidate == target {
			return true
		}
	}
	return false
}

func corexStringIndex(s, needle string) int {
	if needle == "" {
		return 0
	}
	if len(needle) > len(s) {
		return -1
	}

	for i := 0; i+len(needle) <= len(s); i++ {
		if s[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func absPath(path string) string                                { return corexAbsPath(path) }
func joinPath(parts ...string) string                           { return corexJoinPath(parts...) }
func cleanPath(path string) string                              { return corexCleanPath(path) }
func pathDir(path string) string                                { return corexPathDir(path) }
func pathBase(path string) string                               { return corexPathBase(path) }
func pathIsAbs(path string) bool                                { return corexPathIsAbs(path) }
func env(key string) string                                     { return corexEnv(key) }
func sprintf(format string, args ...any) string                 { return corexSprintf(format, args...) }
func sprint(args ...any) string                                 { return corexSprint(args...) }
func contains(s, substr string) bool                            { return corexContains(s, substr) }
func hasSuffix(s, suffix string) bool                           { return corexHasSuffix(s, suffix) }
func split(s, sep string) []string                              { return corexSplit(s, sep) }
func splitN(s, sep string, n int) []string                      { return corexSplitN(s, sep, n) }
func join(sep string, parts []string) string                    { return corexJoin(sep, parts) }
func lower(s string) string                                     { return corexLower(s) }
func replaceAll(s, old, new string) string                      { return corexReplaceAll(s, old, new) }
func replaceN(s, old, new string, n int) string                 { return corexReplaceN(s, old, new, n) }
func trimSpace(s string) string                                 { return corexTrimSpace(s) }
func trimCutset(s, cutset string) string                        { return corexTrimCutset(s, cutset) }
func repeat(s string, count int) string                         { return corexRepeat(s, count) }
func fields(s string) []string                                  { return corexFields(s) }
func newBuilder() stringBuffer                                  { return corexNewBuilder() }
func newReader(s string) interface{ Read([]byte) (int, error) } { return corexNewReader(s) }
func readAllString(reader any) (string, error)                  { return corexReadAllString(reader) }
func writeString(writer interface{ Write([]byte) (int, error) }, value string) {
	corexWriteString(writer, value)
}
