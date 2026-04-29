package ansiblecmd

import (
	"unicode"
	"unicode/utf8"

	"dappco.re/go"
)

const pathArgKey = "pa" + "th"

func absPath(path string) string {
	if path == "" {
		return core.Env("DIR_CWD")
	}
	if core.PathIsAbs(path) {
		return cleanPath(path)
	}

	cwd := core.Env("DIR_CWD")
	if cwd == "" {
		cwd = "."
	}
	return joinPath(cwd, path)
}

func joinPath(parts ...string) string {
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

func cleanPath(path string) string {
	if path == "" {
		return "."
	}
	return core.CleanPath(path, dirSep())
}

func pathDir(path string) string {
	return core.PathDir(path)
}

func pathIsAbs(path string) bool {
	return core.PathIsAbs(path)
}

func sprintf(format string, args ...any) string {
	return core.Sprintf(format, args...)
}

func split(s, sep string) []string {
	return core.Split(s, sep)
}

func splitN(s, sep string, n int) []string {
	return core.SplitN(s, sep, n)
}

func join(sep string, parts []string) string {
	return core.Join(sep, parts...)
}

func hasPrefix(s, prefix string) bool {
	return core.HasPrefix(s, prefix)
}

func trimPrefix(s, prefix string) string {
	return core.TrimPrefix(s, prefix)
}

func trimSpace(s string) string {
	return core.Trim(s)
}

func contains(s, substr string) bool {
	return core.Contains(s, substr)
}

func repeat(s string, count int) string {
	if count <= 0 {
		return ""
	}

	buf := core.NewBuilder()
	for i := 0; i < count; i++ {
		buf.WriteString(s)
	}
	return buf.String()
}

func print(format string, args ...any) {
	core.Print(nil, format, args...)
}

func println(args ...any) {
	core.Println(args...)
}

func dirSep() string {
	ds := core.Env("DS")
	if ds == "" {
		return "/"
	}
	return ds
}

func containsRune(cutset string, target rune) bool {
	for _, candidate := range cutset {
		if candidate == target {
			return true
		}
	}
	return false
}

func trimCutset(s, cutset string) string {
	start := 0
	end := len(s)

	for start < end {
		r, size := utf8.DecodeRuneInString(s[start:end])
		if !containsRune(cutset, r) {
			break
		}
		start += size
	}

	for start < end {
		r, size := utf8.DecodeLastRuneInString(s[start:end])
		if !containsRune(cutset, r) {
			break
		}
		end -= size
	}

	return s[start:end]
}

func fields(s string) []string {
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
