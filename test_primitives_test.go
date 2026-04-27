package ansible

import (
	"io/fs"

	coreio "dappco.re/go/io"
)

func readTestFile(path string) ([]byte, error) {
	content, err := coreio.Local.Read(path)
	if err != nil {
		return nil, err
	}
	return []byte(content), nil
}

func writeTestFile(path string, content []byte, mode fs.FileMode) error {
	return coreio.Local.WriteMode(path, string(content), mode)
}

func joinStrings(parts []string, sep string) string {
	return join(sep, parts)
}
