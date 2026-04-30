package ansible

import (
	"io/fs"

	core "dappco.re/go"
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

func writeTextFile(t *core.T, path string, data string) {
	t.Helper()
	core.RequireNoError(t, writeTestFile(path, []byte(data), 0o644))
}

func statTestFile(path string) (core.FsFileInfo, error) {
	stat := core.Stat(path)
	if stat.OK {
		return stat.Value.(core.FsFileInfo), nil
	}
	err, _ := stat.Value.(error)
	return nil, err
}

func mkdirAllTest(path string, mode core.FileMode) error {
	result := core.MkdirAll(path, mode)
	if result.OK {
		return nil
	}
	err, _ := result.Value.(error)
	return err
}

func testInventory() *Inventory {
	return &Inventory{
		All: &InventoryGroup{
			Hosts: map[string]*Host{
				"web1": {AnsibleHost: "10.0.0.1"},
			},
			Children: map[string]*InventoryGroup{
				"db": {
					Hosts: map[string]*Host{
						"db1": {AnsibleHost: "10.0.0.2"},
					},
					Vars: map[string]any{"tier": "database"},
				},
			},
			Vars: map[string]any{"env": "test"},
		},
		HostVars: map[string]map[string]any{
			"db1": {"role": "primary"},
		},
	}
}

func joinStrings(parts []string, sep string) string {
	return join(sep, parts)
}
