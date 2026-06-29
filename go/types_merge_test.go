package ansible

import (
	core "dappco.re/go"
)

// --- mergeInventoryGroups ---

func TestTypesMerge_MergeInventoryGroups_Good_MergesAllSections(t *core.T) {
	dst := &InventoryGroup{
		Hosts: map[string]*Host{"web1": {AnsibleHost: "10.0.0.1"}},
		Vars:  map[string]any{"env": "prod"},
	}
	src := &InventoryGroup{
		Hosts:    map[string]*Host{"web2": {AnsibleHost: "10.0.0.2"}},
		Children: map[string]*InventoryGroup{"db": {Vars: map[string]any{"role": "db"}}},
		Vars:     map[string]any{"region": "eu"},
	}

	mergeInventoryGroups(dst, src)

	core.AssertEqual(t, "10.0.0.1", dst.Hosts["web1"].AnsibleHost)
	core.AssertEqual(t, "10.0.0.2", dst.Hosts["web2"].AnsibleHost)
	core.AssertEqual(t, "prod", dst.Vars["env"])
	core.AssertEqual(t, "eu", dst.Vars["region"])
	core.AssertNotNil(t, dst.Children["db"])
}

func TestTypesMerge_MergeInventoryGroups_Bad_NilOperandsNoPanic(t *core.T) {
	dst := &InventoryGroup{}
	// Neither nil operand should mutate or panic.
	core.AssertNotPanics(t, func() { mergeInventoryGroups(nil, dst) })
	core.AssertNotPanics(t, func() { mergeInventoryGroups(dst, nil) })
}

func TestTypesMerge_MergeInventoryGroups_Ugly_EmptyDstMapsAllocated(t *core.T) {
	// dst starts with all nil maps; merge must allocate them from src.
	dst := &InventoryGroup{}
	src := &InventoryGroup{
		Hosts:    map[string]*Host{"h": {AnsibleHost: "1.1.1.1"}},
		Children: map[string]*InventoryGroup{"c": {}},
		Vars:     map[string]any{"k": "v"},
	}

	mergeInventoryGroups(dst, src)

	core.AssertEqual(t, "1.1.1.1", dst.Hosts["h"].AnsibleHost)
	core.AssertNotNil(t, dst.Children["c"])
	core.AssertEqual(t, "v", dst.Vars["k"])
}
