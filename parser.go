package ansible

import (
	"iter"
	"maps"
	"slices"

	coreio "dappco.re/go/core/io"
	coreerr "dappco.re/go/core/log"
	"gopkg.in/yaml.v3"
)

// Parser handles Ansible YAML parsing.
//
// Example:
//
//	parser := NewParser("/workspace/playbooks")
type Parser struct {
	basePath string
	vars     map[string]any
}

// NewParser creates a new Ansible parser.
//
// Example:
//
//	parser := NewParser("/workspace/playbooks")
func NewParser(basePath string) *Parser {
	return &Parser{
		basePath: basePath,
		vars:     make(map[string]any),
	}
}

// ParsePlaybook parses an Ansible playbook file.
//
// Example:
//
//	plays, err := parser.ParsePlaybook("/workspace/playbooks/site.yml")
func (p *Parser) ParsePlaybook(path string) ([]Play, error) {
	return p.parsePlaybook(path, make(map[string]bool))
}

func (p *Parser) parsePlaybook(path string, seen map[string]bool) ([]Play, error) {
	cleanedPath := cleanPath(path)
	if seen[cleanedPath] {
		return nil, coreerr.E("Parser.parsePlaybook", "circular import_playbook detected: "+cleanedPath, nil)
	}
	seen[cleanedPath] = true
	defer delete(seen, cleanedPath)

	data, err := coreio.Local.Read(path)
	if err != nil {
		return nil, coreerr.E("Parser.ParsePlaybook", "read playbook", err)
	}

	var plays []Play
	if err := yaml.Unmarshal([]byte(data), &plays); err != nil {
		return nil, coreerr.E("Parser.ParsePlaybook", "parse playbook", err)
	}

	var expanded []Play

	// Process each play
	for i := range plays {
		if plays[i].ImportPlaybook != "" {
			importPath := joinPath(pathDir(path), plays[i].ImportPlaybook)
			imported, err := p.parsePlaybook(importPath, seen)
			if err != nil {
				return nil, coreerr.E("Parser.ParsePlaybook", sprintf("expand import_playbook %d", i), err)
			}
			expanded = append(expanded, imported...)
			continue
		}

		if err := p.processPlay(&plays[i]); err != nil {
			return nil, coreerr.E("Parser.ParsePlaybook", sprintf("process play %d", i), err)
		}
		expanded = append(expanded, plays[i])
	}

	return expanded, nil
}

// ParsePlaybookIter returns an iterator for plays in an Ansible playbook file.
//
// Example:
//
//	seq, err := parser.ParsePlaybookIter("/workspace/playbooks/site.yml")
func (p *Parser) ParsePlaybookIter(path string) (iter.Seq[Play], error) {
	plays, err := p.ParsePlaybook(path)
	if err != nil {
		return nil, err
	}
	return func(yield func(Play) bool) {
		for _, play := range plays {
			if !yield(play) {
				return
			}
		}
	}, nil
}

// ParseInventory parses an Ansible inventory file.
//
// Example:
//
//	inv, err := parser.ParseInventory("/workspace/inventory.yml")
func (p *Parser) ParseInventory(path string) (*Inventory, error) {
	data, err := coreio.Local.Read(path)
	if err != nil {
		return nil, coreerr.E("Parser.ParseInventory", "read inventory", err)
	}

	var inv Inventory
	if err := yaml.Unmarshal([]byte(data), &inv); err != nil {
		return nil, coreerr.E("Parser.ParseInventory", "parse inventory", err)
	}

	return &inv, nil
}

// ParseTasks parses a tasks file (used by include_tasks).
//
// Example:
//
//	tasks, err := parser.ParseTasks("/workspace/roles/web/tasks/main.yml")
func (p *Parser) ParseTasks(path string) ([]Task, error) {
	data, err := coreio.Local.Read(path)
	if err != nil {
		return nil, coreerr.E("Parser.ParseTasks", "read tasks", err)
	}

	var tasks []Task
	if err := yaml.Unmarshal([]byte(data), &tasks); err != nil {
		return nil, coreerr.E("Parser.ParseTasks", "parse tasks", err)
	}

	for i := range tasks {
		if err := p.extractModule(&tasks[i]); err != nil {
			return nil, coreerr.E("Parser.ParseTasks", sprintf("task %d", i), err)
		}
	}

	return tasks, nil
}

// ParseTasksIter returns an iterator for tasks in a tasks file.
//
// Example:
//
//	seq, err := parser.ParseTasksIter("/workspace/roles/web/tasks/main.yml")
func (p *Parser) ParseTasksIter(path string) (iter.Seq[Task], error) {
	tasks, err := p.ParseTasks(path)
	if err != nil {
		return nil, err
	}
	return func(yield func(Task) bool) {
		for _, task := range tasks {
			if !yield(task) {
				return
			}
		}
	}, nil
}

// ParseRole parses a role and returns its tasks.
//
// Example:
//
//	tasks, err := parser.ParseRole("nginx", "main.yml")
func (p *Parser) ParseRole(name string, tasksFrom string) ([]Task, error) {
	tasks, _, _, err := p.loadRoleData(name, tasksFrom)
	return tasks, err
}

func (p *Parser) loadRoleData(name string, tasksFrom string) ([]Task, map[string]any, map[string]any, error) {
	if tasksFrom == "" {
		tasksFrom = "main.yml"
	}

	// Search paths for roles (in order of precedence)
	searchPaths := []string{
		// Relative to playbook
		joinPath(p.basePath, "roles", name, "tasks", tasksFrom),
		// Parent directory roles
		joinPath(pathDir(p.basePath), "roles", name, "tasks", tasksFrom),
		// Sibling roles directory
		joinPath(p.basePath, "..", "roles", name, "tasks", tasksFrom),
		// playbooks/roles pattern
		joinPath(p.basePath, "playbooks", "roles", name, "tasks", tasksFrom),
		// Common DevOps structure
		joinPath(pathDir(pathDir(p.basePath)), "roles", name, "tasks", tasksFrom),
	}

	var tasksPath string
	for _, sp := range searchPaths {
		// Clean the path to resolve .. segments
		sp = cleanPath(sp)
		if coreio.Local.Exists(sp) {
			tasksPath = sp
			break
		}
	}

	if tasksPath == "" {
		return nil, nil, nil, coreerr.E("Parser.ParseRole", sprintf("role %s not found in search paths: %v", name, searchPaths), nil)
	}

	defaults := make(map[string]any)
	// Load role defaults
	defaultsPath := joinPath(pathDir(pathDir(tasksPath)), "defaults", "main.yml")
	if data, err := coreio.Local.Read(defaultsPath); err == nil {
		if yaml.Unmarshal([]byte(data), &defaults) != nil {
			defaults = make(map[string]any)
		}
	}

	roleVars := make(map[string]any)
	// Load role vars
	varsPath := joinPath(pathDir(pathDir(tasksPath)), "vars", "main.yml")
	if data, err := coreio.Local.Read(varsPath); err == nil {
		if yaml.Unmarshal([]byte(data), &roleVars) != nil {
			roleVars = make(map[string]any)
		}
	}

	tasks, err := p.ParseTasks(tasksPath)
	if err != nil {
		return nil, nil, nil, err
	}

	return tasks, defaults, roleVars, nil
}

// processPlay processes a play and extracts modules from tasks.
func (p *Parser) processPlay(play *Play) error {
	// Merge play vars
	for k, v := range play.Vars {
		p.vars[k] = v
	}

	for i := range play.PreTasks {
		if err := p.extractModule(&play.PreTasks[i]); err != nil {
			return coreerr.E("Parser.processPlay", sprintf("pre_task %d", i), err)
		}
	}

	for i := range play.Tasks {
		if err := p.extractModule(&play.Tasks[i]); err != nil {
			return coreerr.E("Parser.processPlay", sprintf("task %d", i), err)
		}
	}

	for i := range play.PostTasks {
		if err := p.extractModule(&play.PostTasks[i]); err != nil {
			return coreerr.E("Parser.processPlay", sprintf("post_task %d", i), err)
		}
	}

	for i := range play.Handlers {
		if err := p.extractModule(&play.Handlers[i]); err != nil {
			return coreerr.E("Parser.processPlay", sprintf("handler %d", i), err)
		}
	}

	return nil
}

// extractModule extracts the module name and args from a task.
func (p *Parser) extractModule(task *Task) error {
	// First, unmarshal the raw YAML to get all keys
	// This is a workaround since we need to find the module key dynamically

	// Handle block tasks
	for i := range task.Block {
		if err := p.extractModule(&task.Block[i]); err != nil {
			return err
		}
	}
	for i := range task.Rescue {
		if err := p.extractModule(&task.Rescue[i]); err != nil {
			return err
		}
	}
	for i := range task.Always {
		if err := p.extractModule(&task.Always[i]); err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalYAML implements custom YAML unmarshaling for Task.
//
// Example:
//
//	var task Task
//	_ = yaml.Unmarshal([]byte("shell: echo ok"), &task)
func (t *Task) UnmarshalYAML(node *yaml.Node) error {
	// First decode known fields
	type rawTask Task
	var raw rawTask

	// Create a map to capture all fields
	var m map[string]any
	if err := node.Decode(&m); err != nil {
		return err
	}

	// Decode into struct
	if err := node.Decode(&raw); err != nil {
		return err
	}
	*t = Task(raw)
	t.raw = m

	// Find the module key
	knownKeys := map[string]bool{
		"name": true, "register": true, "when": true, "loop": true,
		"loop_control": true, "vars": true, "environment": true,
		"changed_when": true, "failed_when": true, "ignore_errors": true,
		"no_log": true, "become": true, "become_user": true,
		"delegate_to": true, "run_once": true, "tags": true,
		"block": true, "rescue": true, "always": true, "notify": true,
		"retries": true, "delay": true, "until": true,
		"include_tasks": true, "import_tasks": true,
		"include_role": true, "import_role": true,
		"with_items": true, "with_dict": true, "with_file": true,
	}

	for key, val := range m {
		if knownKeys[key] {
			continue
		}

		// Check if this is a module
		if isModule(key) {
			t.Module = key
			t.Args = make(map[string]any)

			switch v := val.(type) {
			case string:
				// Free-form args (e.g., shell: echo hello)
				t.Args["_raw_params"] = v
			case map[string]any:
				t.Args = v
			case nil:
				// Module with no args
			default:
				t.Args["_raw_params"] = v
			}
			break
		}
	}

	// Handle with_items as loop
	if items, ok := m["with_items"]; ok && t.Loop == nil {
		t.Loop = items
	}

	// Handle with_dict as a loop of key/value maps.
	if dict, ok := m["with_dict"]; ok && t.Loop == nil {
		switch v := dict.(type) {
		case map[string]any:
			keys := slices.Sorted(maps.Keys(v))
			items := make([]any, 0, len(keys))
			for _, key := range keys {
				items = append(items, map[string]any{
					"key":   key,
					"value": v[key],
				})
			}
			t.Loop = items
		case map[any]any:
			keys := make([]string, 0, len(v))
			for key := range v {
				if s, ok := key.(string); ok {
					keys = append(keys, s)
				}
			}
			slices.Sort(keys)
			items := make([]any, 0, len(keys))
			for _, key := range keys {
				items = append(items, map[string]any{
					"key":   key,
					"value": v[key],
				})
			}
			t.Loop = items
		}
	}

	// Preserve with_file so the executor can resolve file contents at runtime.
	if files, ok := m["with_file"]; ok && t.WithFile == nil {
		t.WithFile = files
	}

	return nil
}

// isModule checks if a key is a known module.
func isModule(key string) bool {
	for _, m := range KnownModules {
		if key == m {
			return true
		}
		// Also check without ansible.builtin. prefix
		if corexHasPrefix(m, "ansible.builtin.") {
			if key == corexTrimPrefix(m, "ansible.builtin.") {
				return true
			}
		}
	}
	// Accept any key with dots (likely a module)
	return contains(key, ".")
}

// NormalizeModule normalizes a module name to its canonical form.
//
// Example:
//
//	module := NormalizeModule("shell")
func NormalizeModule(name string) string {
	if canonical, ok := ModuleAliases[name]; ok {
		return canonical
	}
	// Add ansible.builtin. prefix if missing
	if !contains(name, ".") {
		return "ansible.builtin." + name
	}
	return name
}

// GetHosts returns hosts matching a pattern from inventory.
//
// Example:
//
//	hosts := GetHosts(inv, "webservers")
func GetHosts(inv *Inventory, pattern string) []string {
	if pattern == "all" {
		return getAllHosts(inv.All)
	}
	if pattern == "localhost" {
		return []string{"localhost"}
	}

	if contains(pattern, ":") {
		return resolveHostPattern(inv, pattern)
	}

	// Check if it's a group name
	hosts := getGroupHosts(inv.All, pattern)
	if len(hosts) > 0 {
		return hosts
	}

	// Check if it's a specific host
	if hasHost(inv.All, pattern) {
		return []string{pattern}
	}

	// Handle patterns with : (intersection/union)
	return nil
}

func resolveHostPattern(inv *Inventory, pattern string) []string {
	if inv == nil {
		return nil
	}

	parts := split(pattern, ":")
	if len(parts) == 0 {
		return nil
	}

	current := make([]string, 0)
	initialised := false

	for _, rawPart := range parts {
		part := corexTrimSpace(rawPart)
		if part == "" {
			continue
		}

		op := byte(0)
		if part[0] == '&' || part[0] == '!' || part[0] == ',' {
			op = part[0]
			part = corexTrimSpace(part[1:])
		}

		if part == "" {
			continue
		}

		segment := resolveAtomicHostPattern(inv, part)
		if !initialised {
			current = append([]string(nil), segment...)
			initialised = true
			continue
		}

		switch op {
		case '&':
			current = intersectHosts(current, segment)
		case '!':
			current = subtractHosts(current, segment)
		default:
			current = unionHosts(current, segment)
		}
	}

	return current
}

func resolveAtomicHostPattern(inv *Inventory, pattern string) []string {
	if inv == nil {
		return nil
	}

	if pattern == "all" {
		return getAllHosts(inv.All)
	}
	if pattern == "localhost" {
		return []string{"localhost"}
	}

	hosts := getGroupHosts(inv.All, pattern)
	if len(hosts) > 0 {
		return hosts
	}

	if hasHost(inv.All, pattern) {
		return []string{pattern}
	}

	return nil
}

func unionHosts(base, extra []string) []string {
	if len(base) == 0 {
		return append([]string(nil), extra...)
	}

	seen := make(map[string]bool, len(base)+len(extra))
	result := make([]string, 0, len(base)+len(extra))
	for _, host := range base {
		if seen[host] {
			continue
		}
		seen[host] = true
		result = append(result, host)
	}
	for _, host := range extra {
		if seen[host] {
			continue
		}
		seen[host] = true
		result = append(result, host)
	}
	return result
}

func intersectHosts(base, extra []string) []string {
	if len(base) == 0 || len(extra) == 0 {
		return nil
	}

	extraSet := make(map[string]bool, len(extra))
	for _, host := range extra {
		extraSet[host] = true
	}

	result := make([]string, 0, len(base))
	for _, host := range base {
		if extraSet[host] {
			result = append(result, host)
		}
	}
	return result
}

func subtractHosts(base, extra []string) []string {
	if len(base) == 0 {
		return nil
	}
	if len(extra) == 0 {
		return append([]string(nil), base...)
	}

	extraSet := make(map[string]bool, len(extra))
	for _, host := range extra {
		extraSet[host] = true
	}

	result := make([]string, 0, len(base))
	for _, host := range base {
		if !extraSet[host] {
			result = append(result, host)
		}
	}
	return result
}

// GetHostsIter returns an iterator for hosts matching a pattern from inventory.
//
// Example:
//
//	seq := GetHostsIter(inv, "all")
func GetHostsIter(inv *Inventory, pattern string) iter.Seq[string] {
	hosts := GetHosts(inv, pattern)
	return func(yield func(string) bool) {
		for _, host := range hosts {
			if !yield(host) {
				return
			}
		}
	}
}

func getAllHosts(group *InventoryGroup) []string {
	if group == nil {
		return nil
	}

	var hosts []string
	seen := make(map[string]bool)
	collectAllHosts(group, seen, &hosts)
	return hosts
}

func collectAllHosts(group *InventoryGroup, seen map[string]bool, hosts *[]string) {
	if group == nil {
		return
	}

	// Sort keys for deterministic traversal.
	hostKeys := slices.Sorted(maps.Keys(group.Hosts))
	for _, name := range hostKeys {
		if seen[name] {
			continue
		}
		seen[name] = true
		*hosts = append(*hosts, name)
	}

	childKeys := slices.Sorted(maps.Keys(group.Children))
	for _, name := range childKeys {
		collectAllHosts(group.Children[name], seen, hosts)
	}
}

// AllHostsIter returns an iterator for all hosts in an inventory group.
//
// Example:
//
//	seq := AllHostsIter(inv.All)
func AllHostsIter(group *InventoryGroup) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, host := range getAllHosts(group) {
			if !yield(host) {
				return
			}
		}
	}
}

func getGroupHosts(group *InventoryGroup, name string) []string {
	if group == nil {
		return nil
	}

	// Check children for the group name
	if child, ok := group.Children[name]; ok {
		return getAllHosts(child)
	}

	// Recurse
	for _, child := range group.Children {
		if hosts := getGroupHosts(child, name); len(hosts) > 0 {
			return hosts
		}
	}

	return nil
}

func hasHost(group *InventoryGroup, name string) bool {
	if group == nil {
		return false
	}

	if _, ok := group.Hosts[name]; ok {
		return true
	}

	for _, child := range group.Children {
		if hasHost(child, name) {
			return true
		}
	}

	return false
}

// GetHostVars returns variables for a specific host.
//
// Example:
//
//	vars := GetHostVars(inv, "web1")
func GetHostVars(inv *Inventory, hostname string) map[string]any {
	vars := make(map[string]any)

	// Collect vars from all levels
	collectHostVars(inv.All, hostname, vars)

	return vars
}

func collectHostVars(group *InventoryGroup, hostname string, vars map[string]any) bool {
	if group == nil {
		return false
	}

	// Check if host is in this group
	found := false
	if host, ok := group.Hosts[hostname]; ok {
		found = true
		// Apply group vars first
		for k, v := range group.Vars {
			vars[k] = v
		}
		// Then host vars
		if host != nil {
			if host.AnsibleHost != "" {
				vars["ansible_host"] = host.AnsibleHost
			}
			if host.AnsiblePort != 0 {
				vars["ansible_port"] = host.AnsiblePort
			}
			if host.AnsibleUser != "" {
				vars["ansible_user"] = host.AnsibleUser
			}
			if host.AnsiblePassword != "" {
				vars["ansible_password"] = host.AnsiblePassword
			}
			if host.AnsibleSSHPrivateKeyFile != "" {
				vars["ansible_ssh_private_key_file"] = host.AnsibleSSHPrivateKeyFile
			}
			if host.AnsibleConnection != "" {
				vars["ansible_connection"] = host.AnsibleConnection
			}
			for k, v := range host.Vars {
				vars[k] = v
			}
		}
	}

	// Check children
	for _, child := range group.Children {
		if collectHostVars(child, hostname, vars) {
			// Apply this group's vars (parent vars)
			for k, v := range group.Vars {
				if _, exists := vars[k]; !exists {
					vars[k] = v
				}
			}
			found = true
		}
	}

	return found
}
