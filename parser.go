package ansible

import (
	"io/fs"
	"iter"
	"maps"
	"reflect"
	"slices"
	"sort"
	"sync"

	coreio "dappco.re/go/io"
	coreerr "dappco.re/go/log"
	"gopkg.in/yaml.v3"
)

// Parser handles Ansible YAML parsing.
//
// Example:
//
//	parser := NewParser("/workspace/playbooks")
type Parser struct {
	basePath string
	mediumMu sync.RWMutex
	medium   coreio.Medium
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

// SetMedium configures the storage medium used for reading parser inputs.
//
// Example:
//
//	parser.SetMedium(io.Local)
func (p *Parser) SetMedium(medium coreio.Medium) {
	if p == nil {
		return
	}
	p.mediumMu.Lock()
	defer p.mediumMu.Unlock()
	p.medium = medium
}

// ParsePlaybook parses an Ansible playbook file.
//
// Example:
//
//	plays, err := parser.ParsePlaybook("/workspace/playbooks/site.yml")
func (p *Parser) ParsePlaybook(path string) ([]Play, error) {
	path = p.resolvePath(path)

	if p.vars == nil {
		p.vars = make(map[string]any)
	}

	savedPlaybookDir, hadPlaybookDir := p.vars["playbook_dir"]
	p.vars["playbook_dir"] = pathDir(path)
	defer func() {
		if hadPlaybookDir {
			p.vars["playbook_dir"] = savedPlaybookDir
		} else {
			delete(p.vars, "playbook_dir")
		}
	}()

	return p.parsePlaybook(path, make(map[string]bool))
}

func (p *Parser) parsePlaybook(path string, seen map[string]bool) ([]Play, error) {
	cleanedPath := cleanPath(path)
	if seen[cleanedPath] {
		return nil, coreerr.E("Parser.parsePlaybook", "circular import_playbook detected: "+cleanedPath, nil)
	}
	seen[cleanedPath] = true
	defer delete(seen, cleanedPath)

	data, err := p.readFile(path)
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
			importPlaybook := p.templatePath(plays[i].ImportPlaybook)
			importPath := importPlaybook
			if importPath != "" && !pathIsAbs(importPath) {
				importPath = joinPath(pathDir(path), importPath)
			}
			savedPlaybookDir, hadPlaybookDir := p.vars["playbook_dir"]
			p.vars["playbook_dir"] = pathDir(importPath)
			imported, err := func() ([]Play, error) {
				defer func() {
					if hadPlaybookDir {
						p.vars["playbook_dir"] = savedPlaybookDir
					} else {
						delete(p.vars, "playbook_dir")
					}
				}()
				return p.parsePlaybook(importPath, seen)
			}()
			if err != nil {
				return nil, coreerr.E("Parser.ParsePlaybook", sprintf("expand import_playbook %d", i), err)
			}
			for i := range imported {
				if imported[i].Vars == nil {
					imported[i].Vars = make(map[string]any)
				}
				imported[i].Vars["playbook_dir"] = savedPlaybookDir
			}
			expanded = append(expanded, imported...)
			continue
		}

		if plays[i].Vars == nil {
			plays[i].Vars = make(map[string]any)
		}
		if _, ok := plays[i].Vars["playbook_dir"]; !ok {
			plays[i].Vars["playbook_dir"] = p.vars["playbook_dir"]
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
//	playsSeq, err := parser.ParsePlaybookIter("/workspace/playbooks/site.yml")
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
//	inventory, err := parser.ParseInventory("/workspace/inventory.yml")
func (p *Parser) ParseInventory(path string) (*Inventory, error) {
	path = p.resolveInventoryPath(path)

	data, err := p.readFile(path)
	if err != nil {
		return nil, coreerr.E("Parser.ParseInventory", "read inventory", err)
	}

	var inv Inventory
	if err := yaml.Unmarshal([]byte(data), &inv); err != nil {
		return nil, coreerr.E("Parser.ParseInventory", "parse inventory", err)
	}

	return &inv, nil
}

// resolveInventoryPath resolves inventory directories to a concrete file.
func (p *Parser) resolveInventoryPath(path string) string {
	path = p.resolvePath(path)
	if path == "" || !p.exists(path) || !p.isDir(path) {
		return path
	}

	for _, name := range []string{"inventory.yml", "hosts.yml", "inventory.yaml", "hosts.yaml"} {
		candidate := joinPath(path, name)
		if p.exists(candidate) {
			return candidate
		}
	}

	return path
}

// ParseTasks parses a tasks file (used by include_tasks).
//
// Example:
//
//	tasks, err := parser.ParseTasks("/workspace/roles/web/tasks/main.yml")
func (p *Parser) ParseTasks(path string) ([]Task, error) {
	path = p.resolvePath(path)

	data, err := p.readFile(path)
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

// ParseTasksFromDir loads tasks from a directory, falling back to main.yml.
//
// Example:
//
//	tasks, err := parser.ParseTasksFromDir("/workspace/roles/web/tasks")
func (p *Parser) ParseTasksFromDir(dir string) ([]Task, error) {
	dir = p.resolvePath(dir)
	if dir == "" {
		return nil, coreerr.E("Parser.ParseTasksFromDir", "directory required", nil)
	}

	if !p.exists(dir) {
		return nil, coreerr.E("Parser.ParseTasksFromDir", "tasks directory not found", nil)
	}

	if !p.isDir(dir) {
		return p.ParseTasks(dir)
	}

	for _, name := range []string{"main.yml", "main.yaml", "tasks.yml", "tasks.yaml"} {
		candidate := joinPath(dir, name)
		if p.exists(candidate) {
			return p.ParseTasks(candidate)
		}
	}

	return nil, coreerr.E("Parser.ParseTasksFromDir", "no task file found in directory", nil)
}

// ParseVarsFiles loads and merges vars from one or more files.
//
// Example:
//
//	vars, err := parser.ParseVarsFiles("/workspace/group_vars/*.yml")
func (p *Parser) ParseVarsFiles(pattern string) (map[string]any, error) {
	pattern = p.resolvePath(pattern)
	if pattern == "" {
		return nil, nil
	}

	matches, err := p.expandFilePattern(pattern)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		if !containsAny(pattern, "*?[") {
			matches = []string{pattern}
		} else {
			return nil, coreerr.E("Parser.ParseVarsFiles", "no vars files matched pattern", nil)
		}
	}

	merged := make(map[string]any)
	for _, file := range matches {
		data, err := p.readFile(file)
		if err != nil {
			return nil, coreerr.E("Parser.ParseVarsFiles", "read vars file", err)
		}

		var vars map[string]any
		if err := yaml.Unmarshal([]byte(data), &vars); err != nil {
			return nil, coreerr.E("Parser.ParseVarsFiles", "parse vars file", err)
		}
		mergeVars(merged, vars, false)
	}

	return merged, nil
}

// ParseRoles loads role definitions from a roles directory.
//
// Example:
//
//	roles, err := parser.ParseRoles("/workspace/roles")
func (p *Parser) ParseRoles(roleDir string) (map[string]*Role, error) {
	roleDir = p.resolvePath(roleDir)
	if roleDir == "" || !p.exists(roleDir) || !p.isDir(roleDir) {
		return nil, coreerr.E("Parser.ParseRoles", "role directory not found", nil)
	}

	entries, err := p.listDir(roleDir)
	if err != nil {
		return nil, coreerr.E("Parser.ParseRoles", "list role directory", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	roles := make(map[string]*Role, len(names))
	for _, name := range names {
		role, err := p.parseRoleAtPath(joinPath(roleDir, name), name)
		if err != nil {
			return nil, err
		}
		if role != nil {
			roles[name] = role
		}
	}

	return roles, nil
}

// resolvePath resolves a possibly relative path against the parser base path.
func (p *Parser) resolvePath(path string) string {
	if path == "" || pathIsAbs(path) || p.basePath == "" {
		return path
	}

	candidates := []string{
		joinPath(p.basePath, path),
		path,
	}
	for _, candidate := range candidates {
		if p.exists(candidate) {
			return candidate
		}
	}

	return joinPath(p.basePath, path)
}

// mediumOrLocal returns the configured storage medium or the OS filesystem.
func (p *Parser) mediumOrLocal() coreio.Medium {
	if medium := p.configuredMedium(); medium != nil {
		return medium
	}
	return coreio.Local
}

// configuredMedium returns the parser medium under read lock.
func (p *Parser) configuredMedium() coreio.Medium {
	if p == nil {
		return nil
	}
	p.mediumMu.RLock()
	defer p.mediumMu.RUnlock()
	return p.medium
}

// readFile reads a file through the configured medium.
func (p *Parser) readFile(path string) (string, error) {
	medium := p.mediumOrLocal()
	if medium == nil {
		return "", coreerr.E("Parser.readFile", "no storage medium configured", nil)
	}
	return coreio.Read(medium, path)
}

// exists checks for a path through the configured medium.
func (p *Parser) exists(path string) bool {
	medium := p.mediumOrLocal()
	if medium == nil {
		return false
	}
	return medium.Exists(path)
}

// isDir reports whether a path is a directory in the configured medium.
func (p *Parser) isDir(path string) bool {
	medium := p.mediumOrLocal()
	if medium == nil {
		return false
	}
	return medium.IsDir(path)
}

// listDir lists directory entries through the configured medium.
func (p *Parser) listDir(path string) ([]fs.DirEntry, error) {
	medium := p.mediumOrLocal()
	if medium == nil {
		return nil, coreerr.E("Parser.listDir", "no storage medium configured", nil)
	}

	entries, err := medium.List(path)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// expandFilePattern expands wildcard paths that can be safely resolved locally.
func (p *Parser) expandFilePattern(pattern string) ([]string, error) {
	if !containsAny(pattern, "*?[") {
		return []string{pattern}, nil
	}

	if medium := p.configuredMedium(); medium != nil && !isDefaultLocalMedium(medium) {
		return nil, coreerr.E("Parser.expandFilePattern", "wildcard patterns require the local filesystem medium", nil)
	}

	matches := pathGlob(pattern)
	sort.Strings(matches)
	return matches, nil
}

// isDefaultLocalMedium reports whether a medium is the package-level local medium.
func isDefaultLocalMedium(medium coreio.Medium) bool {
	if medium == nil || coreio.Local == nil {
		return medium == nil && coreio.Local == nil
	}

	mediumValue := reflect.ValueOf(medium)
	localValue := reflect.ValueOf(coreio.Local)
	if !mediumValue.IsValid() || !localValue.IsValid() || mediumValue.Type() != localValue.Type() {
		return false
	}
	if mediumValue.Kind() != reflect.Pointer {
		return false
	}
	if mediumValue.IsNil() || localValue.IsNil() {
		return mediumValue.IsNil() && localValue.IsNil()
	}
	return mediumValue.Pointer() == localValue.Pointer()
}

// parseRoleAtPath loads a role from a concrete role directory.
func (p *Parser) parseRoleAtPath(rolePath, roleName string) (*Role, error) {
	tasks, defaults, roleVars, handlers, err := p.loadRoleDataFromPath(rolePath, "main.yml", "main.yml", "main.yml", "main.yml")
	if err != nil {
		return nil, err
	}

	return &Role{
		Name:     roleName,
		Path:     rolePath,
		Tasks:    tasks,
		Defaults: defaults,
		Vars:     roleVars,
		Handlers: handlers,
	}, nil
}

// templatePath renders a path-like string against the parser's variable scope.
func (p *Parser) templatePath(value string) string {
	if value == "" {
		return ""
	}

	executor := &Executor{vars: p.vars}
	return executor.templateString(value, "", nil)
}

// ParseTasksIter returns an iterator for tasks in a tasks file.
//
// Example:
//
//	tasksSeq, err := parser.ParseTasksIter("/workspace/roles/web/tasks/main.yml")
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
	tasks, defaults, roleVars, _, err := p.loadRoleData(name, tasksFrom, "", "")
	if err != nil {
		return nil, err
	}

	if p.vars == nil {
		p.vars = make(map[string]any)
	}
	for k, v := range defaults {
		if _, exists := p.vars[k]; !exists {
			p.vars[k] = v
		}
	}
	for k, v := range roleVars {
		p.vars[k] = v
	}

	return tasks, err
}

func (p *Parser) loadRoleData(name string, tasksFrom string, defaultsFrom string, varsFrom string) ([]Task, map[string]any, map[string]any, string, error) {
	if tasksFrom == "" {
		tasksFrom = "main.yml"
	}
	if defaultsFrom == "" {
		defaultsFrom = "main.yml"
	}
	if varsFrom == "" {
		varsFrom = "main.yml"
	}

	tasksPath := p.findRoleFilePath(name, "tasks", tasksFrom)

	if tasksPath == "" {
		return nil, nil, nil, "", coreerr.E("Parser.ParseRole", sprintf("role %s not found", name), nil)
	}

	defaults := make(map[string]any)
	// Load role defaults
	defaultsPath := joinPath(pathDir(pathDir(tasksPath)), "defaults", defaultsFrom)
	if data, err := p.readFile(defaultsPath); err == nil {
		if yaml.Unmarshal([]byte(data), &defaults) != nil {
			defaults = make(map[string]any)
		}
	}

	roleVars := make(map[string]any)
	// Load role vars
	varsPath := joinPath(pathDir(pathDir(tasksPath)), "vars", varsFrom)
	if data, err := p.readFile(varsPath); err == nil {
		if yaml.Unmarshal([]byte(data), &roleVars) != nil {
			roleVars = make(map[string]any)
		}
	}

	tasks, err := p.ParseTasks(tasksPath)
	if err != nil {
		return nil, nil, nil, "", err
	}

	return tasks, defaults, roleVars, tasksPath, nil
}

// loadRoleDataFromPath loads role files from a concrete directory path.
func (p *Parser) loadRoleDataFromPath(rolePath string, tasksFrom string, defaultsFrom string, varsFrom string, handlersFrom string) ([]Task, map[string]any, map[string]any, []Task, error) {
	if rolePath == "" {
		return nil, nil, nil, nil, coreerr.E("Parser.ParseRoles", "role path required", nil)
	}

	if tasksFrom == "" {
		tasksFrom = "main.yml"
	}
	if defaultsFrom == "" {
		defaultsFrom = "main.yml"
	}
	if varsFrom == "" {
		varsFrom = "main.yml"
	}

	tasks := make([]Task, 0)
	var err error
	taskPath := joinPath(rolePath, "tasks", tasksFrom)
	if !p.exists(taskPath) {
		taskPath = joinPath(rolePath, "tasks")
	}
	if p.exists(taskPath) {
		if p.isDir(taskPath) {
			tasks, err = p.ParseTasksFromDir(taskPath)
		} else {
			tasks, err = p.ParseTasks(taskPath)
		}
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	defaults := make(map[string]any)
	if data, err := p.readFile(joinPath(rolePath, "defaults", defaultsFrom)); err == nil {
		if err := yaml.Unmarshal([]byte(data), &defaults); err != nil {
			defaults = make(map[string]any)
		}
	}

	roleVars := make(map[string]any)
	if data, err := p.readFile(joinPath(rolePath, "vars", varsFrom)); err == nil {
		if err := yaml.Unmarshal([]byte(data), &roleVars); err != nil {
			roleVars = make(map[string]any)
		}
	}

	handlers, err := p.loadRoleHandlersFromPath(rolePath, handlersFrom)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return tasks, defaults, roleVars, handlers, nil
}

func (p *Parser) loadRoleHandlers(name string, handlersFrom string) ([]Task, error) {
	if handlersFrom == "" {
		handlersFrom = "main.yml"
	}

	handlersPath := p.findRoleFilePath(name, "handlers", handlersFrom)
	if handlersPath == "" {
		return nil, nil
	}

	data, err := p.readFile(handlersPath)
	if err != nil {
		return nil, coreerr.E("Parser.loadRoleHandlers", "read role handlers", err)
	}

	var handlers []Task
	if err := yaml.Unmarshal([]byte(data), &handlers); err != nil {
		return nil, coreerr.E("Parser.loadRoleHandlers", "parse role handlers", err)
	}

	for i := range handlers {
		if err := p.extractModule(&handlers[i]); err != nil {
			return nil, coreerr.E("Parser.loadRoleHandlers", sprintf("handler %d", i), err)
		}
	}

	return handlers, nil
}

// loadRoleHandlersFromPath loads handler tasks from a concrete role directory.
func (p *Parser) loadRoleHandlersFromPath(rolePath string, handlersFrom string) ([]Task, error) {
	if handlersFrom == "" {
		handlersFrom = "main.yml"
	}
	handlersPath := joinPath(rolePath, "handlers", handlersFrom)
	if !p.exists(handlersPath) {
		return nil, nil
	}
	data, err := p.readFile(handlersPath)
	if err != nil {
		return nil, coreerr.E("Parser.loadRoleHandlersFromPath", "read role handlers", err)
	}
	var handlers []Task
	if err := yaml.Unmarshal([]byte(data), &handlers); err != nil {
		return nil, coreerr.E("Parser.loadRoleHandlersFromPath", "parse role handlers", err)
	}
	for i := range handlers {
		if err := p.extractModule(&handlers[i]); err != nil {
			return nil, coreerr.E("Parser.loadRoleHandlersFromPath", sprintf("handler %d", i), err)
		}
	}
	return handlers, nil
}

func (p *Parser) findRoleFilePath(name string, subdir string, filename string) string {
	searchPaths := []string{
		joinPath(p.basePath, "roles", name, subdir, filename),
		joinPath(pathDir(p.basePath), "roles", name, subdir, filename),
		joinPath(p.basePath, "..", "roles", name, subdir, filename),
		joinPath(p.basePath, "playbooks", "roles", name, subdir, filename),
		joinPath(pathDir(pathDir(p.basePath)), "roles", name, subdir, filename),
	}

	for _, sp := range searchPaths {
		sp = cleanPath(sp)
		if coreio.Local.Exists(sp) {
			return sp
		}
	}

	return ""
}

// processPlay processes a play and extracts modules from tasks.
func (p *Parser) processPlay(
	play *Play,
) error {
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
func (p *Parser) extractModule(
	task *Task,
) error {
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
func (t *Task) UnmarshalYAML(
	node *yaml.Node,
) error {
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
		"check_mode": true, "diff": true,
		"delegate_to": true, "delegate_facts": true, "run_once": true, "tags": true,
		"block": true, "rescue": true, "always": true, "notify": true, "listen": true,
		"module_defaults": true,
		"retries":         true, "delay": true, "until": true,
		"async": true, "poll": true,
		"action": true, "local_action": true,
		"ansible.builtin.action": true, "ansible.legacy.action": true,
		"ansible.builtin.local_action": true, "ansible.legacy.local_action": true,
		"include_tasks": true, "import_tasks": true,
		"ansible.builtin.include_tasks": true, "ansible.legacy.include_tasks": true,
		"ansible.builtin.import_tasks": true, "ansible.legacy.import_tasks": true,
		"apply":        true,
		"include_role": true, "import_role": true, "public": true,
		"ansible.builtin.include_role": true, "ansible.legacy.include_role": true,
		"ansible.builtin.import_role": true, "ansible.legacy.import_role": true,
		"with_items": true, "with_dict": true, "with_indexed_items": true, "with_nested": true, "with_together": true, "with_subelements": true, "with_file": true, "with_fileglob": true, "with_sequence": true,
	}

	if value, ok := directiveValue(m, "include_tasks"); ok && t.IncludeTasks == "" {
		t.IncludeTasks = sprintf("%v", value)
	}
	if value, ok := directiveValue(m, "import_tasks"); ok && t.ImportTasks == "" {
		t.ImportTasks = sprintf("%v", value)
	}
	if value, ok := directiveValue(m, "include_role"); ok && t.IncludeRole == nil {
		var ref RoleRef
		if err := decodeYAMLValue(value, &ref); err != nil {
			return err
		}
		t.IncludeRole = &ref
	}
	if value, ok := directiveValue(m, "import_role"); ok && t.ImportRole == nil {
		var ref RoleRef
		if err := decodeYAMLValue(value, &ref); err != nil {
			return err
		}
		t.ImportRole = &ref
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

	// Handle with_indexed_items as a loop of [index, value] pairs.
	if indexed, ok := m["with_indexed_items"]; ok && t.Loop == nil {
		switch v := indexed.(type) {
		case []any:
			items := make([]any, 0, len(v))
			for i, item := range v {
				items = append(items, []any{i, item})
			}
			t.Loop = items
		case []string:
			items := make([]any, 0, len(v))
			for i, item := range v {
				items = append(items, []any{i, item})
			}
			t.Loop = items
		}
	}

	// Handle with_nested as a cartesian product of input lists.
	if nested, ok := m["with_nested"]; ok && t.Loop == nil {
		if items := expandNestedLoop(nested); len(items) > 0 {
			t.Loop = items
		}
	}

	// Preserve with_file so the executor can resolve file contents at runtime.
	if files, ok := m["with_file"]; ok && t.WithFile == nil {
		t.WithFile = files
	}

	// Preserve with_fileglob so the executor can expand glob patterns at runtime.
	if files, ok := m["with_fileglob"]; ok && t.WithFileGlob == nil {
		t.WithFileGlob = files
	}

	// Preserve with_sequence so the executor can expand numeric ranges at runtime.
	if sequence, ok := m["with_sequence"]; ok && t.WithSequence == nil {
		t.WithSequence = sequence
	}

	// Preserve with_together so the executor can zip legacy loop inputs at runtime.
	if together, ok := m["with_together"]; ok && t.WithTogether == nil {
		t.WithTogether = together
	}

	// Preserve with_subelements so the executor can expand parent/child pairs at runtime.
	if subelements, ok := m["with_subelements"]; ok && t.WithSubelements == nil {
		t.WithSubelements = subelements
	}

	// Expand with_together immediately so existing loop code sees the legacy shape.
	if t.WithTogether != nil && t.Loop == nil {
		t.Loop = expandTogetherLoop(t.WithTogether)
	}

	// Support legacy action/local_action shorthands.
	if t.Module == "" {
		if localAction, ok := directiveValue(m, "local_action"); ok {
			if module, args := parseActionSpec(localAction); module != "" {
				t.Module = module
				t.Args = args
				t.Delegate = "localhost"
			}
		}
	}
	if t.Module == "" {
		if action, ok := directiveValue(m, "action"); ok {
			if module, args := parseActionSpec(action); module != "" {
				t.Module = module
				t.Args = args
			}
		}
	}

	return nil
}

func decodeYAMLValue(
	value any, out any,
) error {
	data, err := yaml.Marshal(value)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, out)
}

// parseActionSpec converts action/local_action values into a module name and
// argument map.
func parseActionSpec(value any) (string, map[string]any) {
	switch v := value.(type) {
	case string:
		return parseActionSpecString(v)
	case map[string]any:
		module := getStringArg(v, "module", "")
		if module == "" {
			module = getStringArg(v, "_raw_params", "")
		}

		args := make(map[string]any)
		for key, val := range v {
			if key == "module" || key == "_raw_params" {
				continue
			}
			args[key] = val
		}

		if raw, ok := v["_raw_params"]; ok {
			args["_raw_params"] = raw
		}

		if module == "" {
			return "", nil
		}
		if len(args) == 0 {
			args = nil
		}
		return module, args
	default:
		return parseActionSpecString(sprintf("%v", value))
	}
}

func parseActionSpecString(raw string) (string, map[string]any) {
	raw = trimSpace(raw)
	if raw == "" {
		return "", nil
	}

	parts := fields(raw)
	if len(parts) == 0 {
		return "", nil
	}

	module := ""
	start := 0
	if kv := splitN(parts[0], "=", 2); len(kv) == 2 && kv[0] == "module" && kv[1] != "" {
		module = kv[1]
		start = 1
	} else {
		module = parts[0]
		start = 1
	}
	if module == "" {
		return "", nil
	}

	if start >= len(parts) {
		return module, nil
	}

	args := make(map[string]any)
	freeFormStart := len(parts)
	for i, part := range parts[start:] {
		key, value, ok := cut(part, "=")
		if !ok || key == "" {
			freeFormStart = start + i
			break
		}
		args[key] = value
	}

	if freeFormStart == len(parts) {
		if len(args) > 0 {
			return module, args
		}
		return module, nil
	}

	if freeFormStart < len(parts) {
		rawParams := join(" ", parts[freeFormStart:])
		if rawParams != "" {
			if len(args) == 0 {
				return module, map[string]any{"_raw_params": rawParams}
			}
			args["_raw_params"] = rawParams
		}
	}

	if len(args) == 0 {
		return module, nil
	}

	return module, args
}

// expandNestedLoop converts with_nested input into a loop of cartesian
// product items. Each output item is a slice containing one value from each
// nested list.
func expandNestedLoop(loop any) []any {
	groups, ok := nestedLoopGroups(loop)
	if !ok || len(groups) == 0 {
		return nil
	}

	products := [][]any{{}}
	for _, group := range groups {
		if len(group) == 0 {
			return nil
		}

		next := make([][]any, 0, len(products)*len(group))
		for _, prefix := range products {
			for _, item := range group {
				combo := make([]any, len(prefix)+1)
				copy(combo, prefix)
				combo[len(prefix)] = item
				next = append(next, combo)
			}
		}
		products = next
	}

	items := make([]any, len(products))
	for i, combo := range products {
		items[i] = combo
	}
	return items
}

// expandTogetherLoop converts with_together input into a zipped loop. Each
// output item contains one value from each input group at the same index.
func expandTogetherLoop(loop any) []any {
	groups, ok := togetherLoopGroups(loop)
	if !ok || len(groups) == 0 {
		return nil
	}

	minLen := len(groups[0])
	for _, group := range groups[1:] {
		if len(group) < minLen {
			minLen = len(group)
		}
	}
	if minLen == 0 {
		return nil
	}

	items := make([]any, 0, minLen)
	for i := 0; i < minLen; i++ {
		combo := make([]any, len(groups))
		for j, group := range groups {
			combo[j] = group[i]
		}
		items = append(items, combo)
	}
	return items
}

func nestedLoopGroups(loop any) ([][]any, bool) {
	switch v := loop.(type) {
	case []any:
		groups := make([][]any, 0, len(v))
		for _, group := range v {
			items := nestedLoopItems(group)
			if len(items) == 0 {
				return nil, false
			}
			groups = append(groups, items)
		}
		return groups, true
	case []string:
		items := make([]any, 0, len(v))
		for _, item := range v {
			items = append(items, item)
		}
		if len(items) == 0 {
			return nil, false
		}
		return [][]any{items}, true
	case string:
		if v == "" {
			return nil, false
		}
		return [][]any{{v}}, true
	default:
		items := nestedLoopItems(v)
		if len(items) == 0 {
			return nil, false
		}
		return [][]any{items}, true
	}
}

func nestedLoopItems(value any) []any {
	switch v := value.(type) {
	case nil:
		return nil
	case []any:
		items := make([]any, len(v))
		for i, item := range v {
			items[i] = item
		}
		return items
	case []string:
		items := make([]any, len(v))
		for i, item := range v {
			items[i] = item
		}
		return items
	default:
		return []any{v}
	}
}

func togetherLoopGroups(loop any) ([][]any, bool) {
	switch v := loop.(type) {
	case []any:
		groups := make([][]any, 0, len(v))
		for _, group := range v {
			items := nestedLoopItems(group)
			if len(items) == 0 {
				return nil, false
			}
			groups = append(groups, items)
		}
		return groups, true
	case []string:
		items := make([]any, len(v))
		for i, item := range v {
			items[i] = item
		}
		if len(items) == 0 {
			return nil, false
		}
		return [][]any{items}, true
	case string:
		if v == "" {
			return nil, false
		}
		return [][]any{{v}}, true
	default:
		items := nestedLoopItems(v)
		if len(items) == 0 {
			return nil, false
		}
		return [][]any{items}, true
	}
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

// NormalizeModule normalises a module name to its canonical form.
//
// Example:
//
//	module := NormalizeModule("shell")
func NormalizeModule(name string) string {
	if canonical, ok := ModuleAliases[name]; ok {
		return canonical
	}
	if hasPrefix(name, "ansible.legacy.") {
		// Legacy module names should resolve through the existing short-form
		// and alias logic so we keep compatibility with older playbooks.
		return NormalizeModule(trimPrefix(name, "ansible.legacy."))
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
//	parser := NewParser("/workspace/inventory")
//	inventory, _ := parser.ParseInventory("/workspace/inventory/hosts.yml")
//	hosts := GetHosts(inventory, "webservers")
func GetHosts(inventory *Inventory, pattern string) []string {
	if pattern == "all" {
		return getAllHosts(inventory.All)
	}
	if pattern == "localhost" {
		return []string{"localhost"}
	}

	if contains(pattern, ":") {
		return resolveHostPattern(inventory, pattern)
	}

	// Check if it's a group name
	hosts := getGroupHosts(inventory.All, pattern)
	if len(hosts) > 0 {
		return hosts
	}

	// Check if it's a specific host
	if hasHost(inventory.All, pattern) {
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
//	parser := NewParser("/workspace/inventory")
//	inventory, _ := parser.ParseInventory("/workspace/inventory/hosts.yml")
//	hostsSeq := GetHostsIter(inventory, "all")
func GetHostsIter(inventory *Inventory, pattern string) iter.Seq[string] {
	hosts := GetHosts(inventory, pattern)
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
//	parser := NewParser("/workspace/inventory")
//	inventory, _ := parser.ParseInventory("/workspace/inventory/hosts.yml")
//	hostsSeq := AllHostsIter(inventory.All)
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
//	parser := NewParser("/workspace/inventory")
//	inventory, _ := parser.ParseInventory("/workspace/inventory/hosts.yml")
//	hostVars := GetHostVars(inventory, "web1")
func GetHostVars(inventory *Inventory, hostname string) map[string]any {
	vars := make(map[string]any)
	if inventory == nil {
		return vars
	}

	// Collect vars from all levels
	collectHostVars(inventory.All, hostname, vars)

	if inventory != nil && len(inventory.HostVars) > 0 {
		if hostVars, ok := inventory.HostVars[hostname]; ok {
			for key, value := range hostVars {
				vars[key] = value
			}
		}
	}

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
			if host.AnsibleBecomePassword != "" {
				vars["ansible_become_password"] = host.AnsibleBecomePassword
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
