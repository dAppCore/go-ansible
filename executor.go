package ansible

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"io/fs"
	"maps"
	mathrand "math/rand"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	core "dappco.re/go"
	coreio "dappco.re/go/io"
	coreerr "dappco.re/go/log"
	"gopkg.in/yaml.v3"
)

var errEndPlay = core.NewError("end play")
var errEndHost = core.NewError("end host")
var errEndBatch = core.NewError("end batch")
var errEndRole = core.NewError("end role")
var errTaskFailed = core.NewError("task failed")

// sshExecutorClient is the client contract used by the executor.
type sshExecutorClient interface {
	Run(ctx context.Context, cmd string) core.Result
	RunScript(ctx context.Context, script string) core.Result
	Upload(ctx context.Context, local io.Reader, remote string, mode fs.FileMode) core.Result
	Download(ctx context.Context, remote string) core.Result
	FileExists(ctx context.Context, path string) core.Result
	Stat(ctx context.Context, path string) core.Result
	BecomeState() (become bool, user, password string)
	SetBecome(become bool, user, password string)
	Close() core.Result
}

// environmentSSHClient wraps another SSH client and prefixes commands with
// shell exports so play/task environment variables reach remote execution.
type environmentSSHClient struct {
	sshExecutorClient
	prefix string
}

// Run executes cmd over the wrapped SSH client with the configured
// environment prefix prepended.
//
//	result := c.Run(ctx, "uname -a")
func (c *environmentSSHClient) Run(ctx context.Context, cmd string) core.Result {
	return c.sshExecutorClient.Run(ctx, c.prefix+cmd)
}

// RunScript executes script over the wrapped SSH client with the configured
// environment prefix prepended.
//
//	result := c.RunScript(ctx, "#!/bin/bash\necho hi")
func (c *environmentSSHClient) RunScript(ctx context.Context, script string) core.Result {
	return c.sshExecutorClient.RunScript(ctx, c.prefix+script)
}

// Executor runs Ansible playbooks.
//
// Example:
//
//	executor := NewExecutor("/workspace/playbooks")
type Executor struct {
	parser             *Parser
	inventory          *Inventory
	inventoryPath      string
	vars               map[string]any
	hostVars           map[string]map[string]any
	hostFacts          map[string]map[string]any
	facts              map[string]*Facts
	results            map[string]map[string]*TaskResult // host -> register_name -> result
	handlers           map[string][]Task
	loadedRoleHandlers map[string]bool
	notified           map[string]bool
	clients            map[string]sshExecutorClient
	batchFailedHosts   map[string]bool
	endedHosts         map[string]bool
	mu                 sync.RWMutex

	// Callbacks
	OnPlayStart func(play *Play)
	OnTaskStart func(host string, task *Task)
	OnTaskEnd   func(host string, task *Task, result *TaskResult)
	OnPlayEnd   func(play *Play)

	// Options
	Limit     string
	Tags      []string
	SkipTags  []string
	CheckMode bool
	Diff      bool
	Verbose   int
}

// NewExecutor creates a new playbook executor.
//
// Example:
//
//	executor := NewExecutor("/workspace/playbooks")
func NewExecutor(basePath string) *Executor {
	return &Executor{
		parser:             NewParser(basePath),
		vars:               make(map[string]any),
		hostVars:           make(map[string]map[string]any),
		hostFacts:          make(map[string]map[string]any),
		facts:              make(map[string]*Facts),
		results:            make(map[string]map[string]*TaskResult),
		handlers:           make(map[string][]Task),
		loadedRoleHandlers: make(map[string]bool),
		notified:           make(map[string]bool),
		clients:            make(map[string]sshExecutorClient),
		endedHosts:         make(map[string]bool),
	}
}

// SetInventory loads inventory from a file.
//
// Example:
//
//	err := executor.SetInventory("/workspace/inventory.yml")
func (e *Executor) SetInventory(
	path string,
) error {
	invResult := e.parser.ParseInventory(path)
	if !invResult.OK {
		return coreerr.E("Executor.SetInventory", "parse inventory: "+resultErrorMessage(invResult), nil)
	}
	inv := invResult.Value.(*Inventory)
	e.mu.Lock()
	e.inventoryPath = path
	e.inventory = inv
	e.mu.Unlock()
	return nil
}

// SetInventoryDirect sets inventory directly.
//
// Example:
//
//	executor.SetInventoryDirect(&Inventory{All: &InventoryGroup{}})
func (e *Executor) SetInventoryDirect(inv *Inventory) {
	e.mu.Lock()
	e.inventoryPath = ""
	e.inventory = inv
	e.mu.Unlock()
}

// SetVar sets a variable.
//
// Example:
//
//	executor.SetVar("env", "prod")
func (e *Executor) SetVar(key string, value any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.vars[key] = value
}

// SetMedium configures the storage medium used by the embedded parser.
//
// Example:
//
//	executor.SetMedium(coreio.Local)
func (e *Executor) SetMedium(medium coreio.Medium) {
	if e == nil || e.parser == nil {
		return
	}
	e.parser.SetMedium(medium)
}

func (e *Executor) setHostVars(host string, values map[string]any) {
	if host == "" || len(values) == 0 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.hostVars == nil {
		e.hostVars = make(map[string]map[string]any)
	}
	if e.hostVars[host] == nil {
		e.hostVars[host] = make(map[string]any)
	}

	for key, value := range values {
		e.hostVars[host][key] = value
	}
}

func (e *Executor) setHostFacts(host string, values map[string]any) {
	if host == "" || len(values) == 0 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.hostFacts == nil {
		e.hostFacts = make(map[string]map[string]any)
	}
	if e.hostFacts[host] == nil {
		e.hostFacts[host] = make(map[string]any)
	}

	for key, value := range values {
		e.hostFacts[host][key] = value
	}
}

func (e *Executor) resolveDelegateHost(host string, task *Task) string {
	if task == nil || task.Delegate == "" {
		return host
	}

	resolved := e.templateString(task.Delegate, host, task)
	if resolved == "" {
		return host
	}

	return resolved
}

func (e *Executor) hostScopedVars(host string) map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.hostVars) == 0 {
		return nil
	}

	values := e.hostVars[host]
	if len(values) == 0 {
		return nil
	}

	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func (e *Executor) hostFactsMap(host string) map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var merged map[string]any
	if facts := e.facts[host]; facts != nil {
		merged = factsToMap(facts)
	}

	if values := e.hostFacts[host]; len(values) > 0 {
		if merged == nil {
			merged = make(map[string]any, len(values))
		}
		for key, value := range values {
			merged[key] = value
		}
	}

	if len(merged) == 0 {
		return nil
	}
	return merged
}

func inventoryHostnameShort(host string) string {
	host = corexTrimSpace(host)
	if host == "" {
		return ""
	}

	short, _, ok := cut(host, ".")
	if ok && short != "" {
		return short
	}
	return host
}

func (e *Executor) hostMagicVars(host string) map[string]any {
	values := map[string]any{
		"inventory_hostname":       host,
		"inventory_hostname_short": inventoryHostnameShort(host),
	}

	if e != nil && e.inventory != nil {
		if e.inventoryPath != "" {
			values["inventory_file"] = e.inventoryPath
			values["inventory_dir"] = pathDir(e.inventoryPath)
		}
		if groupNames := hostGroupNames(e.inventory.All, host); len(groupNames) > 0 {
			values["group_names"] = groupNames
		}
		if groups := inventoryGroupHosts(e.inventory); len(groups) > 0 {
			values["groups"] = groups
		}
		if hostvars := inventoryHostVars(e.inventory); len(hostvars) > 0 {
			values["hostvars"] = hostvars
		}
	}
	if e != nil {
		values["ansible_check_mode"] = e.CheckMode
		values["ansible_diff_mode"] = e.Diff
		if facts := e.hostFactsMap(host); len(facts) > 0 {
			values["ansible_facts"] = facts
		}
	}

	return values
}

func hostGroupNames(group *InventoryGroup, host string) []string {
	if group == nil || host == "" {
		return nil
	}

	names := make(map[string]bool)
	collectHostGroupNames(group, host, "", names)
	if len(names) == 0 {
		return nil
	}

	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}

func inventoryGroupHosts(inv *Inventory) map[string]any {
	if inv == nil || inv.All == nil {
		return nil
	}

	groups := make(map[string]any)
	collectInventoryGroupHosts(inv.All, "all", groups)
	return groups
}

func collectInventoryGroupHosts(group *InventoryGroup, name string, groups map[string]any) []string {
	if group == nil {
		return nil
	}

	hosts := getAllHosts(group)
	if name != "" {
		values := make([]any, len(hosts))
		for i, host := range hosts {
			values[i] = host
		}
		groups[name] = values
	}

	childNames := slices.Sorted(maps.Keys(group.Children))
	for _, childName := range childNames {
		collectInventoryGroupHosts(group.Children[childName], childName, groups)
	}

	return hosts
}

func inventoryHostVars(inv *Inventory) map[string]any {
	if inv == nil || inv.All == nil {
		return nil
	}

	hosts := getAllHosts(inv.All)
	if len(hosts) == 0 {
		return nil
	}

	values := make(map[string]any, len(hosts))
	for _, host := range hosts {
		hostVars := GetHostVars(inv, host)
		if len(hostVars) == 0 {
			values[host] = map[string]any{}
			continue
		}
		values[host] = hostVars
	}

	return values
}

func collectHostGroupNames(group *InventoryGroup, host, name string, names map[string]bool) bool {
	if group == nil {
		return false
	}

	found := false
	if _, ok := group.Hosts[host]; ok {
		found = true
	}

	childNames := slices.Sorted(maps.Keys(group.Children))
	for _, childName := range childNames {
		if collectHostGroupNames(group.Children[childName], host, childName, names) {
			found = true
		}
	}

	if found && name != "" {
		names[name] = true
	}

	return found
}

// Run executes a playbook.
//
// Example:
//
//	err := executor.Run(context.Background(), "/workspace/playbooks/site.yml")
func (e *Executor) Run(
	ctx context.Context, playbookPath string,
) error {
	playsResult := e.parser.ParsePlaybook(playbookPath)
	if !playsResult.OK {
		return coreerr.E("Executor.Run", "parse playbook: "+resultErrorMessage(playsResult), nil)
	}
	plays := playsResult.Value.([]Play)

	for i := range plays {
		if err := e.runPlay(ctx, &plays[i]); err != nil {
			return coreerr.E("Executor.Run", sprintf("play %d (%s)", i, plays[i].Name), err)
		}
	}

	return nil
}

// runPlay executes a single play.
func (e *Executor) runPlay(
	ctx context.Context, play *Play,
) error {
	if e.OnPlayStart != nil {
		e.OnPlayStart(play)
	}
	defer func() {
		if e.OnPlayEnd != nil {
			e.OnPlayEnd(play)
		}
	}()

	savedVars := make(map[string]any, len(e.vars))
	for k, v := range e.vars {
		savedVars[k] = v
	}
	defer func() {
		e.vars = savedVars
	}()

	// Get target hosts
	hosts := e.getHosts(play.Hosts)
	if len(hosts) == 0 {
		return nil // No hosts matched
	}
	e.endedHosts = make(map[string]bool)
	e.vars["ansible_play_name"] = play.Name
	e.vars["ansible_play_hosts_all"] = append([]string(nil), hosts...)
	e.vars["ansible_play_hosts"] = append([]string(nil), hosts...)
	e.vars["ansible_limit"] = e.Limit

	// Merge play vars
	if err := e.loadPlayVarsFiles(play); err != nil {
		return err
	}
	for k, v := range play.Vars {
		e.vars[k] = v
	}
	e.loadedRoleHandlers = make(map[string]bool)

	for _, batch := range splitSerialHosts(hosts, play.Serial) {
		if len(batch) == 0 {
			continue
		}
		batch = e.filterActiveHosts(batch)
		if len(batch) == 0 {
			continue
		}
		e.vars["ansible_play_hosts"] = append([]string(nil), e.filterActiveHosts(hosts)...)
		e.vars["ansible_play_batch"] = append([]string(nil), batch...)
		e.batchFailedHosts = make(map[string]bool)
		runSection := func(fn func() error) error {
			if err := fn(); err != nil {
				if core.Is(err, errEndPlay) || core.Is(err, errEndBatch) {
					return err
				}
				if play.ForceHandlers {
					if handlerErr := e.runNotifiedHandlers(ctx, batch, play); handlerErr != nil {
						return handlerErr
					}
				}
				return err
			}
			return nil
		}

		// Gather facts if needed
		gatherFacts := play.GatherFacts == nil || *play.GatherFacts
		if gatherFacts {
			for _, host := range batch {
				if err := e.gatherFacts(ctx, host, play); err != nil {
					// Non-fatal
					if e.Verbose > 0 {
						coreerr.Warn("gather facts failed", "host", host, "err", err)
					}
				}
			}
		}

		// Execute pre_tasks
		for _, task := range play.PreTasks {
			if err := runSection(func() error {
				return e.runTaskOnHosts(ctx, batch, &task, play)
			}); err != nil {
				if core.Is(err, errEndPlay) {
					return nil
				}
				if core.Is(err, errEndBatch) {
					goto nextBatch
				}
				return err
			}
		}

		// Execute roles
		for _, roleRef := range play.Roles {
			if err := runSection(func() error {
				return e.runRole(ctx, batch, &roleRef, play, nil)
			}); err != nil {
				if core.Is(err, errEndPlay) {
					return nil
				}
				if core.Is(err, errEndBatch) {
					goto nextBatch
				}
				return err
			}
		}

		// Execute tasks
		for _, task := range play.Tasks {
			if err := runSection(func() error {
				return e.runTaskOnHosts(ctx, batch, &task, play)
			}); err != nil {
				if core.Is(err, errEndPlay) {
					return nil
				}
				if core.Is(err, errEndBatch) {
					goto nextBatch
				}
				return err
			}
		}

		// Execute post_tasks
		for _, task := range play.PostTasks {
			if err := runSection(func() error {
				return e.runTaskOnHosts(ctx, batch, &task, play)
			}); err != nil {
				if core.Is(err, errEndPlay) {
					return nil
				}
				if core.Is(err, errEndBatch) {
					goto nextBatch
				}
				return err
			}
		}

		// Run notified handlers for this batch.
		if err := e.runNotifiedHandlers(ctx, batch, play); err != nil {
			if core.Is(err, errEndPlay) {
				return nil
			}
			if core.Is(err, errEndBatch) {
				goto nextBatch
			}
			return err
		}

	nextBatch:
	}

	return nil
}

// loadPlayVarsFiles loads any play-level vars_files entries and merges them
// into the play's Vars map before execution begins.
func (e *Executor) loadPlayVarsFiles(
	play *Play,
) error {
	if play == nil {
		return nil
	}

	files := normalizeStringList(play.VarsFiles)
	if len(files) == 0 {
		return nil
	}

	// Vars file paths may reference play or executor variables, so render them
	// against a temporary merged scope before reading from disk.
	savedVars := e.vars
	renderVars := make(map[string]any, len(savedVars)+len(play.Vars))
	for k, v := range savedVars {
		renderVars[k] = v
	}
	for k, v := range play.Vars {
		renderVars[k] = v
	}
	e.vars = renderVars
	defer func() {
		e.vars = savedVars
	}()

	merged := make(map[string]any)
	for _, file := range files {
		resolved := e.resolveLocalPath(e.templateString(file, "", nil))
		dataResult := e.parser.readFile(resolved)
		if !dataResult.OK {
			return coreerr.E("Executor.loadPlayVarsFiles", "read vars file: "+resultErrorMessage(dataResult), nil)
		}
		data := dataResult.Value.(string)

		var vars map[string]any
		if err := yaml.Unmarshal([]byte(data), &vars); err != nil {
			return coreerr.E("Executor.loadPlayVarsFiles", "parse vars file", err)
		}

		mergeVars(merged, vars, false)
	}

	if len(merged) == 0 {
		return nil
	}

	if play.Vars == nil {
		play.Vars = make(map[string]any)
	}

	mergeVars(merged, play.Vars, false)
	play.Vars = merged

	return nil
}

// splitSerialHosts splits a host list into serial batches.
func splitSerialHosts(hosts []string, serial any) [][]string {
	if len(hosts) == 0 {
		return nil
	}

	sizes := resolveSerialBatchSizes(serial, len(hosts))
	if len(sizes) == 0 {
		return [][]string{hosts}
	}

	batches := make([][]string, 0, len(sizes))
	remaining := append([]string(nil), hosts...)
	lastSize := sizes[len(sizes)-1]
	for i := 0; len(remaining) > 0; i++ {
		size := lastSize
		if i < len(sizes) {
			size = sizes[i]
		}
		if size <= 0 {
			size = len(remaining)
		}
		if size > len(remaining) {
			size = len(remaining)
		}
		batches = append(batches, append([]string(nil), remaining[:size]...))
		remaining = remaining[size:]
	}
	return batches
}

// resolveSerialBatchSize converts a play serial value into a concrete batch size.
func resolveSerialBatchSize(serial any, total int) int {
	sizes := resolveSerialBatchSizes(serial, total)
	if len(sizes) == 0 {
		return total
	}
	return sizes[0]
}

func resolveSerialBatchSizes(serial any, total int) []int {
	if total <= 0 {
		return nil
	}

	switch v := serial.(type) {
	case nil:
		return []int{total}
	case int:
		if v > 0 {
			return []int{v}
		}
	case int8:
		if v > 0 {
			return []int{int(v)}
		}
	case int16:
		if v > 0 {
			return []int{int(v)}
		}
	case int32:
		if v > 0 {
			return []int{int(v)}
		}
	case int64:
		if v > 0 {
			return []int{int(v)}
		}
	case uint:
		if v > 0 {
			return []int{int(v)}
		}
	case uint8:
		if v > 0 {
			return []int{int(v)}
		}
	case uint16:
		if v > 0 {
			return []int{int(v)}
		}
	case uint32:
		if v > 0 {
			return []int{int(v)}
		}
	case uint64:
		if v > 0 {
			return []int{int(v)}
		}
	case string:
		s := corexTrimSpace(v)
		if s == "" {
			return []int{total}
		}
		if corexHasSuffix(s, "%") {
			percent, err := strconv.Atoi(trimSuffix(s, "%"))
			if err == nil && percent > 0 {
				size := (total*percent + 99) / 100
				if size < 1 {
					size = 1
				}
				if size > total {
					size = total
				}
				return []int{size}
			}
		}
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return []int{n}
		}
	case []int:
		sizes := make([]int, 0, len(v))
		for _, item := range v {
			if size := resolveSerialBatchSize(item, total); size > 0 {
				sizes = append(sizes, size)
			}
		}
		if len(sizes) > 0 {
			return sizes
		}
	case []string:
		sizes := make([]int, 0, len(v))
		for _, item := range v {
			if size := resolveSerialBatchSize(item, total); size > 0 {
				sizes = append(sizes, size)
			}
		}
		if len(sizes) > 0 {
			return sizes
		}
	case []any:
		sizes := make([]int, 0, len(v))
		for _, item := range v {
			if size := resolveSerialBatchSize(item, total); size > 0 {
				sizes = append(sizes, size)
			}
		}
		if len(sizes) > 0 {
			return sizes
		}
	}

	return []int{total}
}

// runRole executes a role on hosts.
func (e *Executor) runRole(
	ctx context.Context, hosts []string, roleRef *RoleRef, play *Play, inheritedWhen any,
) error {
	oldVars := make(map[string]any, len(e.vars))
	for k, v := range e.vars {
		oldVars[k] = v
	}

	roleDataResult := e.parser.loadRoleData(roleRef.Role, roleRef.TasksFrom, roleRef.DefaultsFrom, roleRef.VarsFrom)
	if !roleDataResult.OK {
		e.vars = oldVars
		return coreerr.E("executor.runRole", sprintf("parse role %s: %s", roleRef.Role, resultErrorMessage(roleDataResult)), nil)
	}
	roleData := roleDataResult.Value.(parserRoleDataResult)
	tasks := roleData.Tasks
	defaults := roleData.Defaults
	roleVars := roleData.Vars
	tasksPath := roleData.Path
	if err := e.attachRoleHandlers(roleRef.Role, roleRef.HandlersFrom, play); err != nil {
		e.vars = oldVars
		return coreerr.E("executor.runRole", sprintf("load handlers for role %s", roleRef.Role), err)
	}

	roleScope := make(map[string]any, len(oldVars)+len(defaults)+len(roleVars)+len(roleRef.Vars))
	for k, v := range oldVars {
		roleScope[k] = v
	}
	for k, v := range defaults {
		if _, exists := roleScope[k]; !exists {
			roleScope[k] = v
		}
	}
	for k, v := range roleVars {
		roleScope[k] = v
	}
	for k, v := range roleRef.Vars {
		roleScope[k] = v
	}
	if roleRef.Role != "" {
		roleScope["role_name"] = roleRef.Role
	}
	if tasksPath != "" {
		roleScope["role_path"] = pathDir(pathDir(tasksPath))
	}
	e.vars = roleScope

	eligibleHosts := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if roleRef.When == nil || e.evaluateWhen(roleRef.When, host, nil) {
			eligibleHosts = append(eligibleHosts, host)
		}
	}
	if len(eligibleHosts) == 0 {
		e.vars = oldVars
		return nil
	}

	// Execute tasks
	for _, task := range tasks {
		effectiveTask := task
		e.applyRoleTaskDefaults(&effectiveTask, roleRef.Apply)
		if len(roleRef.Tags) > 0 {
			effectiveTask.Tags = mergeStringSlices(roleRef.Tags, effectiveTask.Tags)
		}
		if inheritedWhen != nil {
			effectiveTask.When = mergeConditions(inheritedWhen, effectiveTask.When)
		}
		if err := e.runTaskOnHosts(ctx, eligibleHosts, &effectiveTask, play); err != nil {
			if core.Is(err, errEndRole) {
				e.vars = oldVars
				return nil
			}
			// Restore vars
			e.vars = oldVars
			return err
		}
	}

	// Restore vars
	if roleRef == nil || !roleRef.Public {
		e.vars = oldVars
	}
	return nil
}

func (e *Executor) attachRoleHandlers(
	roleName, handlersFrom string, play *Play,
) error {
	if play == nil || roleName == "" {
		return nil
	}
	if e.loadedRoleHandlers == nil {
		e.loadedRoleHandlers = make(map[string]bool)
	}

	if handlersFrom == "" {
		handlersFrom = "main.yml"
	}

	key := roleName + "|handlers/" + handlersFrom
	if e.loadedRoleHandlers[key] {
		return nil
	}

	handlersResult := e.parser.loadRoleHandlers(roleName, handlersFrom)
	if !handlersResult.OK {
		return handlersResult.Value.(error)
	}
	handlers := handlersResult.Value.([]Task)
	if len(handlers) > 0 {
		play.Handlers = append(play.Handlers, handlers...)
	}
	e.loadedRoleHandlers[key] = true
	return nil
}

// runTaskOnHosts runs a task on all hosts.
func (e *Executor) runTaskOnHosts(
	ctx context.Context, hosts []string, task *Task, play *Play,
) error {
	// Check tags
	if !e.matchesTags(effectiveTaskTags(task, play)) {
		return nil
	}

	// run_once executes the task against a single host, then shares any
	// registered result with the rest of the host set.
	if task.RunOnce && len(hosts) > 0 {
		single := *task
		single.RunOnce = false

		if err := e.runTaskOnHosts(ctx, hosts[:1], &single, play); err != nil {
			return err
		}

		e.copyRegisteredResultToHosts(hosts, hosts[0], task.Register)
		return nil
	}

	// Handle block tasks
	if len(task.Block) > 0 {
		return e.runBlock(ctx, hosts, task, play)
	}

	// Handle include/import
	if task.IncludeTasks != "" || task.ImportTasks != "" {
		return e.runIncludeTasks(ctx, hosts, task, play)
	}
	if task.IncludeRole != nil || task.ImportRole != nil {
		return e.runIncludeRole(ctx, hosts, task, play)
	}

	for _, host := range hosts {
		if e.isHostEnded(host) {
			continue
		}
		if err := e.runTaskOnHost(ctx, host, hosts, task, play); err != nil {
			if core.Is(err, errEndHost) {
				continue
			}
			if core.Is(err, errTaskFailed) {
				if play != nil && play.AnyErrorsFatal {
					return err
				}
				// Multi-host batches continue after a task failure unless the
				// play has explicitly requested fail-fast semantics.
				if len(hosts) == 1 {
					return err
				}
				continue
			}
			if !task.IgnoreErrors {
				return err
			}
		}

		if err := e.checkMaxFailPercentage(play, hosts); err != nil {
			return err
		}
	}

	return nil
}

func effectiveTaskTags(task *Task, play *Play) []string {
	var tags []string
	if play != nil && len(play.Tags) > 0 {
		tags = append(tags, play.Tags...)
	}
	if task != nil && len(task.Tags) > 0 {
		tags = append(tags, task.Tags...)
	}
	if task != nil {
		switch {
		case task.IncludeRole != nil && len(task.IncludeRole.Tags) > 0:
			tags = append(tags, task.IncludeRole.Tags...)
		case task.ImportRole != nil && len(task.ImportRole.Tags) > 0:
			tags = append(tags, task.ImportRole.Tags...)
		}
	}
	return tags
}

func mergeStringSlices(parts ...[]string) []string {
	total := 0
	for _, part := range parts {
		total += len(part)
	}
	if total == 0 {
		return nil
	}

	merged := make([]string, 0, total)
	for _, part := range parts {
		merged = append(merged, part...)
	}
	return merged
}

// copyRegisteredResultToHosts shares a registered task result from one host to
// the rest of the current host set.
func (e *Executor) copyRegisteredResultToHosts(hosts []string, sourceHost, register string) {
	if register == "" {
		return
	}

	sourceResults := e.results[sourceHost]
	if sourceResults == nil {
		return
	}

	result, ok := sourceResults[register]
	if !ok || result == nil {
		return
	}

	for _, host := range hosts {
		if host == sourceHost {
			continue
		}
		if e.results[host] == nil {
			e.results[host] = make(map[string]*TaskResult)
		}

		clone := *result
		if result.Results != nil {
			clone.Results = append([]TaskResult(nil), result.Results...)
		}
		if result.Data != nil {
			clone.Data = make(map[string]any, len(result.Data))
			for k, v := range result.Data {
				clone.Data[k] = v
			}
		}
		e.results[host][register] = &clone
	}
}

// runTaskOnHost runs a task on a single host.
func (e *Executor) runTaskOnHost(
	ctx context.Context, host string, hosts []string, task *Task, play *Play,
) error {
	if e.isHostEnded(host) {
		return nil
	}

	start := time.Now()
	e.mu.Lock()
	oldInventoryHostname, hadInventoryHostname := e.vars["inventory_hostname"]
	e.vars["inventory_hostname"] = host
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if hadInventoryHostname {
			e.vars["inventory_hostname"] = oldInventoryHostname
		} else {
			delete(e.vars, "inventory_hostname")
		}
		e.mu.Unlock()
	}()

	savedCheckMode := e.CheckMode
	savedDiff := e.Diff
	if task != nil {
		if task.CheckMode != nil {
			e.CheckMode = *task.CheckMode
		}
		if task.Diff != nil {
			e.Diff = *task.Diff
		}
	}
	defer func() {
		e.CheckMode = savedCheckMode
		e.Diff = savedDiff
	}()

	if e.OnTaskStart != nil {
		e.OnTaskStart(host, task)
	}

	// Initialise host results.
	if e.results[host] == nil {
		e.results[host] = make(map[string]*TaskResult)
	}

	execCtx := ctx
	if task.Async > 0 && task.Poll > 0 {
		timeout := time.Duration(task.Async) * time.Second
		if timeout > 0 {
			var cancel context.CancelFunc
			execCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}

	if task.Async > 0 && task.Poll <= 0 {
		result := &TaskResult{
			Changed: false,
			Msg:     "async task started",
			Data: map[string]any{
				"ansible_job_id": newAsyncJobID(),
				"started":        1,
				"finished":       0,
			},
		}
		if task.Register != "" {
			e.results[host][task.Register] = result
		}
		displayResult := result
		if task.NoLog {
			displayResult = redactTaskResult(result)
		}
		if e.OnTaskEnd != nil {
			e.OnTaskEnd(host, task, displayResult)
		}
		e.launchDetachedAsyncTask(ctx, host, hosts, task, play)
		return nil
	}

	hasLoop := task.Loop != nil || task.WithFile != nil || task.WithFileGlob != nil || task.WithSequence != nil || task.WithTogether != nil || task.WithSubelements != nil

	// Check when condition before allocating a client for simple tasks. Loop
	// tasks evaluate when per item.
	if task.When != nil && !hasLoop {
		if !e.evaluateWhen(task.When, host, task) {
			result := &TaskResult{Skipped: true, Msg: "Skipped due to when condition"}
			if task.Register != "" {
				e.results[host][task.Register] = result
			}
			if e.OnTaskEnd != nil {
				e.OnTaskEnd(host, task, result)
			}
			return nil
		}
	}

	// Honour check mode for tasks that would mutate state.
	if e.CheckMode && !isCheckModeSafeTask(task) {
		result := &TaskResult{Skipped: true, Msg: "Skipped in check mode"}
		if task.Register != "" {
			e.results[host][task.Register] = result
		}
		if e.OnTaskEnd != nil {
			e.OnTaskEnd(host, task, result)
		}
		return nil
	}

	// Get SSH client
	executionHost := e.resolveDelegateHost(host, task)

	clientResult := e.getClient(executionHost, play)
	if !clientResult.OK {
		return coreerr.E("Executor.runTaskOnHost", sprintf("get client for %s: %s", executionHost, resultErrorMessage(clientResult)), nil)
	}
	client := clientResult.Value.(sshExecutorClient)

	// Handle loops, including legacy with_file, with_fileglob, with_sequence,
	// with_together, and with_subelements syntax.
	if hasLoop {
		return e.runLoop(execCtx, host, client, task, play, start)
	}

	// Check when conditions for non-loop tasks after loop-only variables have
	// been ruled out. Loop tasks evaluate their when-clause per item.
	if task.When != nil {
		if !e.evaluateWhen(task.When, host, task) {
			result := &TaskResult{Skipped: true, Msg: "Skipped due to when condition"}
			if task.Register != "" {
				e.results[host][task.Register] = result
			}
			if e.OnTaskEnd != nil {
				e.OnTaskEnd(host, task, result)
			}
			return nil
		}
	}

	// Execute the task, honouring retries/until when configured.
	retryResult := e.runTaskWithRetries(execCtx, host, task, play, func() core.Result {
		return e.executeModule(execCtx, host, client, task, play)
	})
	result := &TaskResult{}
	if !retryResult.OK {
		result = &TaskResult{Failed: true, Msg: resultErrorMessage(retryResult)}
	} else if retryTaskResult, ok := retryResult.Value.(*TaskResult); ok && retryTaskResult != nil {
		result = retryTaskResult
	}
	result.Duration = time.Since(start)

	// Store result
	if task.Register != "" {
		e.results[host][task.Register] = result
	}

	displayResult := result
	if task.NoLog {
		displayResult = redactTaskResult(result)
	}

	// Handle notify
	if result.Changed && task.Notify != nil {
		e.handleNotify(task.Notify)
	}

	if e.OnTaskEnd != nil {
		e.OnTaskEnd(host, task, displayResult)
	}

	if NormalizeModule(task.Module) == "ansible.builtin.meta" {
		if err := e.handleMetaAction(ctx, host, hosts, play, result); err != nil {
			return err
		}
	}

	if result.Failed && !task.IgnoreErrors {
		e.markBatchHostFailed(host)
		return taskFailureError(task, result)
	}
	if result.Failed {
		e.markBatchHostFailed(host)
	}

	return nil
}

func (e *Executor) markBatchHostFailed(host string) {
	if host == "" {
		return
	}
	if e.batchFailedHosts == nil {
		e.batchFailedHosts = make(map[string]bool)
	}
	e.batchFailedHosts[host] = true
}

func (e *Executor) checkMaxFailPercentage(
	play *Play, hosts []string,
) error {
	if play == nil || play.MaxFailPercent <= 0 || len(hosts) == 0 {
		return nil
	}

	threshold := play.MaxFailPercent
	failed := 0
	for _, host := range hosts {
		if e.batchFailedHosts != nil && e.batchFailedHosts[host] {
			failed++
		}
	}

	if failed == 0 {
		return nil
	}

	percentage := (failed * 100) / len(hosts)
	if percentage > threshold {
		return coreerr.E("Executor.runTaskOnHosts", sprintf("max fail percentage exceeded: %d%% failed (threshold %d%%)", percentage, threshold), nil)
	}

	return nil
}

func redactTaskResult(result *TaskResult) *TaskResult {
	if result == nil {
		return nil
	}

	redacted := *result
	redacted.Msg = "censored due to no_log"
	redacted.Stdout = ""
	redacted.Stderr = ""
	redacted.Data = nil
	if len(result.Results) > 0 {
		redacted.Results = make([]TaskResult, len(result.Results))
		for i := range result.Results {
			redacted.Results[i] = *redactTaskResult(&result.Results[i])
		}
	}

	return &redacted
}

func taskFailureError(
	task *Task, result *TaskResult,
) error {
	if task != nil && task.NoLog {
		return coreerr.E("Executor.runTaskOnHost", "task failed", errTaskFailed)
	}

	msg := "task failed"
	if result != nil && result.Msg != "" {
		msg += ": " + result.Msg
	}

	return coreerr.E("Executor.runTaskOnHost", msg, errTaskFailed)
}

// runTaskWithRetries executes a task once or multiple times when retries,
// delay, or until are configured.
func (e *Executor) runTaskWithRetries(ctx context.Context, host string, task *Task, play *Play, execute func() core.Result) core.Result {
	attempts := 1
	if task != nil {
		if task.Until != "" {
			if task.Retries > 0 {
				attempts = task.Retries + 1
			} else {
				attempts = 4
			}
		} else if task.Retries > 0 {
			attempts = task.Retries + 1
		}
	}

	var result *TaskResult
	for attempt := 1; attempt <= attempts; attempt++ {
		executeResult := execute()
		if !executeResult.OK {
			result = &TaskResult{Failed: true, Msg: resultErrorMessage(executeResult)}
		} else if taskResult, ok := executeResult.Value.(*TaskResult); ok {
			result = taskResult
		}
		if result == nil {
			result = &TaskResult{}
		}

		e.applyTaskResultConditions(host, task, result)

		if !shouldRetryTask(task, host, e, result) || attempt == attempts {
			break
		}

		if task != nil && task.Delay > 0 {
			timer := time.NewTimer(time.Duration(task.Delay) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return core.Fail(ctx.Err())
			case <-timer.C:
			}
		}
	}

	return core.Ok(result)
}

func shouldRetryTask(task *Task, host string, e *Executor, result *TaskResult) bool {
	if task == nil || result == nil {
		return false
	}

	if task.Until != "" {
		return !e.evaluateWhenWithLocals(task.Until, host, task, map[string]any{
			"result":   result,
			"stdout":   result.Stdout,
			"stderr":   result.Stderr,
			"rc":       result.RC,
			"changed":  result.Changed,
			"failed":   result.Failed,
			"skipped":  result.Skipped,
			"msg":      result.Msg,
			"duration": result.Duration,
		})
	}

	if task.Retries > 0 {
		return result.Failed
	}

	return false
}

// runLoop handles task loops.
func (e *Executor) runLoop(
	ctx context.Context, host string, client sshExecutorClient, task *Task, play *Play, start time.Time,
) error {
	items := []any(nil)
	itemsResult := core.Ok(nil)
	if task.WithFile != nil {
		itemsResult = e.resolveWithFileLoop(task.WithFile, host, task)
	} else if task.WithFileGlob != nil {
		itemsResult = e.resolveWithFileGlobLoop(task.WithFileGlob, host, task)
	} else if task.WithSequence != nil {
		itemsResult = e.resolveWithSequenceLoop(task.WithSequence, host, task)
	} else if task.WithTogether != nil {
		itemsResult = e.resolveWithTogetherLoop(task.WithTogether, host, task)
	} else if task.WithSubelements != nil {
		itemsResult = e.resolveWithSubelementsLoop(task.WithSubelements, host, task)
	} else {
		items = e.resolveLoopWithTask(task.Loop, host, task)
	}
	if !itemsResult.OK {
		return coreerr.E("Executor.runLoop", "resolve loop: "+resultErrorMessage(itemsResult), nil)
	}
	if itemsResult.Value != nil {
		items = itemsResult.Value.([]any)
	}

	loopVar := "item"
	if task.LoopControl != nil && task.LoopControl.LoopVar != "" {
		loopVar = task.LoopControl.LoopVar
	}

	// Save loop state to restore after loop
	savedVars := make(map[string]any)
	if v, ok := e.vars[loopVar]; ok {
		savedVars[loopVar] = v
	}
	indexVar := ""
	if task.LoopControl != nil && task.LoopControl.IndexVar != "" {
		indexVar = task.LoopControl.IndexVar
		if v, ok := e.vars[indexVar]; ok {
			savedVars[indexVar] = v
		}
	}
	var savedLoopMeta any
	if task.LoopControl != nil && (task.LoopControl.Extended || task.LoopControl.Label != "") {
		if v, ok := e.vars["ansible_loop"]; ok {
			savedLoopMeta = v
		}
	}

	var results []TaskResult
	for i, item := range items {
		// Set loop variables
		e.vars[loopVar] = item
		if indexVar != "" {
			e.vars[indexVar] = i
		}
		if task.LoopControl != nil && (task.LoopControl.Extended || task.LoopControl.Label != "") {
			loopMeta := map[string]any{}
			if task.LoopControl.Extended {
				var prevItem any
				if i > 0 {
					prevItem = items[i-1]
				}
				var nextItem any
				if i+1 < len(items) {
					nextItem = items[i+1]
				}
				loopMeta["index"] = i + 1
				loopMeta["index0"] = i
				loopMeta["first"] = i == 0
				loopMeta["last"] = i == len(items)-1
				loopMeta["length"] = len(items)
				loopMeta["revindex"] = len(items) - i
				loopMeta["revindex0"] = len(items) - i - 1
				loopMeta["allitems"] = append([]any(nil), items...)
				if prevItem != nil {
					loopMeta["previtem"] = prevItem
				}
				if nextItem != nil {
					loopMeta["nextitem"] = nextItem
				}
			}
			if task.LoopControl.Label != "" {
				loopMeta["label"] = e.templateString(task.LoopControl.Label, host, task)
			}
			e.vars["ansible_loop"] = loopMeta
		}

		if task.When != nil && !e.evaluateWhen(task.When, host, task) {
			skipped := TaskResult{Skipped: true, Msg: "Skipped due to when condition"}
			results = append(results, skipped)
			if task.LoopControl != nil && task.LoopControl.Pause > 0 && i < len(items)-1 {
				timer := time.NewTimer(time.Duration(task.LoopControl.Pause) * time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			continue
		}

		retryResult := e.runTaskWithRetries(ctx, host, task, play, func() core.Result {
			return e.executeModule(ctx, host, client, task, play)
		})
		result := &TaskResult{}
		if !retryResult.OK {
			result = &TaskResult{Failed: true, Msg: resultErrorMessage(retryResult)}
		} else if retryTaskResult, ok := retryResult.Value.(*TaskResult); ok && retryTaskResult != nil {
			result = retryTaskResult
		}
		results = append(results, *result)

		if task.LoopControl != nil && task.LoopControl.Pause > 0 && i < len(items)-1 {
			timer := time.NewTimer(time.Duration(task.LoopControl.Pause) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		if result.Failed && !task.IgnoreErrors {
			break
		}
	}

	// Restore loop variables
	if v, ok := savedVars[loopVar]; ok {
		e.vars[loopVar] = v
	} else {
		delete(e.vars, loopVar)
	}
	if indexVar != "" {
		if v, ok := savedVars[indexVar]; ok {
			e.vars[indexVar] = v
		} else {
			delete(e.vars, indexVar)
		}
	}
	if task.LoopControl != nil && (task.LoopControl.Extended || task.LoopControl.Label != "") {
		if savedLoopMeta != nil {
			e.vars["ansible_loop"] = savedLoopMeta
		} else {
			delete(e.vars, "ansible_loop")
		}
	}

	// Store combined result
	if task.Register != "" {
		combined := &TaskResult{Results: results}
		for _, r := range results {
			if r.Changed {
				combined.Changed = true
			}
			if r.Failed {
				combined.Failed = true
			}
		}
		if len(results) > 0 {
			combined.Skipped = true
			for _, r := range results {
				if !r.Skipped {
					combined.Skipped = false
					break
				}
			}
		}
		combined.Duration = time.Since(start)
		e.results[host][task.Register] = combined
	}

	result := &TaskResult{
		Results: results,
		Changed: false,
	}
	for _, r := range results {
		if r.Changed {
			result.Changed = true
		}
		if r.Failed {
			result.Failed = true
		}
	}
	if len(results) > 0 {
		result.Skipped = true
		for _, r := range results {
			if !r.Skipped {
				result.Skipped = false
				break
			}
		}
	}
	result.Duration = time.Since(start)

	displayResult := result
	if task.NoLog {
		displayResult = redactTaskResult(result)
	}

	if result.Changed && task.Notify != nil {
		e.handleNotify(task.Notify)
	}

	if e.OnTaskEnd != nil {
		e.OnTaskEnd(host, task, displayResult)
	}

	if result.Failed {
		e.markBatchHostFailed(host)
		if !task.IgnoreErrors {
			return taskFailureError(task, result)
		}
	}

	return nil
}

// resolveWithFileLoop resolves legacy with_file loop items into file contents.
func (e *Executor) resolveWithFileLoop(loop any, host string, task *Task) core.Result {
	var paths []string

	switch v := loop.(type) {
	case []any:
		paths = make([]string, 0, len(v))
		for _, item := range v {
			s := sprintf("%v", item)
			if s = e.templateString(s, host, task); s != "" {
				paths = append(paths, s)
			}
		}
	case []string:
		paths = make([]string, 0, len(v))
		for _, item := range v {
			if s := e.templateString(item, host, task); s != "" {
				paths = append(paths, s)
			}
		}
	case string:
		if s := e.templateString(v, host, task); s != "" {
			paths = []string{s}
		}
	default:
		return core.Ok([]any(nil))
	}

	items := make([]any, 0, len(paths))
	for _, filePath := range paths {
		contentResult := e.readLoopFile(filePath)
		if !contentResult.OK {
			return contentResult
		}
		items = append(items, contentResult.Value.(string))
	}

	return core.Ok(items)
}

// resolveWithFileGlobLoop resolves legacy with_fileglob loop items into matching file paths.
func (e *Executor) resolveWithFileGlobLoop(loop any, host string, task *Task) core.Result {
	var patterns []string

	switch v := loop.(type) {
	case []any:
		patterns = make([]string, 0, len(v))
		for _, item := range v {
			s := sprintf("%v", item)
			if s = e.templateString(s, host, task); s != "" {
				patterns = append(patterns, s)
			}
		}
	case []string:
		patterns = make([]string, 0, len(v))
		for _, item := range v {
			if s := e.templateString(item, host, task); s != "" {
				patterns = append(patterns, s)
			}
		}
	case string:
		if s := e.templateString(v, host, task); s != "" {
			patterns = []string{s}
		}
	default:
		return core.Ok([]any(nil))
	}

	items := make([]any, 0)
	for _, pattern := range patterns {
		matchesResult := e.resolveFileGlob(pattern)
		if !matchesResult.OK {
			return matchesResult
		}
		matches := matchesResult.Value.([]any)
		for _, match := range matches {
			items = append(items, match)
		}
	}

	return core.Ok(items)
}

type sequenceSpec struct {
	start  int
	end    int
	count  int
	step   int
	format string
	hasEnd bool
}

func (e *Executor) resolveWithSequenceLoop(loop any, host string, task *Task) core.Result {
	specResult := parseSequenceSpec(loop)
	if !specResult.OK {
		return specResult
	}
	spec := specResult.Value.(*sequenceSpec)

	valuesResult := buildSequenceValues(spec)
	if !valuesResult.OK {
		return valuesResult
	}
	values := valuesResult.Value.([]string)

	items := make([]any, len(values))
	for i, value := range values {
		items[i] = value
	}
	return core.Ok(items)
}

func (e *Executor) resolveWithTogetherLoop(loop any, host string, task *Task) core.Result {
	items := expandTogetherLoop(loop)
	if len(items) == 0 {
		return core.Ok([]any(nil))
	}

	return core.Ok(items)
}

func (e *Executor) resolveWithSubelementsLoop(loop any, host string, task *Task) core.Result {
	source, subelement, skipMissing, ok := parseSubelementsSpec(loop)
	if !ok {
		return core.Ok([]any(nil))
	}
	if subelement == "" {
		return core.Fail(coreerr.E("Executor.resolveWithSubelementsLoop", "with_subelements requires a subelement name", nil))
	}

	parents := e.resolveSubelementsParents(source, host, task)
	items := make([]any, 0)
	for _, parent := range parents {
		subelementValues, found := subelementItems(parent, subelement)
		if !found {
			if skipMissing {
				continue
			}
			return core.Fail(coreerr.E("Executor.resolveWithSubelementsLoop", sprintf("with_subelements missing subelement %q", subelement), nil))
		}
		for _, subitem := range subelementValues {
			items = append(items, []any{parent, subitem})
		}
	}

	return core.Ok(items)
}

func parseSubelementsSpec(loop any) (any, string, bool, bool) {
	switch v := loop.(type) {
	case []any:
		if len(v) < 2 {
			return nil, "", false, false
		}
		return v[0], sprintf("%v", v[1]), parseSubelementsSkipMissing(v[2:]), true
	case []string:
		if len(v) < 2 {
			return nil, "", false, false
		}
		return v[0], v[1], parseSubelementsSkipMissingStrings(v[2:]), true
	case string:
		parts := fields(v)
		if len(parts) < 2 {
			return nil, "", false, false
		}
		return parts[0], parts[1], parseSubelementsSkipMissingStrings(parts[2:]), true
	default:
		return nil, "", false, false
	}
}

func parseSubelementsSkipMissing(values []any) bool {
	for _, value := range values {
		if parseSkipMissingValue(value) {
			return true
		}
	}
	return false
}

func parseSubelementsSkipMissingStrings(values []string) bool {
	for _, value := range values {
		if parseSkipMissingValue(value) {
			return true
		}
	}
	return false
}

func parseSkipMissingValue(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		trimmed := trimSpace(v)
		if trimmed == "" {
			return false
		}
		if trimmed == "skip_missing" {
			return true
		}
		if hasPrefix(trimmed, "skip_missing=") {
			return getBoolArg(map[string]any{"skip_missing": trimPrefix(trimmed, "skip_missing=")}, "skip_missing", false)
		}
		return getBoolArg(map[string]any{"skip_missing": trimmed}, "skip_missing", false)
	case map[string]any:
		return getBoolArg(v, "skip_missing", false)
	case map[any]any:
		converted := make(map[string]any, len(v))
		for key, val := range v {
			if s, ok := key.(string); ok {
				converted[s] = val
			}
		}
		return getBoolArg(converted, "skip_missing", false)
	default:
		return false
	}
}

func (e *Executor) resolveSubelementsParents(value any, host string, task *Task) []any {
	switch v := value.(type) {
	case string:
		if items := e.resolveLoopWithTask(v, host, task); len(items) > 0 {
			return items
		}
		if items, ok := anySliceFromValue(e.templateString(v, host, task)); ok {
			return items
		}
		if task != nil {
			if items, ok := anySliceFromValue(task.Vars[v]); ok {
				return items
			}
		}
	default:
		if items, ok := anySliceFromValue(v); ok {
			return items
		}
	}

	return nil
}

func subelementItems(parent any, path string) ([]any, bool) {
	value, ok := lookupNestedValue(parent, path)
	if !ok || value == nil {
		return nil, false
	}

	if items, ok := anySliceFromValue(value); ok {
		return items, true
	}

	return []any{value}, true
}

func parseSequenceSpec(loop any) core.Result {
	spec := &sequenceSpec{
		step:   1,
		format: "%d",
	}

	switch v := loop.(type) {
	case string:
		if r := parseSequenceSpecString(spec, v); !r.OK {
			return r
		}
	case map[string]any:
		if r := parseSequenceSpecMap(spec, v); !r.OK {
			return r
		}
	case map[any]any:
		converted := make(map[string]any, len(v))
		for key, value := range v {
			if s, ok := key.(string); ok {
				converted[s] = value
			}
		}
		if r := parseSequenceSpecMap(spec, converted); !r.OK {
			return r
		}
	default:
		return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence requires a string or mapping", nil))
	}

	if spec.count > 0 && !spec.hasEnd {
		spec.end = spec.start + (spec.count-1)*spec.step
		spec.hasEnd = true
	}
	if !spec.hasEnd {
		return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence requires end or count", nil))
	}
	if spec.step == 0 {
		return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence stride must not be zero", nil))
	}

	if spec.end < spec.start && spec.step > 0 {
		spec.step = -spec.step
	}
	if spec.end > spec.start && spec.step < 0 {
		spec.step = -spec.step
	}

	return core.Ok(spec)
}

func parseSequenceSpecString(
	spec *sequenceSpec, raw string,
) core.Result {
	fields := fields(replaceAll(raw, ",", " "))
	for _, field := range fields {
		parts := splitN(field, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := trimSpace(parts[0])
		value := trimSpace(parts[1])
		if r := applySequenceSpecValue(spec, key, value); !r.OK {
			return r
		}
	}

	if spec.start == 0 && !contains(raw, "start=") {
		spec.start = 0
	}

	return core.Ok(nil)
}

func parseSequenceSpecMap(
	spec *sequenceSpec, values map[string]any,
) core.Result {
	for key, value := range values {
		if r := applySequenceSpecValue(spec, key, value); !r.OK {
			return r
		}
	}
	return core.Ok(nil)
}

func applySequenceSpecValue(
	spec *sequenceSpec, key string, value any,
) core.Result {
	switch lower(trimSpace(key)) {
	case "start":
		n, ok := sequenceSpecInt(value)
		if !ok {
			return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence start must be numeric", nil))
		}
		spec.start = n
	case "end":
		n, ok := sequenceSpecInt(value)
		if !ok {
			return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence end must be numeric", nil))
		}
		spec.end = n
		spec.hasEnd = true
	case "count":
		n, ok := sequenceSpecInt(value)
		if !ok {
			return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence count must be numeric", nil))
		}
		spec.count = n
	case "stride":
		n, ok := sequenceSpecInt(value)
		if !ok {
			return core.Fail(coreerr.E("Executor.resolveWithSequenceLoop", "with_sequence stride must be numeric", nil))
		}
		spec.step = n
	case "format":
		spec.format = sprintf("%v", value)
	default:
		// Ignore unrecognised keys so the parser stays tolerant of extra
		// Ansible sequence options we do not currently model.
	}
	return core.Ok(nil)
}

func sequenceSpecInt(value any) (int, bool) {
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
	case string:
		n, err := strconv.Atoi(trimSpace(v))
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func buildSequenceValues(spec *sequenceSpec) core.Result {
	if spec.count > 0 && !spec.hasEnd {
		spec.end = spec.start + (spec.count-1)*spec.step
		spec.hasEnd = true
	}

	values := make([]string, 0)
	if spec.step > 0 {
		for value := spec.start; value <= spec.end; value += spec.step {
			values = append(values, formatSequenceValue(spec.format, value))
		}
		return core.Ok(values)
	}

	for value := spec.start; value >= spec.end; value += spec.step {
		values = append(values, formatSequenceValue(spec.format, value))
	}
	return core.Ok(values)
}

func formatSequenceValue(format string, value int) string {
	if format == "" {
		return sprintf("%d", value)
	}

	formatted := sprintf(format, value)
	if formatted == "" {
		return sprintf("%d", value)
	}
	return formatted
}

func (e *Executor) resolveFileGlob(pattern string) core.Result {
	candidates := []string{pattern}
	if e.parser != nil && e.parser.basePath != "" && !pathIsAbs(pattern) {
		candidates = append([]string{joinPath(e.parser.basePath, pattern)}, candidates...)
	}

	var matches []string
	for _, candidate := range candidates {
		globMatches := pathGlob(candidate)
		if len(globMatches) > 0 {
			matches = append(matches, globMatches...)
			break
		}
	}

	slices.Sort(matches)
	result := make([]any, len(matches))
	for i, match := range matches {
		result[i] = match
	}
	return core.Ok(result)
}

func (e *Executor) readLoopFile(filePath string) core.Result {
	candidates := []string{filePath}
	if e.parser != nil && e.parser.basePath != "" {
		candidates = append([]string{joinPath(e.parser.basePath, filePath)}, candidates...)
	}

	for _, candidate := range candidates {
		if data, err := coreio.Local.Read(candidate); err == nil {
			return core.Ok(data)
		}
	}

	return core.Fail(coreerr.E("Executor.readLoopFile", "read file "+filePath, nil))
}

// isCheckModeSafeTask reports whether a task can run without changing state
// during check mode.
func isCheckModeSafeTask(task *Task) bool {
	if task == nil {
		return true
	}

	if len(task.Block) > 0 || len(task.Rescue) > 0 || len(task.Always) > 0 {
		return true
	}
	if task.IncludeTasks != "" || task.ImportTasks != "" {
		return true
	}
	if task.IncludeRole != nil || task.ImportRole != nil {
		return true
	}

	switch NormalizeModule(task.Module) {
	case "ansible.builtin.debug",
		"ansible.builtin.fail",
		"ansible.builtin.assert",
		"ansible.builtin.ping",
		"ansible.builtin.pause",
		"ansible.builtin.wait_for",
		"ansible.builtin.stat",
		"ansible.builtin.slurp",
		"ansible.builtin.include_vars",
		"ansible.builtin.meta",
		"ansible.builtin.set_fact",
		"ansible.builtin.add_host",
		"ansible.builtin.group_by",
		"ansible.builtin.setup":
		return true
	default:
		return false
	}
}

// runBlock handles block/rescue/always.
func (e *Executor) runBlock(
	ctx context.Context, hosts []string, task *Task, play *Play,
) error {
	var blockErr error
	var rescueErr error

	inherit := func(child *Task) {
		if child == nil || task == nil {
			return
		}

		child.Vars = mergeTaskVars(task.Vars, child.Vars)
		child.Environment = mergeStringMap(task.Environment, child.Environment)
		if task.When != nil {
			child.When = mergeConditions(task.When, child.When)
		}
		if len(task.Tags) > 0 {
			child.Tags = mergeStringSlices(task.Tags, child.Tags)
		}
		if task.Become != nil && child.Become == nil {
			child.Become = task.Become
		}
		if task.BecomeUser != "" && child.BecomeUser == "" {
			child.BecomeUser = task.BecomeUser
		}
		if task.Delegate != "" && child.Delegate == "" {
			child.Delegate = task.Delegate
		}
		if task.DelegateFacts {
			child.DelegateFacts = true
		}
		if task.RunOnce {
			child.RunOnce = true
		}
		if task.NoLog {
			child.NoLog = true
		}
		if task.IgnoreErrors {
			child.IgnoreErrors = true
		}
	}

	// Try block
	for _, t := range task.Block {
		effective := t
		inherit(&effective)
		if err := e.runTaskOnHosts(ctx, hosts, &effective, play); err != nil {
			blockErr = err
			break
		}
	}

	// Run rescue if block failed
	if blockErr != nil && len(task.Rescue) > 0 {
		for _, t := range task.Rescue {
			effective := t
			inherit(&effective)
			if err := e.runTaskOnHosts(ctx, hosts, &effective, play); err != nil {
				rescueErr = err
				break
			}
		}
	}

	// Always run always block
	for _, t := range task.Always {
		effective := t
		inherit(&effective)
		if err := e.runTaskOnHosts(ctx, hosts, &effective, play); err != nil {
			if blockErr == nil {
				blockErr = err
			}
		}
	}

	if blockErr != nil && len(task.Rescue) == 0 {
		return blockErr
	}

	if rescueErr != nil {
		return rescueErr
	}

	return nil
}

// runIncludeTasks handles include_tasks/import_tasks.
func (e *Executor) runIncludeTasks(
	ctx context.Context, hosts []string, task *Task, play *Play,
) error {
	path := task.IncludeTasks
	if path == "" {
		path = task.ImportTasks
	}

	if path == "" || len(hosts) == 0 {
		return nil
	}

	// Dynamic include_tasks honours the include task's when-clause before the
	// child task file is expanded. Static import_tasks inherits the condition
	// onto each child task so it can be re-evaluated after earlier tasks run.
	if task.ImportTasks == "" && task.When != nil {
		filtered := make([]string, 0, len(hosts))
		for _, host := range hosts {
			if e.evaluateWhen(task.When, host, task) {
				filtered = append(filtered, host)
			}
		}
		hosts = filtered
		if len(hosts) == 0 {
			return nil
		}
	}

	// Resolve the include path per host so host-specific vars can select a
	// different task file for each target.
	hostsByPath := make(map[string][]string)
	pathOrder := make([]string, 0, len(hosts))
	for _, host := range hosts {
		resolvedPath := e.templateString(path, host, task)
		if resolvedPath == "" {
			continue
		}
		if _, ok := hostsByPath[resolvedPath]; !ok {
			pathOrder = append(pathOrder, resolvedPath)
		}
		hostsByPath[resolvedPath] = append(hostsByPath[resolvedPath], host)
	}

	for _, resolvedPath := range pathOrder {
		tasksResult := e.parser.ParseTasks(resolvedPath)
		if !tasksResult.OK {
			return coreerr.E("Executor.runIncludeTasks", "include_tasks "+resolvedPath+": "+resultErrorMessage(tasksResult), nil)
		}
		tasks := tasksResult.Value.([]Task)

		for _, targetHost := range hostsByPath[resolvedPath] {
			for _, t := range tasks {
				effectiveTask := t
				effectiveTask.Vars = mergeTaskVars(task.Vars, t.Vars)
				e.applyRoleTaskDefaults(&effectiveTask, task.Apply)
				if task.ImportTasks != "" && task.When != nil {
					effectiveTask.When = mergeConditions(task.When, effectiveTask.When)
				}
				if len(effectiveTask.Vars) > 0 {
					effectiveTask.Vars = e.templateArgs(effectiveTask.Vars, targetHost, &effectiveTask)
				}
				if len(task.Tags) > 0 {
					effectiveTask.Tags = mergeStringSlices(task.Tags, effectiveTask.Tags)
				}
				if err := e.runTaskOnHosts(ctx, []string{targetHost}, &effectiveTask, play); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// runIncludeRole handles include_role/import_role.
func (e *Executor) runIncludeRole(
	ctx context.Context, hosts []string, task *Task, play *Play,
) error {
	if len(hosts) == 0 {
		return nil
	}

	for _, host := range hosts {
		roleRef := e.resolveIncludeRoleRef(host, task)
		if roleRef == nil || roleRef.Role == "" {
			continue
		}
		inheritedWhen := any(nil)
		if task.ImportRole != nil {
			inheritedWhen = roleRef.When
			roleRef.When = nil
		}

		if err := e.runRole(ctx, []string{host}, roleRef, play, inheritedWhen); err != nil {
			return err
		}
	}

	return nil
}

func (e *Executor) resolveIncludeRoleRef(host string, task *Task) *RoleRef {
	if task == nil {
		return nil
	}

	var ref *RoleRef
	if task.IncludeRole != nil {
		ref = task.IncludeRole
	} else if task.ImportRole != nil {
		ref = task.ImportRole
	} else {
		return nil
	}

	roleName := ref.Role
	if roleName == "" {
		roleName = ref.Name
	}

	tasksFrom := ref.TasksFrom
	defaultsFrom := ref.DefaultsFrom
	varsFrom := ref.VarsFrom
	handlersFrom := ref.HandlersFrom
	roleVars := ref.Vars
	apply := ref.Apply

	renderedVars := mergeTaskVars(roleVars, task.Vars)
	if len(renderedVars) > 0 {
		renderedVars = e.templateArgs(renderedVars, host, task)
	}

	return &RoleRef{
		Role:         e.templateString(roleName, host, task),
		TasksFrom:    e.templateString(tasksFrom, host, task),
		DefaultsFrom: e.templateString(defaultsFrom, host, task),
		VarsFrom:     e.templateString(varsFrom, host, task),
		HandlersFrom: e.templateString(handlersFrom, host, task),
		Vars:         renderedVars,
		Apply:        apply,
		Public:       ref.Public,
		When:         mergeConditions(ref.When, task.When),
		Tags:         mergeStringSlices(ref.Tags, task.Tags),
	}
}

// mergeTaskVars combines include-task vars with child task vars.
func mergeTaskVars(parent, child map[string]any) map[string]any {
	if len(parent) == 0 && len(child) == 0 {
		return nil
	}

	merged := make(map[string]any, len(parent)+len(child))
	for k, v := range parent {
		merged[k] = v
	}
	for k, v := range child {
		merged[k] = v
	}
	return merged
}

func mergeStringMap(parent, child map[string]string) map[string]string {
	if len(parent) == 0 && len(child) == 0 {
		return nil
	}

	merged := make(map[string]string, len(parent)+len(child))
	for k, v := range parent {
		merged[k] = v
	}
	for k, v := range child {
		merged[k] = v
	}
	return merged
}

func (e *Executor) applyRoleTaskDefaults(task *Task, apply *TaskApply) {
	if task == nil || apply == nil {
		return
	}

	if len(apply.Tags) > 0 {
		task.Tags = mergeStringSlices(apply.Tags, task.Tags)
	}
	if len(apply.Vars) > 0 {
		task.Vars = mergeTaskVars(apply.Vars, task.Vars)
	}
	if len(apply.Environment) > 0 {
		task.Environment = mergeStringMap(apply.Environment, task.Environment)
	}
	if apply.When != nil {
		task.When = mergeConditions(apply.When, task.When)
	}
	if apply.Become != nil && task.Become == nil {
		task.Become = apply.Become
	}
	if apply.BecomeUser != "" && task.BecomeUser == "" {
		task.BecomeUser = apply.BecomeUser
	}
	if apply.Delegate != "" && task.Delegate == "" {
		task.Delegate = apply.Delegate
	}
	if apply.DelegateFacts {
		task.DelegateFacts = true
	}
	if apply.RunOnce && !task.RunOnce {
		task.RunOnce = true
	}
	if apply.NoLog {
		task.NoLog = true
	}
	if apply.IgnoreErrors {
		task.IgnoreErrors = true
	}
}

func mergeConditions(parent, child any) any {
	merged := make([]string, 0)
	merged = append(merged, normalizeConditions(parent)...)
	merged = append(merged, normalizeConditions(child)...)
	if len(merged) == 0 {
		return nil
	}
	return merged
}

// getHosts returns hosts matching the pattern.
func (e *Executor) getHosts(pattern string) []string {
	if e.inventory == nil {
		if pattern == "localhost" {
			return []string{"localhost"}
		}
		return nil
	}

	hosts := GetHosts(e.inventory, pattern)

	// Apply limit - filter to hosts that are also in the limit group
	if e.Limit != "" {
		limitHosts := GetHosts(e.inventory, e.Limit)
		limitSet := make(map[string]bool)
		for _, h := range limitHosts {
			limitSet[h] = true
		}

		var filtered []string
		for _, h := range hosts {
			if limitSet[h] || h == e.Limit {
				filtered = append(filtered, h)
			}
		}
		hosts = filtered
	}

	return hosts
}

// getClient returns or creates an SSH client for a host.
func (e *Executor) getClient(host string, play *Play) core.Result {
	// Get host vars
	vars := make(map[string]any)
	if e.inventory != nil {
		vars = GetHostVars(e.inventory, host)
	}

	// Merge with play vars
	for k, v := range e.vars {
		// Executor-scoped vars include play vars and extra vars, so they must
		// override inventory values when they target the same key.
		vars[k] = v
	}

	// Build SSH config
	cfg := SSHConfig{
		Host: host,
		Port: 22,
		User: "root",
	}

	if h, ok := vars["ansible_host"].(string); ok {
		cfg.Host = h
	}
	if p, ok := vars["ansible_port"].(int); ok {
		cfg.Port = p
	}
	if u, ok := vars["ansible_user"].(string); ok {
		cfg.User = u
	}
	if p, ok := vars["ansible_password"].(string); ok {
		cfg.Password = p
	}
	if k, ok := vars["ansible_ssh_private_key_file"].(string); ok {
		cfg.KeyFile = k
	}

	desiredLocal := isLocalConnection(host, play, vars)
	desiredBecome := play != nil && play.Become
	desiredBecomeUser := ""
	desiredBecomePass := ""
	if desiredBecome {
		desiredBecomeUser = play.BecomeUser
		if bp, ok := vars["ansible_become_password"].(string); ok {
			desiredBecomePass = bp
		} else if cfg.Password != "" {
			desiredBecomePass = cfg.Password
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if client, ok := e.clients[host]; ok {
		switch cached := client.(type) {
		case *localClient:
			if desiredLocal {
				cached.SetBecome(desiredBecome, desiredBecomeUser, desiredBecomePass)
				return core.Ok(sshExecutorClient(cached))
			}
			closeExecutorClient(cached)
			delete(e.clients, host)
		case *SSHClient:
			if !desiredLocal &&
				cached.host == cfg.Host &&
				cached.port == cfg.Port &&
				cached.user == cfg.User &&
				cached.password == cfg.Password &&
				cached.keyFile == cfg.KeyFile {
				cached.SetBecome(desiredBecome, desiredBecomeUser, desiredBecomePass)
				return core.Ok(sshExecutorClient(cached))
			}
			closeExecutorClient(cached)
			delete(e.clients, host)
		default:
			client.SetBecome(desiredBecome, desiredBecomeUser, desiredBecomePass)
			return core.Ok(client)
		}
	}

	if desiredLocal {
		client := newLocalClient()
		client.SetBecome(desiredBecome, desiredBecomeUser, desiredBecomePass)
		e.clients[host] = client
		return core.Ok(sshExecutorClient(client))
	}

	// Apply play become settings
	if desiredBecome {
		cfg.Become = true
		cfg.BecomeUser = desiredBecomeUser
		cfg.BecomePass = desiredBecomePass
	}

	clientResult := NewSSHClient(cfg)
	if !clientResult.OK {
		return clientResult
	}
	client := clientResult.Value.(*SSHClient)

	e.clients[host] = client
	return core.Ok(sshExecutorClient(client))
}

// resolveLocalPath resolves a local file path relative to the executor's base
// path when possible.
func (e *Executor) resolveLocalPath(path string) string {
	if path == "" || e == nil || e.parser == nil {
		return path
	}

	return e.parser.resolvePath(path)
}

// gatherFacts collects facts from a host.
func (e *Executor) gatherFacts(
	ctx context.Context, host string, play *Play,
) error {
	clientResult := e.getClient(host, play)
	if !clientResult.OK {
		return coreerr.E("Executor.gatherFacts", "get client: "+resultErrorMessage(clientResult), nil)
	}
	client := clientResult.Value.(sshExecutorClient)

	factsResult := e.collectFacts(ctx, client, false)
	if !factsResult.OK {
		return coreerr.E("Executor.gatherFacts", "collect facts: "+resultErrorMessage(factsResult), nil)
	}
	facts := factsResult.Value.(*Facts)

	e.mu.Lock()
	e.facts[host] = facts
	e.mu.Unlock()

	return nil
}

func isLocalConnection(host string, play *Play, vars map[string]any) bool {
	if host == "localhost" {
		return true
	}
	if play != nil && corexTrimSpace(play.Connection) == "local" {
		return true
	}
	if conn, ok := vars["ansible_connection"].(string); ok && corexTrimSpace(conn) == "local" {
		return true
	}
	return false
}

// evaluateWhen evaluates a when condition.
func (e *Executor) evaluateWhen(when any, host string, task *Task) bool {
	return e.evaluateWhenWithLocals(when, host, task, nil)
}

func (e *Executor) evaluateWhenWithLocals(when any, host string, task *Task, locals map[string]any) bool {
	conditions := normalizeConditions(when)

	for _, cond := range conditions {
		cond = e.templateString(cond, host, task)
		if !e.evalConditionWithLocals(cond, host, task, locals) {
			return false
		}
	}

	return true
}

func normalizeConditions(when any) []string {
	switch v := when.(type) {
	case string:
		return []string{v}
	case []any:
		var conds []string
		for _, c := range v {
			if s, ok := c.(string); ok {
				conds = append(conds, s)
			}
		}
		return conds
	case []string:
		return v
	}
	return nil
}

// evalCondition evaluates a single condition.
func (e *Executor) evalCondition(cond string, host string) bool {
	return e.evalConditionWithLocals(cond, host, nil, nil)
}

func (e *Executor) evalConditionWithLocals(cond string, host string, task *Task, locals map[string]any) bool {
	cond = corexTrimSpace(cond)
	if cond == "" {
		return true
	}

	if inner, ok := stripOuterParens(cond); ok {
		return e.evalConditionWithLocals(inner, host, task, locals)
	}

	if left, right, ok := splitLogicalCondition(cond, "or"); ok {
		return e.evalConditionWithLocals(left, host, task, locals) || e.evalConditionWithLocals(right, host, task, locals)
	}
	if left, right, ok := splitLogicalCondition(cond, "and"); ok {
		return e.evalConditionWithLocals(left, host, task, locals) && e.evalConditionWithLocals(right, host, task, locals)
	}

	// Handle negation
	if corexHasPrefix(cond, "not ") {
		return !e.evalConditionWithLocals(corexTrimPrefix(cond, "not "), host, task, locals)
	}

	if result, handled := e.evalBinaryCondition(cond, host, task, locals); handled {
		return result
	}

	// Handle boolean literals
	if cond == "true" || cond == "True" {
		return true
	}
	if cond == "false" || cond == "False" {
		return false
	}

	// Handle registered variable checks
	// e.g., "result is success", "result.rc == 0"
	if contains(cond, " is ") {
		parts := splitN(cond, " is ", 2)
		varName := corexTrimSpace(parts[0])
		check := corexTrimSpace(parts[1])

		if result, ok := e.lookupConditionValue(varName, host, task, locals); ok {
			switch v := result.(type) {
			case *TaskResult:
				switch check {
				case "defined":
					return true
				case "not defined", "undefined":
					return false
				case "success", "succeeded":
					return !v.Failed
				case "failed":
					return v.Failed
				case "changed":
					return v.Changed
				case "skipped":
					return v.Skipped
				}
			case TaskResult:
				switch check {
				case "defined":
					return true
				case "not defined", "undefined":
					return false
				case "success", "succeeded":
					return !v.Failed
				case "failed":
					return v.Failed
				case "changed":
					return v.Changed
				case "skipped":
					return v.Skipped
				}
			}
			return true
		}

		if check == "not defined" || check == "undefined" {
			return true
		}
		return false
	}

	// Handle simple var checks
	if contains(cond, " | default(") {
		// Extract var name and check if defined
		re := regexp.MustCompile(`(\w+)\s*\|\s*default\([^)]*\)`)
		if match := re.FindStringSubmatch(cond); len(match) > 1 {
			// Has default, so condition is satisfied
			return true
		}
	}

	// Check if it's a variable that should be truthy
	if result, ok := e.lookupConditionValue(cond, host, task, locals); ok {
		switch v := result.(type) {
		case *TaskResult:
			return !v.Failed && !v.Skipped
		case TaskResult:
			return !v.Failed && !v.Skipped
		case bool:
			return v
		case string:
			return v != "" && v != "false" && v != "False"
		case int:
			return v != 0
		case int64:
			return v != 0
		case float64:
			return v != 0
		}
	}

	// Check vars
	if val, ok := e.vars[cond]; ok {
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return v != "" && v != "false" && v != "False"
		case int:
			return v != 0
		}
	}

	// Default to true for unknown conditions (be permissive)
	return true
}

func stripOuterParens(cond string) (string, bool) {
	cond = corexTrimSpace(cond)
	if len(cond) < 2 || cond[0] != '(' || cond[len(cond)-1] != ')' {
		return "", false
	}

	depth := 0
	inSingle := false
	inDouble := false
	escaped := false
	for i := 0; i < len(cond); i++ {
		ch := cond[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && (inSingle || inDouble) {
			escaped = true
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(cond)-1 {
				return "", false
			}
			if depth < 0 {
				return "", false
			}
		}
	}

	if depth != 0 {
		return "", false
	}

	return corexTrimSpace(cond[1 : len(cond)-1]), true
}

func splitLogicalCondition(cond, op string) (string, string, bool) {
	depth := 0
	inSingle := false
	inDouble := false
	escaped := false
	for i := 0; i <= len(cond)-len(op); i++ {
		ch := cond[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && (inSingle || inDouble) {
			escaped = true
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth != 0 || !hasPrefix(cond[i:], op) {
			continue
		}

		if i > 0 {
			prev := cond[i-1]
			if !isConditionBoundary(prev) {
				continue
			}
		}
		if end := i + len(op); end < len(cond) {
			if !isConditionBoundary(cond[end]) {
				continue
			}
		}

		left := corexTrimSpace(cond[:i])
		right := corexTrimSpace(cond[i+len(op):])
		if left == "" || right == "" {
			continue
		}
		return left, right, true
	}

	return "", "", false
}

func isConditionBoundary(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r', '(', ')':
		return true
	default:
		return false
	}
}

// evalBinaryCondition evaluates recognised binary when-expression operators.
func (e *Executor) evalBinaryCondition(cond string, host string, task *Task, locals map[string]any) (bool, bool) {
	ops := []string{"not in", "contains", "<=", ">=", "==", "!=", "<", ">", "in"}
	for _, op := range ops {
		left, right, ok := splitBinaryCondition(cond, op)
		if !ok {
			continue
		}

		leftValue, leftOK := e.resolveConditionOperandValue(left, host, task, locals)
		rightValue, rightOK := e.resolveConditionOperandValue(right, host, task, locals)
		if !leftOK || !rightOK {
			return false, true
		}

		switch op {
		case "==":
			return templateConditionEqual(leftValue, rightValue), true
		case "!=":
			return !templateConditionEqual(leftValue, rightValue), true
		case "<":
			return templateConditionCompare(leftValue, rightValue) < 0, true
		case ">":
			return templateConditionCompare(leftValue, rightValue) > 0, true
		case "<=":
			return templateConditionCompare(leftValue, rightValue) <= 0, true
		case ">=":
			return templateConditionCompare(leftValue, rightValue) >= 0, true
		case "in":
			return templateConditionContains(rightValue, leftValue), true
		case "not in":
			return !templateConditionContains(rightValue, leftValue), true
		case "contains":
			return templateConditionContains(leftValue, rightValue), true
		}
	}

	return false, false
}

// splitBinaryCondition splits a condition around an operator outside quotes.
func splitBinaryCondition(cond, op string) (string, string, bool) {
	depth := 0
	inSingle := false
	inDouble := false
	escaped := false
	wordOp := isWordConditionOperator(op)

	for i := 0; i <= len(cond)-len(op); i++ {
		ch := cond[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && (inSingle || inDouble) {
			escaped = true
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth != 0 || !hasPrefix(cond[i:], op) {
			continue
		}

		end := i + len(op)
		if wordOp {
			if i > 0 && !isConditionBoundary(cond[i-1]) {
				continue
			}
			if end < len(cond) && !isConditionBoundary(cond[end]) {
				continue
			}
		}

		left := corexTrimSpace(cond[:i])
		right := corexTrimSpace(cond[end:])
		if left == "" || right == "" {
			continue
		}
		return left, right, true
	}

	return "", "", false
}

// isWordConditionOperator reports whether an operator needs token boundaries.
func isWordConditionOperator(op string) bool {
	for i := 0; i < len(op); i++ {
		ch := op[i]
		if ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') {
			return true
		}
	}
	return false
}

// resolveConditionOperandValue resolves a condition operand to a native value.
func (e *Executor) resolveConditionOperandValue(expr string, host string, task *Task, locals map[string]any) (any, bool) {
	expr = corexTrimSpace(expr)
	if expr == "" {
		return "", true
	}

	switch expr {
	case "true", "True":
		return true, true
	case "false", "False":
		return false, true
	}

	if len(expr) >= 2 {
		if (expr[0] == '\'' && expr[len(expr)-1] == '\'') || (expr[0] == '"' && expr[len(expr)-1] == '"') {
			return expr[1 : len(expr)-1], true
		}
	}

	if i, err := strconv.Atoi(expr); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f, true
	}

	if value, ok := e.lookupConditionValue(expr, host, task, locals); ok {
		return value, true
	}

	return expr, false
}

// templateConditionEqual compares condition operands using numeric coercion first.
func templateConditionEqual(left, right any) bool {
	if leftFloat, leftOK := templateFloat(left); leftOK {
		if rightFloat, rightOK := templateFloat(right); rightOK {
			return leftFloat == rightFloat
		}
	}

	return reflect.DeepEqual(left, right) || templateStringify(left) == templateStringify(right)
}

// templateConditionCompare orders condition operands numerically or lexically.
func templateConditionCompare(left, right any) int {
	if leftFloat, leftOK := templateFloat(left); leftOK {
		if rightFloat, rightOK := templateFloat(right); rightOK {
			switch {
			case leftFloat < rightFloat:
				return -1
			case leftFloat > rightFloat:
				return 1
			default:
				return 0
			}
		}
	}

	leftStr := templateStringify(left)
	rightStr := templateStringify(right)
	switch {
	case leftStr < rightStr:
		return -1
	case leftStr > rightStr:
		return 1
	default:
		return 0
	}
}

// templateConditionContains implements Ansible-style membership checks.
func templateConditionContains(container, item any) bool {
	if container == nil {
		return false
	}

	if containerStr, ok := container.(string); ok {
		return contains(containerStr, templateStringify(item))
	}

	if items, ok := anySliceFromValue(container); ok {
		for _, candidate := range items {
			if templateConditionEqual(candidate, item) || templateStringify(candidate) == templateStringify(item) {
				return true
			}
		}
		return false
	}

	rv := reflect.ValueOf(container)
	if !rv.IsValid() {
		return false
	}
	if rv.Kind() == reflect.Map {
		for _, key := range rv.MapKeys() {
			if templateStringify(key.Interface()) == templateStringify(item) {
				return true
			}
			if templateConditionEqual(key.Interface(), item) {
				return true
			}
		}
		return false
	}

	return contains(templateStringify(container), templateStringify(item))
}

func (e *Executor) lookupConditionValue(name string, host string, task *Task, locals map[string]any) (any, bool) {
	name = corexTrimSpace(name)

	if locals != nil {
		if val, ok := locals[name]; ok {
			return val, true
		}

		parts := splitN(name, ".", 2)
		if len(parts) == 2 {
			if base, ok := locals[parts[0]]; ok {
				if value, ok := taskResultField(base, parts[1]); ok {
					return value, true
				}
			}
		}
	}

	if result := e.getRegisteredVar(host, name); result != nil {
		if len(splitN(name, ".", 2)) == 2 {
			parts := splitN(name, ".", 2)
			if value, ok := taskResultField(result, parts[1]); ok {
				return value, true
			}
		}
		return result, true
	}

	if task != nil {
		if val, ok := task.Vars[name]; ok {
			return val, true
		}
	}

	if hostVars := e.hostScopedVars(host); hostVars != nil {
		if val, ok := hostVars[name]; ok {
			return val, true
		}
	}

	if val, ok := e.vars[name]; ok {
		return val, true
	}

	if e.inventory != nil {
		hostVars := GetHostVars(e.inventory, host)
		if val, ok := hostVars[name]; ok {
			return val, true
		}
	}

	if facts, ok := e.facts[host]; ok {
		if name == "ansible_facts" {
			if merged := e.hostFactsMap(host); len(merged) > 0 {
				return merged, true
			}
			return factsToMap(facts), true
		}
		switch name {
		case "ansible_hostname":
			return facts.Hostname, true
		case "ansible_fqdn":
			return facts.FQDN, true
		case "ansible_os_family":
			return facts.OS, true
		case "ansible_memtotal_mb":
			return facts.Memory, true
		case "ansible_processor_vcpus":
			return facts.CPUs, true
		case "ansible_default_ipv4_address":
			return facts.IPv4, true
		case "ansible_distribution":
			return facts.Distribution, true
		case "ansible_distribution_version":
			return facts.Version, true
		case "ansible_architecture":
			return facts.Architecture, true
		case "ansible_kernel":
			return facts.Kernel, true
		case "ansible_virtualization_role":
			return facts.VirtualizationRole, true
		case "ansible_virtualization_type":
			return facts.VirtualizationType, true
		}
	}

	if value, ok := e.lookupMagicScopeValue(name, host); ok {
		return value, true
	}
	if value, ok := e.hostMagicVars(host)[name]; ok {
		return value, true
	}

	if contains(name, ".") {
		parts := splitN(name, ".", 2)
		base := parts[0]
		path := parts[1]

		if locals != nil {
			if val, ok := locals[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}

		if base == "ansible_facts" {
			if merged := e.hostFactsMap(host); len(merged) > 0 {
				if nested, ok := lookupNestedValue(merged, path); ok {
					return nested, true
				}
			}
			if facts, ok := e.facts[host]; ok {
				if nested, ok := lookupNestedValue(factsToMap(facts), path); ok {
					return nested, true
				}
			}
		}

		if task != nil {
			if val, ok := task.Vars[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}

		if hostVars := e.hostScopedVars(host); hostVars != nil {
			if val, ok := hostVars[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}

		if val, ok := e.vars[base]; ok {
			if nested, ok := lookupNestedValue(val, path); ok {
				return nested, true
			}
		}

		if e.inventory != nil {
			hostVars := GetHostVars(e.inventory, host)
			if val, ok := hostVars[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}

		if magic, ok := e.lookupMagicScopeValue(base, host); ok {
			if nested, ok := lookupNestedValue(magic, path); ok {
				return nested, true
			}
		}

		if magic, ok := e.hostMagicVars(host)[base]; ok {
			if nested, ok := lookupNestedValue(magic, path); ok {
				return nested, true
			}
		}
	}

	return nil, false
}

func taskResultField(value any, field string) (any, bool) {
	switch v := value.(type) {
	case *TaskResult:
		return taskResultField(*v, field)
	case TaskResult:
		switch field {
		case "stdout":
			return v.Stdout, true
		case "stderr":
			return v.Stderr, true
		case "rc":
			return v.RC, true
		case "changed":
			return v.Changed, true
		case "failed":
			return v.Failed, true
		case "skipped":
			return v.Skipped, true
		case "msg":
			return v.Msg, true
		}
	case map[string]any:
		if val, ok := v[field]; ok {
			return val, true
		}
	}
	return nil, false
}

func (e *Executor) resolveConditionOperand(expr string, host string, task *Task, locals map[string]any) (string, bool) {
	value, ok := e.resolveConditionOperandValue(expr, host, task, locals)
	if !ok {
		return corexTrimSpace(expr), false
	}
	return templateStringify(value), true
}

func (e *Executor) applyTaskResultConditions(host string, task *Task, result *TaskResult) {
	if result == nil || task == nil {
		return
	}

	locals := map[string]any{
		"result":   result,
		"stdout":   result.Stdout,
		"stderr":   result.Stderr,
		"rc":       result.RC,
		"changed":  result.Changed,
		"failed":   result.Failed,
		"skipped":  result.Skipped,
		"msg":      result.Msg,
		"duration": result.Duration,
	}

	if task.ChangedWhen != nil {
		result.Changed = e.evaluateWhenWithLocals(task.ChangedWhen, host, task, locals)
		locals["changed"] = result.Changed
		locals["result"] = result
	}

	if task.FailedWhen != nil {
		result.Failed = e.evaluateWhenWithLocals(task.FailedWhen, host, task, locals)
		locals["failed"] = result.Failed
		locals["result"] = result
	}
}

// getRegisteredVar gets a registered task result.
func (e *Executor) getRegisteredVar(host string, name string) *TaskResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Handle dotted access (e.g., "result.stdout")
	parts := splitN(name, ".", 2)
	varName := parts[0]

	if hostResults, ok := e.results[host]; ok {
		if result, ok := hostResults[varName]; ok {
			return result
		}
	}

	return nil
}

// templateString applies Jinja2-like templating.
func (e *Executor) templateString(s string, host string, task *Task) string {
	// Handle {{ var }} syntax
	re := regexp.MustCompile(`\{\{\s*([^}]+)\s*\}\}`)

	return re.ReplaceAllStringFunc(s, func(match string) string {
		expr := corexTrimSpace(match[2 : len(match)-2])
		return e.resolveExpr(expr, host, task)
	})
}

// resolveExpr resolves a template expression.
func (e *Executor) resolveExpr(expr string, host string, task *Task) string {
	value, _ := e.resolveExprValue(expr, host, task)
	return templateStringify(value)
}

// resolveExprBase resolves a single templating expression without applying filters.
func (e *Executor) resolveExprBase(expr string, host string, task *Task) string {
	value, ok := e.resolveExprBaseValue(expr, host, task)
	if !ok {
		return "{{ " + corexTrimSpace(expr) + " }}"
	}
	return templateStringify(value)
}

// splitTemplatePipeline splits a template expression into a base expression
// and any chained filters, preserving quoted or parenthesised filter arguments.
func splitTemplatePipeline(expr string) []string {
	expr = corexTrimSpace(expr)
	if expr == "" {
		return nil
	}

	parts := make([]string, 0, 4)
	current := newBuilder()
	var (
		depth    int
		inSingle bool
		inDouble bool
		escaped  bool
	)

	flush := func() {
		part := corexTrimSpace(current.String())
		if part != "" {
			parts = append(parts, part)
		}
		current.Reset()
	}

	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		switch {
		case escaped:
			current.WriteByte(ch)
			escaped = false
		case ch == '\\' && (inSingle || inDouble):
			current.WriteByte(ch)
			escaped = true
		case ch == '\'' && !inDouble:
			current.WriteByte(ch)
			inSingle = !inSingle
		case ch == '"' && !inSingle:
			current.WriteByte(ch)
			inDouble = !inDouble
		case inSingle || inDouble:
			current.WriteByte(ch)
		case ch == '(':
			depth++
			current.WriteByte(ch)
		case ch == ')':
			if depth > 0 {
				depth--
			}
			current.WriteByte(ch)
		case ch == '|' && depth == 0:
			flush()
		default:
			current.WriteByte(ch)
		}
	}

	flush()
	if len(parts) == 0 {
		return nil
	}
	return parts
}

// buildEnvironmentPrefix renders merged play/task environment variables as
// shell export statements.
func (e *Executor) buildEnvironmentPrefix(host string, task *Task, play *Play) string {
	env := make(map[string]string)

	if play != nil {
		for key, value := range play.Environment {
			env[key] = value
		}
	}
	if task != nil {
		for key, value := range task.Environment {
			env[key] = value
		}
	}

	if len(env) == 0 {
		return ""
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		renderedKey := e.templateString(key, host, task)
		if renderedKey == "" {
			continue
		}

		renderedValue := e.templateString(env[key], host, task)
		parts = append(parts, sprintf("export %s=%s", renderedKey, shellQuote(renderedValue)))
	}

	if len(parts) == 0 {
		return ""
	}

	return join("; ", parts) + "; "
}

// shellQuote wraps a string in single quotes for shell use.
func shellQuote(value string) string {
	return "'" + replaceAll(value, "'", "'\\''") + "'"
}

// lookupExprValue resolves the first segment of an expression against the
// executor, task, and inventory scopes.
func (e *Executor) lookupExprValue(name string, host string, task *Task) (any, bool) {
	if task != nil {
		if val, ok := task.Vars[name]; ok {
			return val, true
		}
	}
	if hostVars := e.hostScopedVars(host); hostVars != nil {
		if val, ok := hostVars[name]; ok {
			return val, true
		}
	}
	if val, ok := e.vars[name]; ok {
		return val, true
	}
	if e.inventory != nil {
		hostVars := GetHostVars(e.inventory, host)
		if val, ok := hostVars[name]; ok {
			return val, true
		}
	}

	if name == "ansible_facts" {
		if facts := e.hostFactsMap(host); len(facts) > 0 {
			return facts, true
		}
	}

	if facts, ok := e.facts[host]; ok {
		switch name {
		case "ansible_hostname":
			return facts.Hostname, true
		case "ansible_fqdn":
			return facts.FQDN, true
		case "ansible_os_family":
			return facts.OS, true
		case "ansible_memtotal_mb":
			return facts.Memory, true
		case "ansible_processor_vcpus":
			return facts.CPUs, true
		case "ansible_default_ipv4_address":
			return facts.IPv4, true
		case "ansible_distribution":
			return facts.Distribution, true
		case "ansible_distribution_version":
			return facts.Version, true
		case "ansible_architecture":
			return facts.Architecture, true
		case "ansible_kernel":
			return facts.Kernel, true
		case "ansible_virtualization_role":
			return facts.VirtualizationRole, true
		case "ansible_virtualization_type":
			return facts.VirtualizationType, true
		}
	}

	if value, ok := e.lookupMagicScopeValue(name, host); ok {
		return value, true
	}
	if value, ok := e.hostMagicVars(host)[name]; ok {
		return value, true
	}

	if contains(name, ".") {
		parts := splitN(name, ".", 2)
		base := parts[0]
		path := parts[1]

		if base == "ansible_facts" {
			if facts := e.hostFactsMap(host); len(facts) > 0 {
				if nested, ok := lookupNestedValue(facts, path); ok {
					return nested, true
				}
			}
		}

		if hostVars := e.hostScopedVars(host); hostVars != nil {
			if val, ok := hostVars[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}
		if task != nil {
			if val, ok := task.Vars[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}
		if val, ok := e.vars[base]; ok {
			if nested, ok := lookupNestedValue(val, path); ok {
				return nested, true
			}
		}
		if e.inventory != nil {
			hostVars := GetHostVars(e.inventory, host)
			if val, ok := hostVars[base]; ok {
				if nested, ok := lookupNestedValue(val, path); ok {
					return nested, true
				}
			}
		}

		if value, ok := e.lookupMagicScopeValue(base, host); ok {
			if nested, ok := lookupNestedValue(value, path); ok {
				return nested, true
			}
		}
	}
	return nil, false
}

func (e *Executor) lookupMagicScopeValue(name string, host string) (any, bool) {
	if e == nil || e.inventory == nil {
		return nil, false
	}

	switch name {
	case "groups":
		if groups := inventoryGroupHosts(e.inventory); len(groups) > 0 {
			return groups, true
		}
	case "hostvars":
		if hostvars := inventoryHostVars(e.inventory); len(hostvars) > 0 {
			return hostvars, true
		}
	}

	return nil, false
}

// lookupNestedValue walks a dotted path through nested maps.
func lookupNestedValue(value any, path string) (any, bool) {
	if path == "" {
		return value, true
	}

	current := value
	for _, segment := range split(path, ".") {
		switch next := current.(type) {
		case map[string]any:
			var ok bool
			current, ok = next[segment]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(next) {
				return nil, false
			}
			current = next[index]
		case []string:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(next) {
				return nil, false
			}
			current = next[index]
		default:
			return nil, false
		}
	}

	return current, true
}

// applyFilter applies a Jinja2 filter.
func (e *Executor) applyFilter(value, filter string) string {
	return templateStringify(e.applyFilterValue(value, filter))
}

func parseRegexReplaceFilter(filter string) (string, string, bool) {
	if !corexHasPrefix(filter, "regex_replace(") || !corexHasSuffix(filter, ")") {
		return "", "", false
	}

	args := trimSpace(filter[len("regex_replace(") : len(filter)-1])
	parts := splitFilterArgs(args)
	if len(parts) < 2 {
		return "", "", false
	}

	return trimCutset(parts[0], "'\""), trimCutset(parts[1], "'\""), true
}

func splitFilterArgs(args string) []string {
	if args == "" {
		return nil
	}

	current := newBuilder()
	var (
		parts    []string
		inSingle bool
		inDouble bool
		escaped  bool
	)

	for _, r := range args {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\' && (inSingle || inDouble):
			current.WriteRune(r)
			escaped = true
		case r == '\'' && !inDouble:
			current.WriteRune(r)
			inSingle = !inSingle
		case r == '"' && !inSingle:
			current.WriteRune(r)
			inDouble = !inDouble
		case r == ',' && !inSingle && !inDouble:
			part := trimSpace(current.String())
			if part != "" {
				parts = append(parts, part)
			}
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}

	if tail := trimSpace(current.String()); tail != "" {
		parts = append(parts, tail)
	}

	return parts
}

func isUnresolvedTemplateValue(value string) bool {
	return corexHasPrefix(value, "{{ ") && corexHasSuffix(value, " }}")
}

// handleLookup handles lookup() expressions.
func (e *Executor) handleLookup(expr string, host string, task *Task) string {
	value, ok := e.lookupValue(expr, host, task)
	if !ok {
		return ""
	}

	switch v := value.(type) {
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, sprintf("%v", item))
		}
		return join(",", parts)
	case []string:
		return join(",", v)
	default:
		return sprintf("%v", v)
	}
}

func (e *Executor) lookupValue(expr string, host string, task *Task) (any, bool) {
	// Parse lookup('type', 'arg') and accept fully-qualified lookup names.
	re := regexp.MustCompile(`lookup\s*\(\s*['"]([\w.]+)['"]\s*,\s*(.+?)\s*\)`)
	match := re.FindStringSubmatch(expr)
	if len(match) < 3 {
		return nil, false
	}

	lookupType := normalizeLookupName(match[1])
	arg := trimSpace(match[2])
	arg, quoted := unquoteLookupArg(arg)

	switch lookupType {
	case "env":
		return env(arg), true
	case "file":
		if data, err := coreio.Local.Read(e.resolveLocalPath(arg)); err == nil {
			return data, true
		}
	case "template":
		if dataResult := e.TemplateFile(arg, host, task); dataResult.OK {
			return dataResult.Value.(string), true
		}
	case "fileglob":
		matchesResult := e.resolveFileGlob(arg)
		if matchesResult.OK {
			matches := matchesResult.Value.([]any)
			if len(matches) > 0 {
				return matches, true
			}
		}
		return []any{}, true
	case "pipe":
		runResult := runLocalShell(context.Background(), arg, "")
		run := commandRunValue(runResult)
		if runResult.OK && run.ExitCode == 0 {
			return trimRightCutset(run.Stdout, "\r\n"), true
		}
	case "vars":
		if value, ok := e.lookupConditionValue(arg, host, task, nil); ok {
			return value, true
		}
	case "password":
		if value, ok := e.lookupPassword(arg, host, task); ok {
			return value, true
		}
	case "first_found":
		if value, ok := e.lookupFirstFound(arg, quoted, host, task); ok {
			return value, true
		}
	}

	return nil, false
}

func normalizeLookupName(name string) string {
	name = corexTrimSpace(name)
	if name == "" {
		return ""
	}

	if idx := stringLastIndex(name, "."); idx >= 0 {
		return name[idx+1:]
	}

	return name
}

func unquoteLookupArg(arg string) (string, bool) {
	if len(arg) >= 2 {
		if (arg[0] == '\'' && arg[len(arg)-1] == '\'') || (arg[0] == '"' && arg[len(arg)-1] == '"') {
			return arg[1 : len(arg)-1], true
		}
	}
	return arg, false
}

func (e *Executor) lookupFirstFound(arg string, quoted bool, host string, task *Task) (string, bool) {
	resolved := any(arg)
	if !quoted {
		if value, ok := e.lookupConditionValue(arg, host, task, nil); ok {
			resolved = value
		}
	}

	files, paths := firstFoundTerms(resolved)
	if len(files) == 0 {
		return "", false
	}

	candidates := make([]string, 0, len(files))
	if len(paths) == 0 {
		candidates = append(candidates, files...)
	} else {
		for _, base := range paths {
			if base == "" {
				continue
			}
			for _, file := range files {
				if file == "" {
					continue
				}
				if pathIsAbs(file) {
					candidates = append(candidates, file)
					continue
				}
				candidates = append(candidates, joinPath(base, file))
			}
		}
	}

	for _, candidate := range candidates {
		resolvedPath := e.resolveLocalPath(candidate)
		if stat := core.Stat(resolvedPath); stat.OK {
			info := stat.Value.(core.FsFileInfo)
			if !info.IsDir() {
				return resolvedPath, true
			}
		}
	}

	return "", false
}

func firstFoundTerms(value any) ([]string, []string) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return normalizeStringList(v), nil
	case []string:
		return append([]string(nil), v...), nil
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s := corexTrimSpace(corexSprint(item)); s != "" && s != "<nil>" {
				out = append(out, s)
			}
		}
		return out, nil
	case map[string]any:
		files := normalizeStringArgs(v["files"])
		if len(files) == 0 {
			files = normalizeStringArgs(v["terms"])
		}
		paths := normalizeStringArgs(v["paths"])
		return files, paths
	case map[any]any:
		converted := make(map[string]any, len(v))
		for key, val := range v {
			if s, ok := key.(string); ok {
				converted[s] = val
			}
		}
		return firstFoundTerms(converted)
	default:
		s := corexTrimSpace(corexSprint(v))
		if s == "" || s == "<nil>" {
			return nil, nil
		}
		return []string{s}, nil
	}
}

func (e *Executor) lookupPassword(arg string, host string, task *Task) (string, bool) {
	spec := parsePasswordLookupSpec(arg)
	spec.path = e.resolveLookupPasswordValue(spec.path, host, task)
	spec.seed = e.resolveLookupPasswordValue(spec.seed, host, task)
	if spec.path == "" {
		return "", false
	}

	resolvedPath := e.resolveLocalPath(spec.path)
	if resolvedPath != "/dev/null" {
		if data, err := coreio.Local.Read(resolvedPath); err == nil {
			return trimRightCutset(data, "\r\n"), true
		}
	}

	passwordResult := generatePassword(spec.length, spec.chars, spec.seed)
	if !passwordResult.OK {
		return "", false
	}
	password := passwordResult.Value.(string)

	if resolvedPath != "/dev/null" {
		if err := coreio.Local.EnsureDir(pathDir(resolvedPath)); err != nil {
			return "", false
		}
		if err := coreio.Local.Write(resolvedPath, password); err != nil {
			return "", false
		}
	}

	return password, true
}

func (e *Executor) resolveLookupPasswordValue(value string, host string, task *Task) string {
	value = corexTrimSpace(value)
	if value == "" {
		return ""
	}

	if resolved, ok := e.lookupConditionValue(value, host, task, nil); ok {
		return sprintf("%v", resolved)
	}

	if contains(value, "{{") {
		return e.templateString(value, host, task)
	}

	return value
}

type passwordLookupSpec struct {
	path   string
	length int
	chars  string
	seed   string
}

func parsePasswordLookupSpec(arg string) passwordLookupSpec {
	spec := passwordLookupSpec{
		length: 20,
		chars:  passwordLookupCharset("ascii_letters,digits"),
	}

	fields := fields(arg)
	for _, field := range fields {
		key, value, ok := cut(field, "=")
		if ok {
			switch lower(trimSpace(key)) {
			case "length":
				if n, err := strconv.Atoi(trimSpace(value)); err == nil && n > 0 {
					spec.length = n
				}
			case "chars":
				if chars := passwordLookupCharset(value); chars != "" {
					spec.chars = chars
				}
			case "seed":
				spec.seed = value
			}
			continue
		}

		if spec.path == "" {
			spec.path = field
		}
	}

	return spec
}

func passwordLookupCharset(value string) string {
	if value == "" {
		return ""
	}

	chars := newBuilder()
	seen := make(map[rune]bool)
	appendChars := func(set string) {
		for _, r := range set {
			if seen[r] {
				continue
			}
			seen[r] = true
			chars.WriteRune(r)
		}
	}

	for _, token := range split(value, ",") {
		switch lower(trimSpace(token)) {
		case "ascii_letters":
			appendChars("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
		case "ascii_lowercase":
			appendChars("abcdefghijklmnopqrstuvwxyz")
		case "ascii_uppercase":
			appendChars("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		case "digits":
			appendChars("0123456789")
		case "hexdigits":
			appendChars("0123456789abcdefABCDEF")
		default:
			appendChars(token)
		}
	}

	return chars.String()
}

func generatePassword(length int, chars, seed string) core.Result {
	if length <= 0 {
		length = 20
	}
	if chars == "" {
		chars = passwordLookupCharset("ascii_letters,digits")
	}

	if seed != "" {
		sum := sha256.Sum256([]byte(seed))
		seedValue := int64(binary.LittleEndian.Uint64(sum[:8]))
		r := mathrand.New(mathrand.NewSource(seedValue))
		buf := make([]byte, length)
		for i := range buf {
			buf[i] = chars[r.Intn(len(chars))]
		}
		return core.Ok(string(buf))
	}

	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		return core.Fail(coreerr.E("Executor.lookupPassword", "generate password", err))
	}

	output := make([]byte, length)
	for i, b := range buf {
		output[i] = chars[int(b)%len(chars)]
	}

	return core.Ok(string(output))
}

// resolveLoop resolves loop items.
func (e *Executor) resolveLoop(loop any, host string) []any {
	return e.resolveLoopWithTask(loop, host, nil)
}

func (e *Executor) resolveLoopWithTask(loop any, host string, task *Task) []any {
	switch v := loop.(type) {
	case []any:
		items := make([]any, len(v))
		for i, item := range v {
			items[i] = e.templateLoopItem(item, host, task)
		}
		return items
	case []string:
		items := make([]any, len(v))
		for i, s := range v {
			items[i] = e.templateLoopItem(s, host, task)
		}
		return items
	case string:
		if items, ok := e.resolveLoopExpression(v, host, task); ok {
			return items
		}

		// Template the string and see if it's a var reference
		resolved := e.templateString(v, host, task)
		if items, ok := anySliceFromValue(e.vars[resolved]); ok {
			return items
		}
		if task != nil {
			if items, ok := anySliceFromValue(task.Vars[resolved]); ok {
				return items
			}
		}
	}
	return nil
}

func (e *Executor) templateLoopItem(value any, host string, task *Task) any {
	switch v := value.(type) {
	case string:
		return e.templateString(v, host, task)
	case map[string]any:
		tmpl := make(map[string]any, len(v))
		for key, item := range v {
			tmpl[key] = e.templateLoopItem(item, host, task)
		}
		return tmpl
	case []any:
		tmpl := make([]any, len(v))
		for i, item := range v {
			tmpl[i] = e.templateLoopItem(item, host, task)
		}
		return tmpl
	case []string:
		tmpl := make([]any, len(v))
		for i, item := range v {
			tmpl[i] = e.templateLoopItem(item, host, task)
		}
		return tmpl
	default:
		return value
	}
}

func (e *Executor) resolveLoopExpression(loop string, host string, task *Task) ([]any, bool) {
	expr, ok := extractSingleTemplateExpression(loop)
	if !ok {
		return nil, false
	}

	parts := splitTemplatePipeline(expr)
	if len(parts) == 0 {
		return nil, false
	}

	if value, ok := e.resolveLoopExpressionValue(parts, host, task); ok {
		if items, ok := anySliceFromValue(value); ok {
			return items, true
		}
	}

	return nil, false
}

func (e *Executor) resolveLoopExpressionValue(parts []string, host string, task *Task) (any, bool) {
	if len(parts) == 0 {
		return nil, false
	}

	baseExpr := corexTrimSpace(parts[0])
	if corexHasPrefix(baseExpr, "lookup(") {
		if value, ok := e.lookupValue(baseExpr, host, task); ok {
			if items, ok := anySliceFromValue(value); ok {
				return items, true
			}
			if !isEmptyLoopValue(value) {
				return value, true
			}
		}
	}

	value, ok := e.lookupExprValue(baseExpr, host, task)
	if !ok || isEmptyLoopValue(value) {
		if fallback, found := resolveDefaultLoopValue(parts[1:]); found {
			return fallback, true
		}
		if ok {
			return value, true
		}
		return nil, false
	}

	return value, true
}

func isEmptyLoopValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return corexTrimSpace(v) == ""
	case []any:
		return len(v) == 0
	case []string:
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

func resolveDefaultLoopValue(filters []string) (any, bool) {
	for _, filter := range filters {
		filter = corexTrimSpace(filter)
		if !corexHasPrefix(filter, "default(") || !corexHasSuffix(filter, ")") {
			continue
		}

		raw := trimSpace(filter[len("default(") : len(filter)-1])
		if raw == "" {
			return []any{}, true
		}

		var value any
		if err := yaml.Unmarshal([]byte(raw), &value); err != nil {
			return trimCutset(raw, "'\""), true
		}
		return value, true
	}

	return nil, false
}

func extractSingleTemplateExpression(value string) (string, bool) {
	re := regexp.MustCompile(`^\s*\{\{\s*(.+?)\s*\}\}\s*$`)
	match := re.FindStringSubmatch(value)
	if len(match) < 2 {
		return "", false
	}

	inner := trimSpace(match[1])
	if inner == "" {
		return "", false
	}

	return inner, true
}

func anySliceFromValue(value any) ([]any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, false
	case []any:
		return append([]any(nil), v...), true
	case []string:
		items := make([]any, len(v))
		for i, item := range v {
			items[i] = item
		}
		return items, true
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, false
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}

	items := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		items[i] = rv.Index(i).Interface()
	}
	return items, true
}

// matchesTags checks if task tags match execution tags.
func (e *Executor) matchesTags(taskTags []string) bool {
	// Tasks tagged "always" should run even when an explicit include filter is
	// set, unless the caller explicitly skips that tag.
	if slices.Contains(taskTags, "always") {
		for _, skip := range e.SkipTags {
			if skip == "always" {
				return false
			}
		}
		return true
	}

	// If no tags specified, run all
	if len(e.Tags) == 0 && len(e.SkipTags) == 0 {
		return true
	}

	// Check skip tags
	for _, skip := range e.SkipTags {
		if slices.Contains(taskTags, skip) {
			return false
		}
	}

	// Check include tags
	if len(e.Tags) > 0 {
		for _, tag := range e.Tags {
			if tag == "all" || slices.Contains(taskTags, tag) {
				return true
			}
		}
		return false
	}

	return true
}

// handleNotify marks handlers as notified.
func (e *Executor) handleNotify(notify any) {
	switch v := notify.(type) {
	case string:
		e.notified[v] = true
	case []any:
		for _, n := range v {
			if s, ok := n.(string); ok {
				e.notified[s] = true
			}
		}
	case []string:
		for _, s := range v {
			e.notified[s] = true
		}
	}
}

// runNotifiedHandlers executes any handlers that have been notified and then
// clears the notification state for those handlers.
func (e *Executor) runNotifiedHandlers(
	ctx context.Context, hosts []string, play *Play,
) error {
	if play == nil || len(play.Handlers) == 0 {
		return nil
	}

	pending := make(map[string]bool)
	for name, notified := range e.notified {
		if notified {
			pending[name] = true
			e.notified[name] = false
		}
	}

	if len(pending) == 0 {
		return nil
	}

	executed := make(map[string]bool)
	for _, handler := range play.Handlers {
		if handlerMatchesNotifications(&handler, pending) {
			if handler.Name != "" {
				if executed[handler.Name] {
					continue
				}
				executed[handler.Name] = true
			}
			if err := e.runTaskOnHosts(ctx, hosts, &handler, play); err != nil {
				return err
			}
		}
	}

	return nil
}

func handlerMatchesNotifications(handler *Task, pending map[string]bool) bool {
	if handler == nil || len(pending) == 0 {
		return false
	}

	if handler.Name != "" && pending[handler.Name] {
		return true
	}

	for _, listen := range normalizeStringList(handler.Listen) {
		if pending[listen] {
			return true
		}
	}

	return false
}

// handleMetaAction applies module meta side effects after the task result has
// been recorded and callbacks have fired.
func (e *Executor) handleMetaAction(
	ctx context.Context, host string, hosts []string, play *Play, result *TaskResult,
) error {
	if result == nil || result.Data == nil {
		return nil
	}

	action, _ := result.Data["action"].(string)
	switch action {
	case "flush_handlers":
		return e.runNotifiedHandlers(ctx, hosts, play)
	case "clear_facts":
		e.clearFacts(hosts)
		return nil
	case "clear_host_errors":
		e.clearHostErrors()
		return nil
	case "refresh_inventory":
		if r := e.refreshInventory(); !r.OK {
			return coreerr.E("Executor.handleMetaAction", "refresh inventory: "+resultErrorMessage(r), nil)
		}
		return nil
	case "end_play":
		return errEndPlay
	case "end_batch":
		return errEndBatch
	case "end_host":
		e.markHostEnded(host)
		return errEndHost
	case "reset_connection":
		e.resetConnection(host)
		return nil
	case "end_role":
		return errEndRole
	default:
		return nil
	}
}

// clearFacts removes cached facts for the given hosts.
func (e *Executor) clearFacts(hosts []string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, host := range hosts {
		delete(e.facts, host)
		delete(e.hostFacts, host)
	}
}

// clearHostErrors resets the current batch failure tracking so later tasks can
// proceed after a meta clear_host_errors action.
func (e *Executor) clearHostErrors() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.batchFailedHosts = make(map[string]bool)
}

// refreshInventory reloads inventory from the last configured source.
func (e *Executor) refreshInventory() core.Result {
	e.mu.RLock()
	path := e.inventoryPath
	e.mu.RUnlock()

	if path == "" {
		return core.Ok(nil)
	}

	invResult := e.parser.ParseInventory(path)
	if !invResult.OK {
		return core.Fail(coreerr.E("Executor.refreshInventory", "reload inventory: "+resultErrorMessage(invResult), nil))
	}
	inv := invResult.Value.(*Inventory)

	e.mu.Lock()
	e.inventory = inv
	e.clients = make(map[string]sshExecutorClient)
	e.mu.Unlock()
	return core.Ok(nil)
}

// markHostEnded records that a host should be skipped for the rest of the play.
func (e *Executor) markHostEnded(host string) {
	if host == "" {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.endedHosts == nil {
		e.endedHosts = make(map[string]bool)
	}
	e.endedHosts[host] = true
}

// isHostEnded reports whether a host has been retired for the current play.
func (e *Executor) isHostEnded(host string) bool {
	if host == "" {
		return false
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.endedHosts[host]
}

// filterActiveHosts removes hosts that have already been ended.
func (e *Executor) filterActiveHosts(hosts []string) []string {
	if len(hosts) == 0 {
		return hosts
	}

	filtered := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if !e.isHostEnded(host) {
			filtered = append(filtered, host)
		}
	}

	return filtered
}

// resetConnection closes and removes the cached SSH client for a host.
func (e *Executor) resetConnection(host string) {
	if host == "" {
		return
	}

	e.mu.Lock()
	client, ok := e.clients[host]
	if ok {
		delete(e.clients, host)
	}
	e.mu.Unlock()

	if ok {
		closeExecutorClient(client)
	}
}

// Close closes all SSH connections.
//
// Example:
//
//	executor.Close()
func (e *Executor) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, client := range e.clients {
		closeExecutorClient(client)
	}
	e.clients = make(map[string]sshExecutorClient)
}

func closeExecutorClient(client sshExecutorClient) {
	if client == nil {
		return
	}
	if r := client.Close(); !r.OK {
		return
	}
}

// TemplateFile processes a template file.
//
// Example:
//
//	content, err := executor.TemplateFile("/workspace/templates/app.conf.j2", "web1", &Task{})
func (e *Executor) TemplateFile(src, host string, task *Task) core.Result {
	src = e.resolveLocalPath(src)
	if src == "" {
		return core.Fail(coreerr.E("Executor.TemplateFile", "template source path required", nil))
	}

	content, err := coreio.Local.Read(src)
	if err != nil {
		return core.Fail(coreerr.E("Executor.TemplateFile", "read template "+src, err))
	}

	return core.Ok(e.templateString(content, host, task))
}
