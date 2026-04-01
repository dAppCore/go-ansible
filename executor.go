package ansible

import (
	"context"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	coreio "dappco.re/go/core/io"
	coreerr "dappco.re/go/core/log"
)

// Executor runs Ansible playbooks.
//
// Example:
//
//	exec := NewExecutor("/workspace/playbooks")
type Executor struct {
	parser    *Parser
	inventory *Inventory
	vars      map[string]any
	facts     map[string]*Facts
	results   map[string]map[string]*TaskResult // host -> register_name -> result
	handlers  map[string][]Task
	notified  map[string]bool
	clients   map[string]*SSHClient
	mu        sync.RWMutex

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
//	exec := NewExecutor("/workspace/playbooks")
func NewExecutor(basePath string) *Executor {
	return &Executor{
		parser:   NewParser(basePath),
		vars:     make(map[string]any),
		facts:    make(map[string]*Facts),
		results:  make(map[string]map[string]*TaskResult),
		handlers: make(map[string][]Task),
		notified: make(map[string]bool),
		clients:  make(map[string]*SSHClient),
	}
}

// SetInventory loads inventory from a file.
//
// Example:
//
//	err := exec.SetInventory("/workspace/inventory.yml")
func (e *Executor) SetInventory(path string) error {
	inv, err := e.parser.ParseInventory(path)
	if err != nil {
		return err
	}
	e.inventory = inv
	return nil
}

// SetInventoryDirect sets inventory directly.
//
// Example:
//
//	exec.SetInventoryDirect(&Inventory{All: &InventoryGroup{}})
func (e *Executor) SetInventoryDirect(inv *Inventory) {
	e.inventory = inv
}

// SetVar sets a variable.
//
// Example:
//
//	exec.SetVar("env", "prod")
func (e *Executor) SetVar(key string, value any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.vars[key] = value
}

// Run executes a playbook.
//
// Example:
//
//	err := exec.Run(context.Background(), "/workspace/playbooks/site.yml")
func (e *Executor) Run(ctx context.Context, playbookPath string) error {
	plays, err := e.parser.ParsePlaybook(playbookPath)
	if err != nil {
		return coreerr.E("Executor.Run", "parse playbook", err)
	}

	for i := range plays {
		if err := e.runPlay(ctx, &plays[i]); err != nil {
			return coreerr.E("Executor.Run", sprintf("play %d (%s)", i, plays[i].Name), err)
		}
	}

	return nil
}

// runPlay executes a single play.
func (e *Executor) runPlay(ctx context.Context, play *Play) error {
	if e.OnPlayStart != nil {
		e.OnPlayStart(play)
	}
	defer func() {
		if e.OnPlayEnd != nil {
			e.OnPlayEnd(play)
		}
	}()

	// Get target hosts
	hosts := e.getHosts(play.Hosts)
	if len(hosts) == 0 {
		return nil // No hosts matched
	}

	// Merge play vars
	for k, v := range play.Vars {
		e.vars[k] = v
	}

	for _, batch := range splitSerialHosts(hosts, play.Serial) {
		if len(batch) == 0 {
			continue
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
			if err := e.runTaskOnHosts(ctx, batch, &task, play); err != nil {
				return err
			}
		}

		// Execute roles
		for _, roleRef := range play.Roles {
			if err := e.runRole(ctx, batch, &roleRef, play); err != nil {
				return err
			}
		}

		// Execute tasks
		for _, task := range play.Tasks {
			if err := e.runTaskOnHosts(ctx, batch, &task, play); err != nil {
				return err
			}
		}

		// Execute post_tasks
		for _, task := range play.PostTasks {
			if err := e.runTaskOnHosts(ctx, batch, &task, play); err != nil {
				return err
			}
		}

		// Run notified handlers for this batch.
		if err := e.runNotifiedHandlers(ctx, batch, play); err != nil {
			return err
		}
	}

	return nil
}

// splitSerialHosts splits a host list into serial batches.
func splitSerialHosts(hosts []string, serial any) [][]string {
	batchSize := resolveSerialBatchSize(serial, len(hosts))
	if batchSize <= 0 || batchSize >= len(hosts) {
		if len(hosts) == 0 {
			return nil
		}
		return [][]string{hosts}
	}

	batches := make([][]string, 0, (len(hosts)+batchSize-1)/batchSize)
	for len(hosts) > 0 {
		size := batchSize
		if size > len(hosts) {
			size = len(hosts)
		}
		batch := append([]string(nil), hosts[:size]...)
		batches = append(batches, batch)
		hosts = hosts[size:]
	}
	return batches
}

// resolveSerialBatchSize converts a play serial value into a concrete batch size.
func resolveSerialBatchSize(serial any, total int) int {
	if total <= 0 {
		return 0
	}

	switch v := serial.(type) {
	case nil:
		return total
	case int:
		if v > 0 {
			return v
		}
	case int8:
		if v > 0 {
			return int(v)
		}
	case int16:
		if v > 0 {
			return int(v)
		}
	case int32:
		if v > 0 {
			return int(v)
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case uint:
		if v > 0 {
			return int(v)
		}
	case uint8:
		if v > 0 {
			return int(v)
		}
	case uint16:
		if v > 0 {
			return int(v)
		}
	case uint32:
		if v > 0 {
			return int(v)
		}
	case uint64:
		if v > 0 {
			return int(v)
		}
	case string:
		s := corexTrimSpace(v)
		if s == "" {
			return total
		}
		if corexHasSuffix(s, "%") {
			percent, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
			if err == nil && percent > 0 {
				size := (total*percent + 99) / 100
				if size < 1 {
					size = 1
				}
				if size > total {
					size = total
				}
				return size
			}
			return total
		}
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}

	return total
}

// runRole executes a role on hosts.
func (e *Executor) runRole(ctx context.Context, hosts []string, roleRef *RoleRef, play *Play) error {
	// Check when condition
	if roleRef.When != nil {
		if !e.evaluateWhen(roleRef.When, "", nil) {
			return nil
		}
	}

	// Parse role tasks
	tasks, err := e.parser.ParseRole(roleRef.Role, roleRef.TasksFrom)
	if err != nil {
		return coreerr.E("executor.runRole", sprintf("parse role %s", roleRef.Role), err)
	}

	// Merge role vars
	oldVars := make(map[string]any)
	for k, v := range e.vars {
		oldVars[k] = v
	}
	for k, v := range roleRef.Vars {
		e.vars[k] = v
	}

	// Execute tasks
	for _, task := range tasks {
		if err := e.runTaskOnHosts(ctx, hosts, &task, play); err != nil {
			// Restore vars
			e.vars = oldVars
			return err
		}
	}

	// Restore vars
	e.vars = oldVars
	return nil
}

// runTaskOnHosts runs a task on all hosts.
func (e *Executor) runTaskOnHosts(ctx context.Context, hosts []string, task *Task, play *Play) error {
	// Check tags
	if !e.matchesTags(task.Tags) {
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
		if err := e.runTaskOnHost(ctx, host, hosts, task, play); err != nil {
			if !task.IgnoreErrors {
				return err
			}
		}
	}

	return nil
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
func (e *Executor) runTaskOnHost(ctx context.Context, host string, hosts []string, task *Task, play *Play) error {
	start := time.Now()

	if e.OnTaskStart != nil {
		e.OnTaskStart(host, task)
	}

	// Initialize host results
	if e.results[host] == nil {
		e.results[host] = make(map[string]*TaskResult)
	}

	// Check when condition
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
	client, err := e.getClient(host, play)
	if err != nil {
		return coreerr.E("Executor.runTaskOnHost", sprintf("get client for %s", host), err)
	}

	// Handle loops
	if task.Loop != nil {
		return e.runLoop(ctx, host, client, task, play)
	}

	// Execute the task, honouring retries/until when configured.
	result, err := e.runTaskWithRetries(ctx, host, task, play, func() (*TaskResult, error) {
		return e.executeModule(ctx, host, client, task, play)
	})
	if err != nil {
		result = &TaskResult{Failed: true, Msg: err.Error()}
	}
	result.Duration = time.Since(start)

	// Store result
	if task.Register != "" {
		e.results[host][task.Register] = result
	}

	// Handle notify
	if result.Changed && task.Notify != nil {
		e.handleNotify(task.Notify)
	}

	if e.OnTaskEnd != nil {
		e.OnTaskEnd(host, task, result)
	}

	if NormalizeModule(task.Module) == "ansible.builtin.meta" {
		if err := e.handleMetaAction(ctx, hosts, play, result); err != nil {
			return err
		}
	}

	if result.Failed && !task.IgnoreErrors {
		return coreerr.E("Executor.runTaskOnHost", "task failed: "+result.Msg, nil)
	}

	return nil
}

// runTaskWithRetries executes a task once or multiple times when retries,
// delay, or until are configured.
func (e *Executor) runTaskWithRetries(ctx context.Context, host string, task *Task, play *Play, execute func() (*TaskResult, error)) (*TaskResult, error) {
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
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		result, err = execute()
		if err != nil {
			result = &TaskResult{Failed: true, Msg: err.Error()}
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
				return result, ctx.Err()
			case <-timer.C:
			}
		}
	}

	return result, nil
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
func (e *Executor) runLoop(ctx context.Context, host string, client *SSHClient, task *Task, play *Play) error {
	items := e.resolveLoop(task.Loop, host)

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

	var results []TaskResult
	for i, item := range items {
		// Set loop variables
		e.vars[loopVar] = item
		if indexVar != "" {
			e.vars[indexVar] = i
		}

		result, err := e.runTaskWithRetries(ctx, host, task, play, func() (*TaskResult, error) {
			return e.executeModule(ctx, host, client, task, play)
		})
		if err != nil {
			result = &TaskResult{Failed: true, Msg: err.Error()}
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

	// Store combined result
	if task.Register != "" {
		combined := &TaskResult{
			Results: results,
			Changed: false,
		}
		for _, r := range results {
			if r.Changed {
				combined.Changed = true
			}
			if r.Failed {
				combined.Failed = true
			}
		}
		e.results[host][task.Register] = combined
	}

	return nil
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
func (e *Executor) runBlock(ctx context.Context, hosts []string, task *Task, play *Play) error {
	var blockErr error

	// Try block
	for _, t := range task.Block {
		if err := e.runTaskOnHosts(ctx, hosts, &t, play); err != nil {
			blockErr = err
			break
		}
	}

	// Run rescue if block failed
	if blockErr != nil && len(task.Rescue) > 0 {
		for _, t := range task.Rescue {
			if err := e.runTaskOnHosts(ctx, hosts, &t, play); err != nil {
				// Rescue also failed
				break
			}
		}
	}

	// Always run always block
	for _, t := range task.Always {
		if err := e.runTaskOnHosts(ctx, hosts, &t, play); err != nil {
			if blockErr == nil {
				blockErr = err
			}
		}
	}

	if blockErr != nil && len(task.Rescue) == 0 {
		return blockErr
	}

	return nil
}

// runIncludeTasks handles include_tasks/import_tasks.
func (e *Executor) runIncludeTasks(ctx context.Context, hosts []string, task *Task, play *Play) error {
	path := task.IncludeTasks
	if path == "" {
		path = task.ImportTasks
	}

	// Resolve path relative to playbook
	path = e.templateString(path, "", nil)

	tasks, err := e.parser.ParseTasks(path)
	if err != nil {
		return coreerr.E("Executor.runIncludeTasks", "include_tasks "+path, err)
	}

	for _, t := range tasks {
		if err := e.runTaskOnHosts(ctx, hosts, &t, play); err != nil {
			return err
		}
	}

	return nil
}

// runIncludeRole handles include_role/import_role.
func (e *Executor) runIncludeRole(ctx context.Context, hosts []string, task *Task, play *Play) error {
	var roleName, tasksFrom string
	var roleVars map[string]any

	if task.IncludeRole != nil {
		roleName = task.IncludeRole.Name
		tasksFrom = task.IncludeRole.TasksFrom
		roleVars = task.IncludeRole.Vars
	} else {
		roleName = task.ImportRole.Name
		tasksFrom = task.ImportRole.TasksFrom
		roleVars = task.ImportRole.Vars
	}

	roleRef := &RoleRef{
		Role:      roleName,
		TasksFrom: tasksFrom,
		Vars:      roleVars,
	}

	return e.runRole(ctx, hosts, roleRef, play)
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
			if limitSet[h] || h == e.Limit || contains(h, e.Limit) {
				filtered = append(filtered, h)
			}
		}
		hosts = filtered
	}

	return hosts
}

// getClient returns or creates an SSH client for a host.
func (e *Executor) getClient(host string, play *Play) (*SSHClient, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if client, ok := e.clients[host]; ok {
		return client, nil
	}

	// Get host vars
	vars := make(map[string]any)
	if e.inventory != nil {
		vars = GetHostVars(e.inventory, host)
	}

	// Merge with play vars
	for k, v := range e.vars {
		if _, exists := vars[k]; !exists {
			vars[k] = v
		}
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

	// Apply play become settings
	if play.Become {
		cfg.Become = true
		cfg.BecomeUser = play.BecomeUser
		if bp, ok := vars["ansible_become_password"].(string); ok {
			cfg.BecomePass = bp
		} else if cfg.Password != "" {
			// Use SSH password for sudo if no become password specified
			cfg.BecomePass = cfg.Password
		}
	}

	client, err := NewSSHClient(cfg)
	if err != nil {
		return nil, err
	}

	e.clients[host] = client
	return client, nil
}

// gatherFacts collects facts from a host.
func (e *Executor) gatherFacts(ctx context.Context, host string, play *Play) error {
	if play.Connection == "local" || host == "localhost" {
		// Local facts
		e.facts[host] = &Facts{
			Hostname: "localhost",
		}
		return nil
	}

	client, err := e.getClient(host, play)
	if err != nil {
		return err
	}

	facts, err := e.collectFacts(ctx, client)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.facts[host] = facts
	e.mu.Unlock()

	return nil
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

	// Handle negation
	if corexHasPrefix(cond, "not ") {
		return !e.evalConditionWithLocals(corexTrimPrefix(cond, "not "), host, task, locals)
	}

	// Handle equality/inequality
	if contains(cond, "==") {
		parts := splitN(cond, "==", 2)
		if len(parts) == 2 {
			left, leftOK := e.resolveConditionOperand(parts[0], host, task, locals)
			right, rightOK := e.resolveConditionOperand(parts[1], host, task, locals)
			if !leftOK || !rightOK {
				return true
			}
			return left == right
		}
	}
	if contains(cond, "!=") {
		parts := splitN(cond, "!=", 2)
		if len(parts) == 2 {
			left, leftOK := e.resolveConditionOperand(parts[0], host, task, locals)
			right, rightOK := e.resolveConditionOperand(parts[1], host, task, locals)
			if !leftOK || !rightOK {
				return true
			}
			return left != right
		}
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

	if val, ok := e.vars[name]; ok {
		return val, true
	}

	if task != nil {
		if val, ok := task.Vars[name]; ok {
			return val, true
		}
	}

	if e.inventory != nil {
		hostVars := GetHostVars(e.inventory, host)
		if val, ok := hostVars[name]; ok {
			return val, true
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
	expr = corexTrimSpace(expr)

	if expr == "true" || expr == "True" || expr == "false" || expr == "False" {
		return expr, true
	}
	if len(expr) > 0 && expr[0] >= '0' && expr[0] <= '9' {
		return expr, true
	}
	if (len(expr) >= 2 && expr[0] == '\'' && expr[len(expr)-1] == '\'') || (len(expr) >= 2 && expr[0] == '"' && expr[len(expr)-1] == '"') {
		return expr[1 : len(expr)-1], true
	}

	if value, ok := e.lookupConditionValue(expr, host, task, locals); ok {
		return sprintf("%v", value), true
	}

	return expr, false
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
	// Handle filters
	if contains(expr, " | ") {
		parts := splitN(expr, " | ", 2)
		value := e.resolveExpr(parts[0], host, task)
		return e.applyFilter(value, parts[1])
	}

	// Handle lookups
	if corexHasPrefix(expr, "lookup(") {
		return e.handleLookup(expr)
	}

	// Handle registered vars
	if contains(expr, ".") {
		parts := splitN(expr, ".", 2)
		if result := e.getRegisteredVar(host, parts[0]); result != nil {
			switch parts[1] {
			case "stdout":
				return result.Stdout
			case "stderr":
				return result.Stderr
			case "rc":
				return sprintf("%d", result.RC)
			case "changed":
				return sprintf("%t", result.Changed)
			case "failed":
				return sprintf("%t", result.Failed)
			}
		}
	}

	// Check vars
	if val, ok := e.vars[expr]; ok {
		return sprintf("%v", val)
	}

	// Check task vars
	if task != nil {
		if val, ok := task.Vars[expr]; ok {
			return sprintf("%v", val)
		}
	}

	// Check host vars
	if e.inventory != nil {
		hostVars := GetHostVars(e.inventory, host)
		if val, ok := hostVars[expr]; ok {
			return sprintf("%v", val)
		}
	}

	// Check facts
	if facts, ok := e.facts[host]; ok {
		switch expr {
		case "ansible_hostname":
			return facts.Hostname
		case "ansible_fqdn":
			return facts.FQDN
		case "ansible_os_family":
			return facts.OS
		case "ansible_memtotal_mb":
			return sprintf("%d", facts.Memory)
		case "ansible_processor_vcpus":
			return sprintf("%d", facts.CPUs)
		case "ansible_default_ipv4_address":
			return facts.IPv4
		case "ansible_distribution":
			return facts.Distribution
		case "ansible_distribution_version":
			return facts.Version
		case "ansible_architecture":
			return facts.Architecture
		case "ansible_kernel":
			return facts.Kernel
		}
	}

	return "{{ " + expr + " }}" // Return as-is if unresolved
}

// applyFilter applies a Jinja2 filter.
func (e *Executor) applyFilter(value, filter string) string {
	filter = corexTrimSpace(filter)

	// Handle default filter
	if corexHasPrefix(filter, "default(") {
		if value == "" || value == "{{ "+filter+" }}" {
			// Extract default value
			re := regexp.MustCompile(`default\(([^)]*)\)`)
			if match := re.FindStringSubmatch(filter); len(match) > 1 {
				return trimCutset(match[1], "'\"")
			}
		}
		return value
	}

	// Handle bool filter
	if filter == "bool" {
		lowered := lower(value)
		if lowered == "true" || lowered == "yes" || lowered == "1" {
			return "true"
		}
		return "false"
	}

	// Handle trim
	if filter == "trim" {
		return corexTrimSpace(value)
	}

	// Handle b64decode
	if filter == "b64decode" {
		// Would need base64 decode
		return value
	}

	return value
}

// handleLookup handles lookup() expressions.
func (e *Executor) handleLookup(expr string) string {
	// Parse lookup('type', 'arg')
	re := regexp.MustCompile(`lookup\s*\(\s*['"](\w+)['"]\s*,\s*['"]([^'"]+)['"]\s*`)
	match := re.FindStringSubmatch(expr)
	if len(match) < 3 {
		return ""
	}

	lookupType := match[1]
	arg := match[2]

	switch lookupType {
	case "env":
		return env(arg)
	case "file":
		if data, err := coreio.Local.Read(arg); err == nil {
			return data
		}
	}

	return ""
}

// resolveLoop resolves loop items.
func (e *Executor) resolveLoop(loop any, host string) []any {
	switch v := loop.(type) {
	case []any:
		return v
	case []string:
		items := make([]any, len(v))
		for i, s := range v {
			items[i] = s
		}
		return items
	case string:
		// Template the string and see if it's a var reference
		resolved := e.templateString(v, host, nil)
		if val, ok := e.vars[resolved]; ok {
			if items, ok := val.([]any); ok {
				return items
			}
		}
	}
	return nil
}

// matchesTags checks if task tags match execution tags.
func (e *Executor) matchesTags(taskTags []string) bool {
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
func (e *Executor) runNotifiedHandlers(ctx context.Context, hosts []string, play *Play) error {
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

	for _, handler := range play.Handlers {
		if pending[handler.Name] {
			if err := e.runTaskOnHosts(ctx, hosts, &handler, play); err != nil {
				return err
			}
		}
	}

	return nil
}

// handleMetaAction applies module meta side effects after the task result has
// been recorded and callbacks have fired.
func (e *Executor) handleMetaAction(ctx context.Context, hosts []string, play *Play, result *TaskResult) error {
	if result == nil || result.Data == nil {
		return nil
	}

	action, _ := result.Data["action"].(string)
	switch action {
	case "flush_handlers":
		return e.runNotifiedHandlers(ctx, hosts, play)
	default:
		return nil
	}
}

// Close closes all SSH connections.
//
// Example:
//
//	exec.Close()
func (e *Executor) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, client := range e.clients {
		_ = client.Close()
	}
	e.clients = make(map[string]*SSHClient)
}

// TemplateFile processes a template file.
//
// Example:
//
//	content, err := exec.TemplateFile("/workspace/templates/app.conf.j2", "web1", &Task{})
func (e *Executor) TemplateFile(src, host string, task *Task) (string, error) {
	content, err := coreio.Local.Read(src)
	if err != nil {
		return "", err
	}

	// Convert Jinja2 to Go template syntax (basic conversion)
	tmplContent := content
	tmplContent = replaceAll(tmplContent, "{{", "{{ .")
	tmplContent = replaceAll(tmplContent, "{%", "{{")
	tmplContent = replaceAll(tmplContent, "%}", "}}")

	tmpl, err := template.New("template").Parse(tmplContent)
	if err != nil {
		// Fall back to simple replacement
		return e.templateString(content, host, task), nil
	}

	// Build context map
	context := make(map[string]any)
	for k, v := range e.vars {
		context[k] = v
	}
	// Add host vars
	if e.inventory != nil {
		hostVars := GetHostVars(e.inventory, host)
		for k, v := range hostVars {
			context[k] = v
		}
	}
	// Add facts
	if facts, ok := e.facts[host]; ok {
		context["ansible_hostname"] = facts.Hostname
		context["ansible_fqdn"] = facts.FQDN
		context["ansible_os_family"] = facts.OS
		context["ansible_memtotal_mb"] = facts.Memory
		context["ansible_processor_vcpus"] = facts.CPUs
		context["ansible_default_ipv4_address"] = facts.IPv4
		context["ansible_distribution"] = facts.Distribution
		context["ansible_distribution_version"] = facts.Version
		context["ansible_architecture"] = facts.Architecture
		context["ansible_kernel"] = facts.Kernel
	}

	buf := newBuilder()
	if err := tmpl.Execute(buf, context); err != nil {
		return e.templateString(content, host, task), nil
	}

	return buf.String(), nil
}
