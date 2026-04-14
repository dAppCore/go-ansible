package ansible

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

func newAsyncJobID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func (e *Executor) launchDetachedAsyncTask(ctx context.Context, host string, hosts []string, task *Task, play *Play) {
	if e == nil || task == nil {
		return
	}

	asyncCtx := ctx
	timeout := time.Duration(task.Async) * time.Second
	if timeout > 0 {
		var cancel context.CancelFunc
		asyncCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	clone := e.cloneAsyncExecutor()
	cloneTask := cloneTaskForAsync(task)
	cloneTask.Async = 0
	cloneTask.Poll = 0
	clonePlay := clonePlayForAsync(play)
	cloneHosts := append([]string(nil), hosts...)

	go func() {
		_ = clone.runTaskOnHost(asyncCtx, host, cloneHosts, &cloneTask, clonePlay)
	}()
}

func (e *Executor) cloneAsyncExecutor() *Executor {
	if e == nil {
		return nil
	}

	clone := &Executor{
		parser:             cloneParser(e.parser),
		inventory:          cloneInventory(e.inventory),
		inventoryPath:      e.inventoryPath,
		vars:               cloneAnyMap(e.vars),
		hostVars:           cloneHostVarsMap(e.hostVars),
		hostFacts:          cloneHostVarsMap(e.hostFacts),
		facts:              cloneFactsMap(e.facts),
		results:            cloneResultsMap(e.results),
		handlers:           cloneTaskHandlersMap(e.handlers),
		loadedRoleHandlers: cloneBoolMap(e.loadedRoleHandlers),
		notified:           cloneBoolMap(e.notified),
		clients:            cloneClientMap(e.clients),
		batchFailedHosts:   cloneBoolMap(e.batchFailedHosts),
		endedHosts:         cloneBoolMap(e.endedHosts),
		Limit:              e.Limit,
		Tags:               append([]string(nil), e.Tags...),
		SkipTags:           append([]string(nil), e.SkipTags...),
		CheckMode:          e.CheckMode,
		Diff:               e.Diff,
		Verbose:            e.Verbose,
	}

	return clone
}

func cloneClientMap(src map[string]sshExecutorClient) map[string]sshExecutorClient {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]sshExecutorClient, len(src))
	for key, client := range src {
		dst[key] = client
	}
	return dst
}

func cloneParser(parser *Parser) *Parser {
	if parser == nil {
		return nil
	}

	clone := &Parser{
		basePath: parser.basePath,
		medium:   parser.medium,
		vars:     cloneAnyMap(parser.vars),
	}
	return clone
}

func clonePlayForAsync(play *Play) *Play {
	if play == nil {
		return nil
	}

	clone := *play
	clone.Vars = cloneAnyMap(play.Vars)
	clone.ModuleDefaults = cloneModuleDefaults(play.ModuleDefaults)
	clone.PreTasks = cloneTaskSlice(play.PreTasks)
	clone.Tasks = cloneTaskSlice(play.Tasks)
	clone.PostTasks = cloneTaskSlice(play.PostTasks)
	clone.Roles = cloneRoleRefSlice(play.Roles)
	clone.Handlers = cloneTaskSlice(play.Handlers)
	clone.Tags = append([]string(nil), play.Tags...)
	clone.Environment = cloneStringMap(play.Environment)
	clone.Serial = cloneAnyValue(play.Serial)
	if play.GatherFacts != nil {
		value := *play.GatherFacts
		clone.GatherFacts = &value
	}
	if play.VarsFiles != nil {
		clone.VarsFiles = cloneAnyValue(play.VarsFiles)
	}

	return &clone
}

func cloneTaskForAsync(task *Task) Task {
	if task == nil {
		return Task{}
	}

	clone := *task
	clone.Args = cloneAnyMap(task.Args)
	clone.Vars = cloneAnyMap(task.Vars)
	clone.Environment = cloneStringMap(task.Environment)
	clone.Tags = append([]string(nil), task.Tags...)
	clone.Block = cloneTaskSlice(task.Block)
	clone.Rescue = cloneTaskSlice(task.Rescue)
	clone.Always = cloneTaskSlice(task.Always)
	clone.Loop = cloneAnyValue(task.Loop)
	clone.Notify = cloneAnyValue(task.Notify)
	clone.Listen = cloneAnyValue(task.Listen)
	clone.IncludeRole = cloneRoleRefPtr(task.IncludeRole)
	clone.ImportRole = cloneRoleRefPtr(task.ImportRole)
	clone.Apply = cloneTaskApplyPtr(task.Apply)
	clone.LoopControl = cloneLoopControlPtr(task.LoopControl)
	return clone
}

func cloneTaskSlice(tasks []Task) []Task {
	if len(tasks) == 0 {
		return nil
	}

	clone := make([]Task, len(tasks))
	for i := range tasks {
		clone[i] = cloneTaskForAsync(&tasks[i])
	}
	return clone
}

func cloneRoleRefSlice(roles []RoleRef) []RoleRef {
	if len(roles) == 0 {
		return nil
	}

	clone := make([]RoleRef, len(roles))
	for i := range roles {
		clone[i] = cloneRoleRef(roles[i])
	}
	return clone
}

func cloneRoleRefPtr(role *RoleRef) *RoleRef {
	if role == nil {
		return nil
	}
	clone := cloneRoleRef(*role)
	return &clone
}

func cloneRoleRef(role RoleRef) RoleRef {
	clone := role
	clone.Vars = cloneAnyMap(role.Vars)
	clone.Tags = append([]string(nil), role.Tags...)
	clone.Apply = cloneTaskApplyPtr(role.Apply)
	clone.When = cloneAnyValue(role.When)
	return clone
}

func cloneTaskApplyPtr(apply *TaskApply) *TaskApply {
	if apply == nil {
		return nil
	}
	clone := *apply
	clone.Tags = append([]string(nil), apply.Tags...)
	clone.Vars = cloneAnyMap(apply.Vars)
	clone.Environment = cloneStringMap(apply.Environment)
	clone.When = cloneAnyValue(apply.When)
	return &clone
}

func cloneLoopControlPtr(control *LoopControl) *LoopControl {
	if control == nil {
		return nil
	}
	clone := *control
	return &clone
}

func cloneModuleDefaults(src map[string]map[string]any) map[string]map[string]any {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]map[string]any, len(src))
	for key, value := range src {
		dst[key] = cloneAnyMap(value)
	}
	return dst
}

func cloneInventory(inv *Inventory) *Inventory {
	if inv == nil {
		return nil
	}

	clone := &Inventory{
		All:      cloneInventoryGroup(inv.All),
		HostVars: cloneHostVarsMap(inv.HostVars),
	}
	return clone
}

func cloneInventoryGroup(group *InventoryGroup) *InventoryGroup {
	if group == nil {
		return nil
	}

	clone := &InventoryGroup{
		Vars: cloneAnyMap(group.Vars),
	}
	if len(group.Hosts) > 0 {
		clone.Hosts = make(map[string]*Host, len(group.Hosts))
		for name, host := range group.Hosts {
			clone.Hosts[name] = cloneHost(host)
		}
	}
	if len(group.Children) > 0 {
		clone.Children = make(map[string]*InventoryGroup, len(group.Children))
		for name, child := range group.Children {
			clone.Children[name] = cloneInventoryGroup(child)
		}
	}
	return clone
}

func cloneHost(host *Host) *Host {
	if host == nil {
		return nil
	}

	clone := *host
	clone.Vars = cloneAnyMap(host.Vars)
	return &clone
}

func cloneHostVarsMap(src map[string]map[string]any) map[string]map[string]any {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]map[string]any, len(src))
	for key, value := range src {
		dst[key] = cloneAnyMap(value)
	}
	return dst
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = cloneAnyValue(value)
	}
	return dst
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneFactsMap(src map[string]*Facts) map[string]*Facts {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]*Facts, len(src))
	for key, value := range src {
		if value == nil {
			dst[key] = nil
			continue
		}
		clone := *value
		dst[key] = &clone
	}
	return dst
}

func cloneResultsMap(src map[string]map[string]*TaskResult) map[string]map[string]*TaskResult {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]map[string]*TaskResult, len(src))
	for host, hostResults := range src {
		if len(hostResults) == 0 {
			continue
		}
		clonedHostResults := make(map[string]*TaskResult, len(hostResults))
		for name, result := range hostResults {
			clonedHostResults[name] = cloneTaskResult(result)
		}
		dst[host] = clonedHostResults
	}
	return dst
}

func cloneTaskHandlersMap(src map[string][]Task) map[string][]Task {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string][]Task, len(src))
	for name, tasks := range src {
		dst[name] = cloneTaskSlice(tasks)
	}
	return dst
}

func cloneTaskResult(result *TaskResult) *TaskResult {
	if result == nil {
		return nil
	}

	clone := *result
	if len(result.Results) > 0 {
		clone.Results = make([]TaskResult, len(result.Results))
		for i := range result.Results {
			cloned := cloneTaskResult(&result.Results[i])
			if cloned != nil {
				clone.Results[i] = *cloned
			}
		}
	}
	if len(result.Data) > 0 {
		clone.Data = cloneAnyMap(result.Data)
	}
	return &clone
}

func cloneAnyValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return value
	case map[string]any:
		return cloneAnyMap(v)
	case map[any]any:
		dst := make(map[any]any, len(v))
		for key, item := range v {
			dst[key] = cloneAnyValue(item)
		}
		return dst
	case []any:
		dst := make([]any, len(v))
		for i, item := range v {
			dst[i] = cloneAnyValue(item)
		}
		return dst
	case []string:
		return append([]string(nil), v...)
	case []Task:
		return cloneTaskSlice(v)
	case []RoleRef:
		return cloneRoleRefSlice(v)
	default:
		return value
	}
}
