package ansible

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"io/fs"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"time"

	core "dappco.re/go"
	coreio "dappco.re/go/io"
	coreerr "dappco.re/go/log"
	"gopkg.in/yaml.v3"
)

type sshFactsRunner interface {
	Run(ctx context.Context, cmd string) core.Result
}

type commandRunner interface {
	Run(ctx context.Context, cmd string) core.Result
}

// executeModule dispatches to the appropriate module handler.
func (e *Executor) executeModule(ctx context.Context, host string, client sshExecutorClient, task *Task, play *Play) core.Result {
	originalModule := task.Module
	module := NormalizeModule(originalModule)
	executionHost := e.resolveDelegateHost(host, task)
	factsHost := host
	if task.DelegateFacts && task.Delegate != "" {
		factsHost = executionHost
	}

	// Apply task-level become overrides, including an explicit disable.
	if task.Become != nil {
		// Save old state to restore after the task finishes.
		oldBecome, oldUser, oldPass := client.BecomeState()

		if *task.Become {
			becomePass := oldPass
			if becomePass == "" {
				becomePass = e.resolveBecomePassword(host)
			}
			client.SetBecome(true, task.BecomeUser, becomePass)
		} else {
			client.SetBecome(false, "", "")
		}

		defer client.SetBecome(oldBecome, oldUser, oldPass)
	}

	if prefix := e.buildEnvironmentPrefix(host, task, play); prefix != "" {
		client = &environmentSSHClient{
			sshExecutorClient: client,
			prefix:            prefix,
		}
	}

	// Merge play-level module defaults before templating so defaults and task
	// arguments can both resolve host-scoped variables.
	args := mergeModuleDefaults(task.Args, e.resolveModuleDefaults(play, module))
	args = e.templateArgs(args, host, task)

	switch module {
	// Command execution
	case "ansible.builtin.shell":
		return e.moduleShell(ctx, client, args)
	case "ansible.builtin.command":
		return e.moduleCommand(ctx, client, args)
	case "ansible.builtin.raw":
		return e.moduleRaw(ctx, client, args)
	case "ansible.builtin.script":
		return e.moduleScript(ctx, client, args)

	// File operations
	case "ansible.builtin.copy":
		return e.moduleCopy(ctx, client, args, host, task)
	case "ansible.builtin.template":
		return e.moduleTemplate(ctx, client, args, host, task)
	case "ansible.builtin.file":
		return e.moduleFile(ctx, client, args)
	case "ansible.builtin.lineinfile":
		return e.moduleLineinfile(ctx, client, args)
	case "ansible.builtin.replace":
		return e.moduleReplace(ctx, client, args)
	case "ansible.builtin.stat":
		return e.moduleStat(ctx, client, args)
	case "ansible.builtin.slurp":
		return e.moduleSlurp(ctx, client, args)
	case "ansible.builtin.fetch":
		return e.moduleFetch(ctx, client, args)
	case "ansible.builtin.get_url":
		return e.moduleGetURL(ctx, client, args)

	// Package management
	case "ansible.builtin.apt":
		return e.moduleApt(ctx, client, args)
	case "ansible.builtin.apt_key":
		return e.moduleAptKey(ctx, client, args)
	case "ansible.builtin.apt_repository":
		return e.moduleAptRepository(ctx, client, args)
	case "ansible.builtin.yum":
		return e.moduleYum(ctx, client, args)
	case "ansible.builtin.dnf":
		return e.moduleDnf(ctx, client, args)
	case "ansible.builtin.rpm":
		return e.moduleRPM(ctx, client, args, "rpm")
	case "ansible.builtin.package":
		return e.modulePackage(ctx, client, args)
	case "ansible.builtin.pip":
		return e.modulePip(ctx, client, args)

	// Service management
	case "ansible.builtin.service":
		return e.moduleService(ctx, client, args)
	case "ansible.builtin.systemd":
		return e.moduleSystemd(ctx, client, args)

	// User/Group
	case "ansible.builtin.user":
		return e.moduleUser(ctx, client, args)
	case "ansible.builtin.group":
		return e.moduleGroup(ctx, client, args)

	// HTTP
	case "ansible.builtin.uri":
		return e.moduleURI(ctx, client, args)

	// Misc
	case "ansible.builtin.debug":
		return e.moduleDebug(host, task, args)
	case "ansible.builtin.fail":
		return e.moduleFail(args)
	case "ansible.builtin.assert":
		return e.moduleAssert(args, host)
	case "ansible.builtin.ping":
		return e.modulePing(ctx, client, args)
	case "ansible.builtin.set_fact":
		return e.moduleSetFact(factsHost, args)
	case "ansible.builtin.add_host":
		return e.moduleAddHost(args)
	case "ansible.builtin.group_by":
		return e.moduleGroupBy(host, args)
	case "ansible.builtin.pause":
		return e.modulePause(ctx, args)
	case "ansible.builtin.wait_for":
		return e.moduleWaitFor(ctx, client, args)
	case "ansible.builtin.wait_for_connection":
		return e.moduleWaitForConnection(ctx, client, args)
	case "ansible.builtin.git":
		return e.moduleGit(ctx, client, args)
	case "ansible.builtin.unarchive":
		return e.moduleUnarchive(ctx, client, args)
	case "ansible.builtin.archive":
		return e.moduleArchive(ctx, client, args)

	// Additional modules
	case "ansible.builtin.hostname":
		return e.moduleHostname(ctx, client, args)
	case "ansible.builtin.sysctl":
		return e.moduleSysctl(ctx, client, args)
	case "ansible.builtin.cron":
		return e.moduleCron(ctx, client, args)
	case "ansible.builtin.blockinfile":
		return e.moduleBlockinfile(ctx, client, args)
	case "ansible.builtin.include_vars":
		return e.moduleIncludeVars(args)
	case "ansible.builtin.meta":
		return e.moduleMeta(args)
	case "ansible.builtin.setup":
		return e.moduleSetup(ctx, factsHost, client, args)
	case "ansible.builtin.reboot":
		return e.moduleReboot(ctx, client, args)

	// Community modules (basic support)
	case "community.general.ufw":
		return e.moduleUFW(ctx, client, args)
	case "ansible.posix.authorized_key":
		return e.moduleAuthorizedKey(ctx, client, args)
	case "community.docker.docker_compose":
		return e.moduleDockerCompose(ctx, client, args)
	case "community.docker.docker_compose_v2":
		return e.moduleDockerCompose(ctx, client, args)

	default:
		// For unknown modules, try to execute as shell if it looks like a command
		if contains(task.Module, " ") || task.Module == "" {
			return e.moduleShell(ctx, client, args)
		}
		if originalModule != "" && originalModule != module {
			return core.Fail(coreerr.E("Executor.executeModule", "unsupported module: "+originalModule+" (resolved to "+module+")", nil))
		}
		return core.Fail(coreerr.E("Executor.executeModule", "unsupported module: "+module, nil))
	}
}

func (e *Executor) resolveModuleDefaults(play *Play, module string) map[string]any {
	if play == nil || len(play.ModuleDefaults) == 0 || module == "" {
		return nil
	}

	canonical := NormalizeModule(module)

	merged := make(map[string]any)
	seen := false
	keys := make([]string, 0, len(play.ModuleDefaults))
	for key := range play.ModuleDefaults {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if NormalizeModule(key) != canonical {
			continue
		}
		defaults := play.ModuleDefaults[key]
		if len(defaults) == 0 {
			continue
		}
		for k, v := range defaults {
			merged[k] = v
		}
		seen = true
	}

	if !seen {
		return nil
	}
	return merged
}

func mergeModuleDefaults(args, defaults map[string]any) map[string]any {
	if len(args) == 0 && len(defaults) == 0 {
		return nil
	}

	merged := make(map[string]any, len(args)+len(defaults))
	for k, v := range defaults {
		merged[k] = v
	}
	for k, v := range args {
		merged[k] = v
	}
	return merged
}

func (e *Executor) resolveBecomePassword(host string) string {
	if e == nil {
		return ""
	}

	if v, ok := e.vars["ansible_become_password"].(string); ok && v != "" {
		return v
	}

	if e.inventory != nil {
		if hostVars := GetHostVars(e.inventory, host); len(hostVars) > 0 {
			if v, ok := hostVars["ansible_become_password"].(string); ok && v != "" {
				return v
			}
		}
	}

	return ""
}

func remoteFileText(ctx context.Context, client sshExecutorClient, path string) (string, bool) {
	dataResult := client.Download(ctx, path)
	if !dataResult.OK {
		return "", false
	}
	data := dataResult.Value.([]byte)
	return string(data), true
}

func runBestEffort(ctx context.Context, client sshExecutorClient, cmd string) {
	if result := client.Run(ctx, cmd); !result.OK {
		return
	}
}

func fileDiffData(path, before, after string) map[string]any {
	return map[string]any{
		pathArgKey: path,
		"before":   before,
		"after":    after,
	}
}

func backupRemoteFile(ctx context.Context, client sshExecutorClient, path string) core.Result {
	before, ok := remoteFileText(ctx, client, path)
	if !ok {
		return core.Ok(backupRemoteFileResult{})
	}

	backupPath := sprintf("%s.%s.bak", path, time.Now().UTC().Format("20060102T150405Z"))
	if r := client.Upload(ctx, newReader(before), backupPath, 0600); !r.OK {
		return r
	}

	return core.Ok(backupRemoteFileResult{Path: backupPath, HadBefore: true})
}

func backupCronTab(ctx context.Context, client sshExecutorClient, user, name string) core.Result {
	run := client.Run(ctx, sprintf("crontab -u %s -l 2>/dev/null", user))
	out := commandRunValue(run)
	if !run.OK {
		return wrapFailure(run, "Executor.moduleCron", "backup crontab")
	}
	if out.ExitCode != 0 || trimSpace(out.Stdout) == "" {
		return core.Ok("")
	}

	backupName := user
	if backupName == "" {
		backupName = "root"
	}
	if name != "" {
		backupName += "-" + name
	}
	backupName = sanitizeBackupToken(backupName)

	backupPath := joinPath("/tmp", sprintf("ansible-cron-%s.%s.bak", backupName, time.Now().UTC().Format("20060102T150405Z")))
	if r := client.Upload(ctx, newReader(out.Stdout), backupPath, 0600); !r.OK {
		return wrapFailure(r, "Executor.moduleCron", "backup crontab")
	}

	return core.Ok(backupPath)
}

func sanitizeBackupToken(value string) string {
	if value == "" {
		return "default"
	}

	b := newBuilder()
	b.Grow(len(value))
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}

	token := trimCutset(b.String(), "-")
	if token == "" {
		return "default"
	}
	return token
}

// templateArgs templates all string values in args.
func (e *Executor) templateArgs(args map[string]any, host string, task *Task) map[string]any {
	// Set inventory_hostname for templating
	oldInventoryHostname, hasInventoryHostname := e.vars["inventory_hostname"]
	e.vars["inventory_hostname"] = host
	defer func() {
		if hasInventoryHostname {
			e.vars["inventory_hostname"] = oldInventoryHostname
		} else {
			delete(e.vars, "inventory_hostname")
		}
	}()

	result := make(map[string]any)
	for k, v := range args {
		switch val := v.(type) {
		case string:
			result[k] = e.templateString(val, host, task)
		case map[string]any:
			// Recurse for nested maps
			result[k] = e.templateArgs(val, host, task)
		case []any:
			// Template strings in arrays
			templated := make([]any, len(val))
			for i, item := range val {
				if s, ok := item.(string); ok {
					templated[i] = e.templateString(s, host, task)
				} else {
					templated[i] = item
				}
			}
			result[k] = templated
		default:
			result[k] = v
		}
	}
	return result
}

// --- Command Modules ---

func (e *Executor) moduleShell(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		cmd = getStringArg(args, "cmd", "")
	}
	if cmd == "" {
		return core.Fail(coreerr.E("Executor.moduleShell", "no command specified", nil))
	}

	chdir := getStringArg(args, "chdir", "")

	skipResult := shouldSkipCommandModule(ctx, client, args, chdir)
	if !skipResult.OK {
		return skipResult
	}
	skip := skipResult.Value.(bool)
	if skip {
		return core.Ok(&TaskResult{Changed: false})
	}

	// Handle chdir
	if chdir != "" {
		cmd = sprintf("cd %q && %s", chdir, cmd)
	}

	if stdin := getStringArg(args, "stdin", ""); stdin != "" {
		cmd = prefixCommandStdin(cmd, stdin, getBoolArg(args, "stdin_add_newline", true))
	}

	run := runShellScriptCommand(ctx, client, cmd, getStringArg(args, "executable", ""))
	out := commandRunValue(run)
	if !run.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{
		Changed: true,
		Stdout:  out.Stdout,
		Stderr:  out.Stderr,
		RC:      out.ExitCode,
		Failed:  out.ExitCode != 0,
	})
}

func (e *Executor) moduleCommand(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	cmd := buildCommandModuleCommand(args)
	if cmd == "" {
		return core.Fail(coreerr.E("Executor.moduleCommand", "no command specified", nil))
	}

	chdir := getStringArg(args, "chdir", "")

	skipResult := shouldSkipCommandModule(ctx, client, args, chdir)
	if !skipResult.OK {
		return skipResult
	}
	skip := skipResult.Value.(bool)
	if skip {
		return core.Ok(&TaskResult{Changed: false})
	}

	// Handle chdir
	if chdir != "" {
		cmd = sprintf("cd %q && %s", chdir, cmd)
	}

	if stdin := getStringArg(args, "stdin", ""); stdin != "" {
		cmd = prefixCommandStdin(cmd, stdin, getBoolArg(args, "stdin_add_newline", true))
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{
		Changed: true,
		Stdout:  out.Stdout,
		Stderr:  out.Stderr,
		RC:      out.ExitCode,
		Failed:  out.ExitCode != 0,
	})
}

func buildCommandModuleCommand(args map[string]any) string {
	if argv := commandArgv(args); len(argv) > 0 {
		return join(" ", quoteArgs(argv))
	}

	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		cmd = getStringArg(args, "cmd", "")
	}
	return cmd
}

func commandArgv(args map[string]any) []string {
	raw, ok := args["argv"]
	if !ok {
		return nil
	}

	switch v := raw.(type) {
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				if s != "" {
					out = append(out, s)
				}
				continue
			}
			s := sprintf("%v", item)
			if s != "" && s != "<nil>" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	default:
		s := sprintf("%v", v)
		if s == "" || s == "<nil>" {
			return nil
		}
		return []string{s}
	}
}

func shouldSkipCommandModule(ctx context.Context, client sshExecutorClient, args map[string]any, chdir string) core.Result {
	if path := getStringArg(args, "creates", ""); path != "" {
		path = resolveCommandModulePath(path, chdir)
		existsResult := client.FileExists(ctx, path)
		if !existsResult.OK {
			return wrapFailure(existsResult, "Executor.shouldSkipCommandModule", "creates check")
		}
		exists := existsResult.Value.(bool)
		if exists {
			return core.Ok(true)
		}
	}

	if path := getStringArg(args, "removes", ""); path != "" {
		path = resolveCommandModulePath(path, chdir)
		existsResult := client.FileExists(ctx, path)
		if !existsResult.OK {
			return wrapFailure(existsResult, "Executor.shouldSkipCommandModule", "removes check")
		}
		exists := existsResult.Value.(bool)
		if !exists {
			return core.Ok(true)
		}
	}

	return core.Ok(false)
}

func resolveCommandModulePath(filePath, chdir string) string {
	filePath = trimSpace(filePath)
	if filePath == "" || pathIsAbs(filePath) || chdir == "" {
		return filePath
	}

	return joinPath(chdir, filePath)
}

func (e *Executor) moduleRaw(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		return core.Fail(coreerr.E("Executor.moduleRaw", "no command specified", nil))
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{
		Changed: true,
		Stdout:  out.Stdout,
		Stderr:  out.Stderr,
		RC:      out.ExitCode,
	})
}

func (e *Executor) moduleScript(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	script := getStringArg(args, "_raw_params", "")
	if script == "" {
		return core.Fail(coreerr.E("Executor.moduleScript", "no script specified", nil))
	}

	chdir := getStringArg(args, "chdir", "")

	skipResult := shouldSkipCommandModule(ctx, client, args, chdir)
	if !skipResult.OK {
		return skipResult
	}
	skip := skipResult.Value.(bool)
	if skip {
		return core.Ok(&TaskResult{Changed: false})
	}

	// Read local script
	script = e.resolveLocalPath(script)
	data, err := coreio.Local.Read(script)
	if err != nil {
		return core.Fail(coreerr.E("Executor.moduleScript", "read script", err))
	}

	if chdir != "" {
		data = sprintf("cd %q && %s", chdir, data)
	}

	run := runShellScriptCommand(ctx, client, data, getStringArg(args, "executable", ""))
	out := commandRunValue(run)
	if !run.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{
		Changed: true,
		Stdout:  out.Stdout,
		Stderr:  out.Stderr,
		RC:      out.ExitCode,
		Failed:  out.ExitCode != 0,
	})
}

// runShellScriptCommand executes a shell script using either the default
// heredoc path or a caller-specified executable.
//
// Example:
//
//	stdout, stderr, rc, err := runShellScriptCommand(ctx, client, "echo hi", "/bin/dash")
func runShellScriptCommand(ctx context.Context, client sshExecutorClient, script, executable string) core.Result {
	if executable == "" {
		return client.RunScript(ctx, script)
	}

	cmd := sprintf("%s -c %s", shellSingleQuote(executable), shellSingleQuote(script))
	return client.Run(ctx, cmd)
}

// --- File Modules ---

func (e *Executor) moduleCopy(ctx context.Context, client sshExecutorClient, args map[string]any, host string, task *Task) core.Result {
	dest := getStringArg(args, "dest", "")
	if dest == "" {
		return core.Fail(coreerr.E("Executor.moduleCopy", "dest required", nil))
	}
	force := getBoolArg(args, "force", true)
	backup := getBoolArg(args, "backup", false)
	remoteSrc := getBoolArg(args, "remote_src", false)

	var content string
	if src := getStringArg(args, "src", ""); src != "" {
		if remoteSrc {
			dataResult := client.Download(ctx, src)
			if !dataResult.OK {
				return wrapFailure(dataResult, "Executor.moduleCopy", "download src")
			}
			data := dataResult.Value.([]byte)
			content = string(data)
		} else {
			src = e.resolveLocalPath(src)
			var err error
			content, err = coreio.Local.Read(src)
			if err != nil {
				return core.Fail(coreerr.E("Executor.moduleCopy", "read src", err))
			}
		}
	} else if c := getStringArg(args, "content", ""); c != "" {
		content = c
	} else {
		return core.Fail(coreerr.E("Executor.moduleCopy", "src or content required", nil))
	}

	mode := fs.FileMode(0644)
	if m := getStringArg(args, "mode", ""); m != "" {
		if parsed, err := strconv.ParseInt(m, 8, 32); err == nil {
			mode = fs.FileMode(parsed)
		}
	}

	before, hasBefore := remoteFileText(ctx, client, dest)
	if hasBefore && !force {
		return core.Ok(&TaskResult{Changed: false, Msg: sprintf("skipped existing destination: %s", dest)})
	}
	if hasBefore && before == content {
		if getStringArg(args, "owner", "") == "" && getStringArg(args, "group", "") == "" {
			return core.Ok(&TaskResult{Changed: false, Msg: sprintf("already up to date: %s", dest)})
		}
	}

	var backupPath string
	if backup && hasBefore {
		backupResult := backupRemoteFile(ctx, client, dest)
		if !backupResult.OK {
			return wrapFailure(backupResult, "Executor.moduleCopy", "backup destination")
		}
		backupPath = backupResult.Value.(backupRemoteFileResult).Path
	}

	if r := client.Upload(ctx, newReader(content), dest, mode); !r.OK {
		return r
	}

	// Handle owner/group (best-effort, errors ignored)
	if owner := getStringArg(args, "owner", ""); owner != "" {
		runBestEffort(ctx, client, sprintf("chown %s %q", owner, dest))
	}
	if group := getStringArg(args, "group", ""); group != "" {
		runBestEffort(ctx, client, sprintf("chgrp %s %q", group, dest))
	}

	result := &TaskResult{Changed: true, Msg: sprintf("copied to %s", dest)}
	if backupPath != "" {
		result.Data = map[string]any{"backup_file": backupPath}
	}
	if e.Diff {
		if hasBefore {
			if result.Data == nil {
				result.Data = make(map[string]any)
			}
			result.Data["diff"] = fileDiffData(dest, before, content)
		}
	}
	return core.Ok(result)
}

func (e *Executor) moduleTemplate(ctx context.Context, client sshExecutorClient, args map[string]any, host string, task *Task) core.Result {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	if src == "" || dest == "" {
		return core.Fail(coreerr.E("Executor.moduleTemplate", "src and dest required", nil))
	}
	force := getBoolArg(args, "force", true)
	backup := getBoolArg(args, "backup", false)

	// Process template
	src = e.resolveLocalPath(src)
	templateResult := e.TemplateFile(src, host, task)
	if !templateResult.OK {
		return wrapFailure(templateResult, "Executor.moduleTemplate", "template")
	}
	content := templateResult.Value.(string)

	mode := fs.FileMode(0644)
	if m := getStringArg(args, "mode", ""); m != "" {
		if parsed, err := strconv.ParseInt(m, 8, 32); err == nil {
			mode = fs.FileMode(parsed)
		}
	}

	before, hasBefore := remoteFileText(ctx, client, dest)
	if hasBefore && !force {
		return core.Ok(&TaskResult{Changed: false, Msg: sprintf("skipped existing destination: %s", dest)})
	}
	if hasBefore && before == content {
		return core.Ok(&TaskResult{Changed: false, Msg: sprintf("already up to date: %s", dest)})
	}

	var backupPath string
	if backup && hasBefore {
		backupResult := backupRemoteFile(ctx, client, dest)
		if !backupResult.OK {
			return wrapFailure(backupResult, "Executor.moduleTemplate", "backup destination")
		}
		backupPath = backupResult.Value.(backupRemoteFileResult).Path
	}

	if r := client.Upload(ctx, newReader(content), dest, mode); !r.OK {
		return r
	}

	result := &TaskResult{Changed: true, Msg: sprintf("templated to %s", dest)}
	if backupPath != "" {
		result.Data = map[string]any{"backup_file": backupPath}
	}
	if e.Diff {
		if hasBefore {
			if result.Data == nil {
				result.Data = make(map[string]any)
			}
			result.Data["diff"] = fileDiffData(dest, before, content)
		}
	}
	return core.Ok(result)
}

func (e *Executor) moduleFile(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	path := getStringArg(args, pathArgKey, "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return core.Fail(coreerr.E("Executor.moduleFile", "path required", nil))
	}

	state := getStringArg(args, "state", "file")

	switch state {
	case "directory":
		mode := getStringArg(args, "mode", "0755")
		cmd := sprintf("mkdir -p %q && chmod %s %q", path, mode, path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
		}

	case "absent":
		cmd := sprintf("rm -rf %q", path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, RC: out.ExitCode})
		}

	case "touch":
		cmd := sprintf("touch %q", path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, RC: out.ExitCode})
		}

	case "link":
		src := getStringArg(args, "src", "")
		if src == "" {
			return core.Fail(coreerr.E("Executor.moduleFile", "src required for link state", nil))
		}
		cmd := sprintf("ln -sf %q %q", src, path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, RC: out.ExitCode})
		}

	case "hard":
		src := getStringArg(args, "src", "")
		if src == "" {
			return core.Fail(coreerr.E("Executor.moduleFile", "src required for hard state", nil))
		}
		cmd := sprintf("ln -f %q %q", src, path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, RC: out.ExitCode})
		}

	case "file":
		// Ensure file exists and set permissions
		if mode := getStringArg(args, "mode", ""); mode != "" {
			runBestEffort(ctx, client, sprintf("chmod %s %q", mode, path))
		}
	}

	// Handle owner/group (best-effort, errors ignored)
	if owner := getStringArg(args, "owner", ""); owner != "" {
		runBestEffort(ctx, client, sprintf("chown %s %q", owner, path))
	}
	if group := getStringArg(args, "group", ""); group != "" {
		runBestEffort(ctx, client, sprintf("chgrp %s %q", group, path))
	}
	if recurse := getBoolArg(args, "recurse", false); recurse {
		if owner := getStringArg(args, "owner", ""); owner != "" {
			runBestEffort(ctx, client, sprintf("chown -R %s %q", owner, path))
		}
		if group := getStringArg(args, "group", ""); group != "" {
			runBestEffort(ctx, client, sprintf("chgrp -R %s %q", group, path))
		}
		if mode := getStringArg(args, "mode", ""); mode != "" {
			runBestEffort(ctx, client, sprintf("chmod -R %s %q", mode, path))
		}
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleLineinfile(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	path := getStringArg(args, pathArgKey, "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return core.Fail(coreerr.E("Executor.moduleLineinfile", "path required", nil))
	}

	before, hasBefore := remoteFileText(ctx, client, path)

	line := getStringArg(args, "line", "")
	regexp := getStringArg(args, "regexp", "")
	searchString := getStringArg(args, "search_string", "")
	state := getStringArg(args, "state", "present")
	backup := getBoolArg(args, "backup", false)
	backrefs := getBoolArg(args, "backrefs", false)
	create := getBoolArg(args, "create", false)
	insertBefore := getStringArg(args, "insertbefore", "")
	insertAfter := getStringArg(args, "insertafter", "")
	firstMatch := getBoolArg(args, "firstmatch", false)

	var backupPath string
	ensureBackup := func() core.Result {
		if !backup || backupPath != "" {
			return core.Ok(nil)
		}

		backupResult := backupRemoteFile(ctx, client, path)
		if !backupResult.OK {
			return wrapFailure(backupResult, "Executor.moduleLineinfile", "backup remote file")
		}
		backupData := backupResult.Value.(backupRemoteFileResult)
		backupPath = backupData.Path
		hasCopy := backupData.HadBefore
		if !hasCopy {
			backupPath = ""
		}
		return core.Ok(nil)
	}

	if state != "absent" && line != "" && regexp == "" && insertBefore == "" && insertAfter == "" {
		if hasBefore && fileContainsExactLine(before, line) {
			return core.Ok(&TaskResult{Changed: false, Msg: sprintf("already up to date: %s", path)})
		}
	}

	if state == "absent" {
		if searchString != "" {
			if !hasBefore || !contains(before, searchString) {
				return core.Ok(&TaskResult{Changed: false})
			}
			if r := ensureBackup(); !r.OK {
				return r
			}

			updated, changed := removeLinesContaining(before, searchString)
			if !changed {
				return core.Ok(&TaskResult{Changed: false})
			}
			if r := client.Upload(ctx, newReader(updated), path, 0644); !r.OK {
				return wrapFailure(r, "Executor.moduleLineinfile", "upload lineinfile search_string removal")
			}
			result := &TaskResult{Changed: true}
			if backupPath != "" {
				result.Data = map[string]any{"backup_file": backupPath}
			}
			if e.Diff {
				if after, ok := remoteFileText(ctx, client, path); ok && before != after {
					result.Data = ensureTaskResultData(result.Data)
					result.Data["diff"] = fileDiffData(path, before, after)
				}
			}
			return core.Ok(result)
		}

		if regexp != "" {
			if content, ok := remoteFileText(ctx, client, path); !ok || !regexpMatchString(regexp, content) {
				return core.Ok(&TaskResult{Changed: false})
			}
			if r := ensureBackup(); !r.OK {
				return r
			}
			cmd := sprintf("sed -i '/%s/d' %q", regexp, path)
			run := client.Run(ctx, cmd)
			out := commandRunValue(run)
			if out.ExitCode != 0 {
				return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, RC: out.ExitCode})
			}
		}
	} else {
		// Create the file first when requested so regexp-based updates have a
		// target to operate on.
		if create {
			runBestEffort(ctx, client, sprintf("touch %q", path))
		}

		// state == present
		if searchString != "" {
			if hasBefore && fileContainsExactLine(before, line) {
				return core.Ok(&TaskResult{Changed: false, Msg: sprintf("already up to date: %s", path)})
			}

			if hasBefore {
				updated, changed := replaceFirstLineContaining(before, searchString, line)
				if changed {
					if r := ensureBackup(); !r.OK {
						return r
					}
					if r := client.Upload(ctx, newReader(updated), path, 0644); !r.OK {
						return wrapFailure(r, "Executor.moduleLineinfile", "upload lineinfile search_string replacement")
					}
					result := &TaskResult{Changed: true}
					if backupPath != "" {
						result.Data = map[string]any{"backup_file": backupPath}
					}
					if e.Diff {
						if after, ok := remoteFileText(ctx, client, path); ok && before != after {
							result.Data = ensureTaskResultData(result.Data)
							result.Data["diff"] = fileDiffData(path, before, after)
						}
					}
					return core.Ok(result)
				}
			}

			if r := ensureBackup(); !r.OK {
				return r
			}
			insertedResult := insertLineRelativeToMatch(ctx, client, path, line, insertBefore, insertAfter, firstMatch)
			if !insertedResult.OK {
				return insertedResult
			} else if insertedResult.Value.(bool) {
				return core.Ok(&TaskResult{Changed: true})
			}

			updated := line
			if hasBefore {
				updated = before
				if updated != "" && !hasSuffix(updated, "\n") {
					updated += "\n"
				}
				updated += line
			}
			if !hasBefore && line != "" {
				updated = line + "\n"
			}

			if r := client.Upload(ctx, newReader(updated), path, 0644); !r.OK {
				return wrapFailure(r, "Executor.moduleLineinfile", "upload lineinfile search_string append")
			}
			result := &TaskResult{Changed: true}
			if backupPath != "" {
				result.Data = map[string]any{"backup_file": backupPath}
			}
			if e.Diff {
				if after, ok := remoteFileText(ctx, client, path); ok && before != after {
					result.Data = ensureTaskResultData(result.Data)
					result.Data["diff"] = fileDiffData(path, before, after)
				}
			}
			return core.Ok(result)
		}

		if regexp != "" {
			escapedLine := replaceAll(line, "/", "\\/")
			sedFlags := "-i"
			if backrefs {
				// When backrefs is enabled, Ansible only replaces matching lines
				// and does not append a new line when the pattern is absent.
				matchCmd := sprintf("grep -Eq %q %q", regexp, path)
				matchRun := client.Run(ctx, matchCmd)
				if commandRunValue(matchRun).ExitCode != 0 {
					return core.Ok(&TaskResult{Changed: false})
				}
				sedFlags = "-E -i"
			}

			if r := ensureBackup(); !r.OK {
				return r
			}

			cmd := sprintf("sed %s 's/%s/%s/' %q", sedFlags, regexp, escapedLine, path)
			run := client.Run(ctx, cmd)
			if commandRunValue(run).ExitCode != 0 {
				if backrefs {
					return core.Ok(&TaskResult{Changed: false})
				}

				if r := ensureBackup(); !r.OK {
					return r
				}
				insertedResult := insertLineRelativeToMatch(ctx, client, path, line, insertBefore, insertAfter, firstMatch)
				if !insertedResult.OK {
					return insertedResult
				} else if insertedResult.Value.(bool) {
					return core.Ok(&TaskResult{Changed: true})
				}

				// Line not found, append.
				if r := ensureBackup(); !r.OK {
					return r
				}
				cmd = sprintf("echo %q >> %q", line, path)
				runBestEffort(ctx, client, cmd)
			}
		} else if line != "" {
			if r := ensureBackup(); !r.OK {
				return r
			}
			insertedResult := insertLineRelativeToMatch(ctx, client, path, line, insertBefore, insertAfter, firstMatch)
			if !insertedResult.OK {
				return insertedResult
			} else if insertedResult.Value.(bool) {
				return core.Ok(&TaskResult{Changed: true})
			}

			// Ensure line is present
			cmd := sprintf("grep -qxF %q %q || echo %q >> %q", line, path, line, path)
			runBestEffort(ctx, client, cmd)
		}
	}

	result := &TaskResult{Changed: true}
	if backupPath != "" {
		result.Data = map[string]any{"backup_file": backupPath}
	}
	if e.Diff {
		if after, ok := remoteFileText(ctx, client, path); ok && before != after {
			if result.Data == nil {
				result.Data = make(map[string]any)
			}
			result.Data["diff"] = fileDiffData(path, before, after)
		}
	}

	return core.Ok(result)
}

func (e *Executor) moduleReplace(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	path := getStringArg(args, pathArgKey, "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return core.Fail(coreerr.E("Executor.moduleReplace", "path required", nil))
	}

	pattern := getStringArg(args, "regexp", "")
	if pattern == "" {
		return core.Fail(coreerr.E("Executor.moduleReplace", "regexp required", nil))
	}

	replacement := getStringArg(args, "replace", "")
	backup := getBoolArg(args, "backup", false)

	before, ok := remoteFileText(ctx, client, path)
	if !ok {
		return core.Ok(&TaskResult{Failed: true, Msg: sprintf("file not found: %s", path)})
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return core.Fail(coreerr.E("Executor.moduleReplace", "compile regexp", err))
	}

	after := re.ReplaceAllString(before, replacement)
	if after == before {
		return core.Ok(&TaskResult{Changed: false, Msg: sprintf("already up to date: %s", path)})
	}

	result := &TaskResult{Changed: true}
	if backup {
		backupResult := backupRemoteFile(ctx, client, path)
		if !backupResult.OK {
			return wrapFailure(backupResult, "Executor.moduleReplace", "backup remote file")
		}
		backupData := backupResult.Value.(backupRemoteFileResult)
		if backupData.HadBefore {
			result.Data = map[string]any{"backup_file": backupData.Path}
		}
	}

	if r := client.Upload(ctx, newReader(after), path, 0644); !r.OK {
		return wrapFailure(r, "Executor.moduleReplace", "upload replacement")
	}

	if e.Diff {
		result.Data = ensureTaskResultData(result.Data)
		result.Data["diff"] = fileDiffData(path, before, after)
	}

	return core.Ok(result)
}

func ensureTaskResultData(data map[string]any) map[string]any {
	if data != nil {
		return data
	}
	return make(map[string]any)
}

func fileContainsExactLine(content, line string) bool {
	if content == "" || line == "" {
		return false
	}

	for _, candidate := range split(content, "\n") {
		if candidate == line {
			return true
		}
	}

	return false
}

func regexpMatchString(pattern, value string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

func insertLineRelativeToMatch(ctx context.Context, client commandRunner, path, line, insertBefore, insertAfter string, firstMatch bool) core.Result {
	if line == "" {
		return core.Ok(false)
	}

	if insertBefore == "BOF" {
		cmd := sprintf("tmp=$(mktemp) && { printf %%s %s; cat %q; } > \"$tmp\" && mv \"$tmp\" %q", shellSingleQuote(line+"\n"), path, path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK {
			return wrapFailure(run, "Executor.moduleLineinfile", "insertbefore line")
		}
		if out.ExitCode != 0 {
			return core.Fail(coreerr.E("Executor.moduleLineinfile", "insertbefore line: "+out.Stderr, nil))
		}
		return core.Ok(true)
	}

	if insertAfter == "EOF" {
		cmd := sprintf("echo %q >> %q", line, path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK {
			return wrapFailure(run, "Executor.moduleLineinfile", "insertafter line")
		}
		if out.ExitCode != 0 {
			return core.Fail(coreerr.E("Executor.moduleLineinfile", "insertafter line: "+out.Stderr, nil))
		}
		return core.Ok(true)
	}

	if insertBefore != "" {
		matchCmd := sprintf("grep -Eq %q %q", insertBefore, path)
		matchRun := client.Run(ctx, matchCmd)
		if commandRunValue(matchRun).ExitCode == 0 {
			cmd := buildLineinfileInsertCommand(path, line, insertBefore, false, firstMatch)
			run := client.Run(ctx, cmd)
			out := commandRunValue(run)
			if !run.OK {
				return wrapFailure(run, "Executor.moduleLineinfile", "insertbefore line")
			}
			if out.ExitCode != 0 {
				return core.Fail(coreerr.E("Executor.moduleLineinfile", "insertbefore line: "+out.Stderr, nil))
			}
			return core.Ok(true)
		}
	}

	if insertAfter != "" {
		matchCmd := sprintf("grep -Eq %q %q", insertAfter, path)
		matchRun := client.Run(ctx, matchCmd)
		if commandRunValue(matchRun).ExitCode == 0 {
			cmd := buildLineinfileInsertCommand(path, line, insertAfter, true, firstMatch)
			run := client.Run(ctx, cmd)
			out := commandRunValue(run)
			if !run.OK {
				return wrapFailure(run, "Executor.moduleLineinfile", "insertafter line")
			}
			if out.ExitCode != 0 {
				return core.Fail(coreerr.E("Executor.moduleLineinfile", "insertafter line: "+out.Stderr, nil))
			}
			return core.Ok(true)
		}
	}

	return core.Ok(false)
}

func replaceFirstLineContaining(content, needle, line string) (string, bool) {
	if content == "" || needle == "" {
		return content, false
	}

	lines := split(content, "\n")
	changed := false
	for i, current := range lines {
		if changed {
			continue
		}
		if contains(current, needle) {
			lines[i] = line
			changed = true
		}
	}
	if !changed {
		return content, false
	}

	return join("\n", lines), true
}

func removeLinesContaining(content, needle string) (string, bool) {
	if content == "" || needle == "" {
		return content, false
	}

	lines := split(content, "\n")
	kept := make([]string, 0, len(lines))
	removed := false
	for _, current := range lines {
		if contains(current, needle) {
			removed = true
			continue
		}
		kept = append(kept, current)
	}
	if !removed {
		return content, false
	}

	return join("\n", kept), true
}

func buildLineinfileInsertCommand(path, line, anchor string, after, firstMatch bool) string {
	quotedLine := shellSingleQuote(line)
	quotedAnchor := shellSingleQuote(anchor)
	if firstMatch {
		if after {
			return sprintf("tmp=$(mktemp) && awk -v line=%s -v re=%s 'BEGIN{done=0} { print; if (!done && $0 ~ re) { print line; done=1 } }' %q > \"$tmp\" && mv \"$tmp\" %q",
				quotedLine, quotedAnchor, path, path)
		}

		return sprintf("tmp=$(mktemp) && awk -v line=%s -v re=%s 'BEGIN{done=0} { if (!done && $0 ~ re) { print line; done=1 } print }' %q > \"$tmp\" && mv \"$tmp\" %q",
			quotedLine, quotedAnchor, path, path)
	}

	if after {
		return sprintf("tmp=$(mktemp) && awk -v line=%s -v re=%s 'BEGIN{pos=0} { lines[NR]=$0; if ($0 ~ re) { pos=NR } } END { for (i=1; i<=NR; i++) { print lines[i]; if (i==pos) { print line } } }' %q > \"$tmp\" && mv \"$tmp\" %q",
			quotedLine, quotedAnchor, path, path)
	}

	return sprintf("tmp=$(mktemp) && awk -v line=%s -v re=%s 'BEGIN{pos=0} { lines[NR]=$0; if ($0 ~ re) { pos=NR } } END { for (i=1; i<=NR; i++) { if (i==pos) { print line } print lines[i] } }' %q > \"$tmp\" && mv \"$tmp\" %q",
		quotedLine, quotedAnchor, path, path)
}

func (e *Executor) moduleStat(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	path := getStringArg(args, pathArgKey, "")
	if path == "" {
		return core.Fail(coreerr.E("Executor.moduleStat", "path required", nil))
	}

	statResult := client.Stat(ctx, path)
	if !statResult.OK {
		return statResult
	}
	stat := statResult.Value.(map[string]any)

	return core.Ok(&TaskResult{
		Changed: false,
		Data:    map[string]any{"stat": stat},
	})
}

func (e *Executor) moduleSlurp(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	path := getStringArg(args, pathArgKey, "")
	if path == "" {
		path = getStringArg(args, "src", "")
	}
	if path == "" {
		return core.Fail(coreerr.E("Executor.moduleSlurp", "path required", nil))
	}

	contentResult := client.Download(ctx, path)
	if !contentResult.OK {
		return contentResult
	}
	content := contentResult.Value.([]byte)

	encoded := base64.StdEncoding.EncodeToString(content)

	return core.Ok(&TaskResult{
		Changed: false,
		Data:    map[string]any{"content": encoded, "encoding": "base64"},
	})
}

func (e *Executor) moduleFetch(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	if src == "" || dest == "" {
		return core.Fail(coreerr.E("Executor.moduleFetch", "src and dest required", nil))
	}

	contentResult := client.Download(ctx, src)
	if !contentResult.OK {
		return contentResult
	}
	content := contentResult.Value.([]byte)

	// Create dest directory
	if err := coreio.Local.EnsureDir(pathDir(dest)); err != nil {
		return core.Fail(err)
	}

	if err := coreio.Local.Write(dest, string(content)); err != nil {
		return core.Fail(err)
	}

	return core.Ok(&TaskResult{Changed: true, Msg: sprintf("fetched %s to %s", src, dest)})
}

func (e *Executor) moduleGetURL(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	url := getStringArg(args, "url", "")
	dest := getStringArg(args, "dest", "")
	if url == "" || dest == "" {
		return core.Fail(coreerr.E("Executor.moduleGetURL", "url and dest required", nil))
	}

	force := getBoolArg(args, "force", true)
	useProxy := getBoolArg(args, "use_proxy", true)
	checksumSpec := corexTrimSpace(getStringArg(args, "checksum", ""))

	if !force {
		existsResult := client.FileExists(ctx, dest)
		if !existsResult.OK {
			return existsResult
		}
		exists := existsResult.Value.(bool)
		if exists {
			return core.Ok(&TaskResult{Changed: false, Msg: sprintf("skipped existing destination: %s", dest)})
		}
	}

	// Stream to stdout so we can validate checksums before writing the file.
	cmd := sprintf("curl -fsSL %q || wget -q -O - %q", url, url)
	if !useProxy {
		cmd = sprintf("curl --noproxy %s -fsSL %q || wget --no-proxy -q -O - %q", shellQuote("*"), url, url)
	}
	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	content := []byte(out.Stdout)
	if checksumSpec != "" {
		checksumResult := resolveGetURLChecksumValue(ctx, client, checksumSpec, dest)
		if !checksumResult.OK {
			return core.Ok(&TaskResult{Failed: true, Msg: checksumResult.Error(), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
		}
		checksumValue := checksumResult.Value.(string)
		if checksumCheck := verifyGetURLChecksum(content, checksumValue); !checksumCheck.OK {
			return core.Ok(&TaskResult{Failed: true, Msg: checksumCheck.Error(), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
		}
	}

	mode := fs.FileMode(0644)
	// Set mode if specified (best-effort).
	if modeArg := getStringArg(args, "mode", ""); modeArg != "" {
		if parsed, err := strconv.ParseInt(modeArg, 8, 32); err == nil {
			mode = fs.FileMode(parsed)
		}
	}

	if r := client.Upload(ctx, core.NewBuffer(content), dest, mode); !r.OK {
		return r
	}

	return core.Ok(&TaskResult{Changed: true})
}

func resolveGetURLChecksumValue(ctx context.Context, client sshExecutorClient, checksumSpec, dest string) core.Result {
	algorithm := "sha256"
	expected := checksumSpec
	if idx := stringIndex(checksumSpec, ":"); idx > 0 {
		candidateAlgorithm := lower(corexTrimSpace(checksumSpec[:idx]))
		if isChecksumAlgorithm(candidateAlgorithm) {
			algorithm = candidateAlgorithm
			expected = corexTrimSpace(checksumSpec[idx+1:])
		}
	}

	expected = corexTrimSpace(expected)
	if expected == "" {
		return core.Fail(coreerr.E("Executor.moduleGetURL", "checksum required", nil))
	}

	if contains(expected, "://") {
		cmd := sprintf("curl -fsSL %q || wget -q -O - %q", expected, expected)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			msg := out.Stderr
			if msg == "" {
				msg = run.Error()
			}
			return core.Fail(coreerr.E("Executor.moduleGetURL", "download checksum file: "+msg, nil))
		}
		parseResult := parseGetURLChecksumFile(out.Stdout, dest, algorithm)
		if !parseResult.OK {
			return parseResult
		}
		expected = parseResult.Value.(string)
	}

	return core.Ok(sprintf("%s:%s", algorithm, lower(expected)))
}

func isChecksumAlgorithm(value string) bool {
	switch value {
	case "", "sha1", "sha224", "sha256", "sha384", "sha512":
		return true
	default:
		return false
	}
}

func parseGetURLChecksumFile(content, dest, algorithm string) core.Result {
	lines := split(content, "\n")
	base := pathBase(dest)

	for _, line := range lines {
		fields := fields(line)
		if len(fields) == 0 {
			continue
		}

		candidate := lower(fields[0])
		if !isHexDigest(candidate) {
			continue
		}

		if len(fields) == 1 {
			return core.Ok(candidate)
		}

		for _, field := range fields[1:] {
			cleaned := trimPrefix(field, "*")
			cleaned = pathBase(trimSpace(cleaned))
			if cleaned == base || cleaned == pathBase(dest) {
				return core.Ok(candidate)
			}
		}
	}

	for _, line := range lines {
		fields := fields(line)
		if len(fields) == 0 {
			continue
		}
		candidate := lower(fields[0])
		if isHexDigest(candidate) {
			return core.Ok(candidate)
		}
	}

	return core.Fail(coreerr.E("Executor.moduleGetURL", sprintf("could not parse checksum file for %s", algorithm), nil))
}

func isHexDigest(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func verifyGetURLChecksum(
	content []byte, checksumValue string,
) core.Result {
	checksumValue = lower(corexTrimSpace(checksumValue))
	if checksumValue == "" {
		return core.Fail(coreerr.E("Executor.moduleGetURL", "checksum required", nil))
	}

	parts := splitN(checksumValue, ":", 2)
	algorithm := "sha256"
	expected := checksumValue
	if len(parts) == 2 {
		algorithm = lower(corexTrimSpace(parts[0]))
		expected = corexTrimSpace(parts[1])
	}

	expected = lower(corexTrimSpace(expected))
	if expected == "" {
		return core.Fail(coreerr.E("Executor.moduleGetURL", "checksum required", nil))
	}

	var actual string
	switch algorithm {
	case "", "sha256":
		sum := sha256.Sum256(content)
		actual = hex.EncodeToString(sum[:])
	case "sha1":
		sum := sha1.Sum(content)
		actual = hex.EncodeToString(sum[:])
	case "sha224":
		sum := sha256.Sum224(content)
		actual = hex.EncodeToString(sum[:])
	case "sha384":
		sum := sha512.Sum384(content)
		actual = hex.EncodeToString(sum[:])
	case "sha512":
		sum := sha512.Sum512(content)
		actual = hex.EncodeToString(sum[:])
	default:
		return core.Fail(coreerr.E("Executor.moduleGetURL", "unsupported checksum algorithm: "+algorithm, nil))
	}

	if actual != expected {
		return core.Fail(coreerr.E("Executor.moduleGetURL", sprintf("checksum mismatch: expected %s but got %s", expected, actual), nil))
	}

	return core.Ok(nil)
}

// --- Package Modules ---

func (e *Executor) moduleApt(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	names := normalizeStringArgs(args["name"])
	state := getStringArg(args, "state", "present")
	updateCache := getBoolArg(args, "update_cache", false)

	var cmd string

	if updateCache {
		runBestEffort(ctx, client, "apt-get update -qq")
	}

	switch state {
	case "present", "installed":
		if len(names) > 0 {
			cmd = sprintf("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq %s", join(" ", names))
		}
	case "absent", "removed":
		if len(names) > 0 {
			cmd = sprintf("DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq %s", join(" ", names))
		}
	case "latest":
		if len(names) > 0 {
			cmd = sprintf("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --only-upgrade %s", join(" ", names))
		}
	}

	if cmd == "" {
		return core.Ok(&TaskResult{Changed: false})
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleAptKey(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	url := getStringArg(args, "url", "")
	keyring := getStringArg(args, "keyring", "")
	state := getStringArg(args, "state", "present")

	if state == "absent" {
		if keyring != "" {
			runBestEffort(ctx, client, sprintf("rm -f %q", keyring))
		}
		return core.Ok(&TaskResult{Changed: true})
	}

	if url == "" {
		return core.Fail(coreerr.E("Executor.moduleAptKey", "url required", nil))
	}

	var cmd string
	if keyring != "" {
		cmd = sprintf("curl -fsSL %q | gpg --dearmor -o %q", url, keyring)
	} else {
		cmd = sprintf("curl -fsSL %q | apt-key add -", url)
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleAptRepository(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	repo := getStringArg(args, "repo", "")
	filename := getStringArg(args, "filename", "")
	state := getStringArg(args, "state", "present")

	if repo == "" {
		return core.Fail(coreerr.E("Executor.moduleAptRepository", "repo required", nil))
	}

	if filename == "" {
		// Generate filename from repo
		filename = replaceAll(repo, " ", "-")
		filename = replaceAll(filename, "/", "-")
		filename = replaceAll(filename, ":", "")
	}

	path := sprintf("/etc/apt/sources.list.d/%s.list", filename)

	if state == "absent" {
		runBestEffort(ctx, client, sprintf("rm -f %q", path))
		return core.Ok(&TaskResult{Changed: true})
	}

	cmd := sprintf("echo %q > %q", repo, path)
	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	// Update apt cache (best-effort)
	if getBoolArg(args, "update_cache", true) {
		runBestEffort(ctx, client, "apt-get update -qq")
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) modulePackage(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	// Detect package manager and delegate
	run := client.Run(ctx, "which apt-get yum dnf 2>/dev/null | head -1")
	stdout := corexTrimSpace(commandRunValue(run).Stdout)

	switch {
	case contains(stdout, "dnf"):
		return e.moduleDnf(ctx, client, args)
	case contains(stdout, "yum"):
		return e.moduleYum(ctx, client, args)
	default:
		return e.moduleApt(ctx, client, args)
	}
}

func (e *Executor) moduleYum(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	return e.moduleRPM(ctx, client, args, "yum")
}

func (e *Executor) moduleDnf(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	return e.moduleRPM(ctx, client, args, "dnf")
}

func (e *Executor) moduleRPM(ctx context.Context, client sshExecutorClient, args map[string]any, packageManager string) core.Result {
	names := normalizeStringArgs(args["name"])
	state := getStringArg(args, "state", "present")
	updateCache := getBoolArg(args, "update_cache", false)

	if updateCache && packageManager != "rpm" {
		runBestEffort(ctx, client, sprintf("%s makecache -y", packageManager))
	}

	var cmd string
	switch state {
	case "present", "installed":
		if len(names) > 0 {
			if packageManager == "rpm" {
				cmd = sprintf("rpm -ivh %s", join(" ", names))
			} else {
				cmd = sprintf("%s install -y -q %s", packageManager, join(" ", names))
			}
		}
	case "absent", "removed":
		if len(names) > 0 {
			if packageManager == "rpm" {
				cmd = sprintf("rpm -e %s", join(" ", names))
			} else {
				cmd = sprintf("%s remove -y -q %s", packageManager, join(" ", names))
			}
		}
	case "latest":
		if len(names) > 0 {
			if packageManager == "rpm" {
				cmd = sprintf("rpm -Uvh %s", join(" ", names))
			} else if packageManager == "dnf" {
				cmd = sprintf("%s upgrade -y -q %s", packageManager, join(" ", names))
			} else {
				cmd = sprintf("%s update -y -q %s", packageManager, join(" ", names))
			}
		}
	}

	if cmd == "" {
		return core.Ok(&TaskResult{Changed: false})
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) modulePip(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	names := normalizeStringArgs(args["name"])
	state := getStringArg(args, "state", "present")
	executable := getStringArg(args, "executable", "pip3")
	virtualenv := getStringArg(args, "virtualenv", "")
	requirements := getStringArg(args, "requirements", "")
	extraArgs := getStringArg(args, "extra_args", "")

	if virtualenv != "" && executable == "pip3" {
		executable = joinPath(virtualenv, "bin", "pip")
	}

	var cmd string
	switch state {
	case "present", "installed":
		parts := []string{executable, "install"}
		if extraArgs != "" {
			parts = append(parts, extraArgs)
		}
		switch {
		case requirements != "":
			parts = append(parts, sprintf("-r %q", requirements))
		case len(names) > 0:
			parts = append(parts, join(" ", names))
		}
		cmd = join(" ", parts)
	case "absent", "removed":
		if len(names) > 0 {
			parts := []string{executable, "uninstall", "-y"}
			if extraArgs != "" {
				parts = append(parts, extraArgs)
			}
			parts = append(parts, join(" ", names))
			cmd = join(" ", parts)
		}
	case "latest":
		if len(names) > 0 {
			parts := []string{executable, "install", "--upgrade"}
			if extraArgs != "" {
				parts = append(parts, extraArgs)
			}
			parts = append(parts, join(" ", names))
			cmd = join(" ", parts)
		}
	}

	if cmd == "" {
		return core.Ok(&TaskResult{Changed: false})
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

// --- Service Modules ---

func (e *Executor) moduleService(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "")
	enabled := args["enabled"]

	if name == "" {
		return core.Fail(coreerr.E("Executor.moduleService", "name required", nil))
	}

	var cmds []string

	if state != "" {
		switch state {
		case "started":
			cmds = append(cmds, sprintf("systemctl start %s", name))
		case "stopped":
			cmds = append(cmds, sprintf("systemctl stop %s", name))
		case "restarted":
			cmds = append(cmds, sprintf("systemctl restart %s", name))
		case "reloaded":
			cmds = append(cmds, sprintf("systemctl reload %s", name))
		}
	}

	if enabled != nil {
		if getBoolArg(args, "enabled", false) {
			cmds = append(cmds, sprintf("systemctl enable %s", name))
		} else {
			cmds = append(cmds, sprintf("systemctl disable %s", name))
		}
	}

	for _, cmd := range cmds {
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
		}
	}

	return core.Ok(&TaskResult{Changed: len(cmds) > 0})
}

func (e *Executor) moduleSystemd(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	// systemd is similar to service
	if getBoolArg(args, "daemon_reload", false) {
		runBestEffort(ctx, client, "systemctl daemon-reload")
	}

	return e.moduleService(ctx, client, args)
}

// --- User/Group Modules ---

func (e *Executor) moduleUser(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	appendGroups := getBoolArg(args, "append", false)
	local := getBoolArg(args, "local", false)

	if name == "" {
		return core.Fail(coreerr.E("Executor.moduleUser", "name required", nil))
	}

	if state == "absent" {
		delCmd := "userdel"
		if local {
			delCmd = "luserdel"
		}
		cmd := sprintf("%s -r %s 2>/dev/null || true", delCmd, name)
		runBestEffort(ctx, client, cmd)
		return core.Ok(&TaskResult{Changed: true})
	}

	// Build useradd/usermod command
	var addOpts []string
	var modOpts []string

	if uid := getStringArg(args, "uid", ""); uid != "" {
		addOpts = append(addOpts, "-u", uid)
		modOpts = append(modOpts, "-u", uid)
	}
	if group := getStringArg(args, "group", ""); group != "" {
		addOpts = append(addOpts, "-g", group)
		modOpts = append(modOpts, "-g", group)
	}
	if groups := normalizeStringArgs(args["groups"]); len(groups) > 0 {
		addOpts = append(addOpts, "-G", join(",", groups))
		if appendGroups {
			modOpts = append(modOpts, "-a")
		}
		modOpts = append(modOpts, "-G", join(",", groups))
	}
	if home := getStringArg(args, "home", ""); home != "" {
		addOpts = append(addOpts, "-d", home)
		modOpts = append(modOpts, "-d", home)
	}
	if shell := getStringArg(args, "shell", ""); shell != "" {
		addOpts = append(addOpts, "-s", shell)
		modOpts = append(modOpts, "-s", shell)
	}
	if getBoolArg(args, "system", false) {
		addOpts = append(addOpts, "-r")
		modOpts = append(modOpts, "-r")
	}
	if getBoolArg(args, "create_home", true) {
		addOpts = append(addOpts, "-m")
		modOpts = append(modOpts, "-m")
	}

	// Try usermod first, then useradd
	addOptsStr := join(" ", addOpts)
	modOptsStr := join(" ", modOpts)
	addCmd := "useradd"
	modCmd := "usermod"
	if local {
		addCmd = "luseradd"
		modCmd = "lusermod"
	}
	var cmd string
	if addOptsStr == "" {
		cmd = sprintf("id %s >/dev/null 2>&1 || %s %s", name, addCmd, name)
	} else {
		cmd = sprintf("id %s >/dev/null 2>&1 && %s %s %s || %s %s %s",
			name, modCmd, modOptsStr, name, addCmd, addOptsStr, name)
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleGroup(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	local := getBoolArg(args, "local", false)
	nonUnique := getBoolArg(args, "non_unique", false)

	if name == "" {
		return core.Fail(coreerr.E("Executor.moduleGroup", "name required", nil))
	}

	if state == "absent" {
		delCmd := "groupdel"
		if local {
			delCmd = "lgroupdel"
		}
		cmd := sprintf("%s %s 2>/dev/null || true", delCmd, name)
		runBestEffort(ctx, client, cmd)
		return core.Ok(&TaskResult{Changed: true})
	}

	var opts []string
	if gid := getStringArg(args, "gid", ""); gid != "" {
		opts = append(opts, "-g", gid)
	}
	if getBoolArg(args, "system", false) {
		opts = append(opts, "-r")
	}
	if nonUnique {
		opts = append(opts, "-o")
	}

	addCmd := "groupadd"
	if local {
		addCmd = "lgroupadd"
	}

	cmd := sprintf("getent group %s >/dev/null 2>&1 || %s %s %s",
		name, addCmd, join(" ", opts), name)

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

// --- HTTP Module ---

func (e *Executor) moduleURI(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	url := getStringArg(args, "url", "")
	method := getStringArg(args, "method", "GET")
	bodyFormat := lower(getStringArg(args, "body_format", ""))
	returnContent := getBoolArg(args, "return_content", false)
	dest := getStringArg(args, "dest", "")
	timeout := getIntArg(args, "timeout", 0)
	validateCerts := getBoolArg(args, "validate_certs", true)
	urlUsername := getStringArg(args, "url_username", "")
	urlPassword := getStringArg(args, "url_password", "")
	forceBasicAuth := getBoolArg(args, "force_basic_auth", false)
	useProxy := getBoolArg(args, "use_proxy", true)
	unixSocket := getStringArg(args, "unix_socket", "")
	followRedirects := lower(getStringArg(args, "follow_redirects", "safe"))
	src := getStringArg(args, "src", "")

	if url == "" {
		return core.Fail(coreerr.E("Executor.moduleURI", "url required", nil))
	}

	var curlOpts []string
	curlOpts = append(curlOpts, "-s", "-S")
	curlOpts = append(curlOpts, "-X", method)

	// Basic auth is modelled explicitly so callers do not need to embed
	// credentials in the URL.
	if urlUsername != "" || urlPassword != "" {
		curlOpts = append(curlOpts, "-u", shellQuote(urlUsername+":"+urlPassword))
		if forceBasicAuth {
			curlOpts = append(curlOpts, "--basic")
		}
	} else if forceBasicAuth {
		curlOpts = append(curlOpts, "--basic")
	}

	if unixSocket != "" {
		curlOpts = append(curlOpts, "--unix-socket", shellQuote(unixSocket))
	}
	if !useProxy {
		curlOpts = append(curlOpts, "--noproxy", shellQuote("*"))
	}

	// Headers
	if headers, ok := args["headers"].(map[string]any); ok {
		for k, v := range headers {
			curlOpts = append(curlOpts, "-H", sprintf("%q", sprintf("%s: %v", k, v)))
		}
	}

	if !validateCerts {
		curlOpts = append(curlOpts, "-k")
	}

	curlOpts = appendURIFollowRedirects(curlOpts, method, followRedirects)

	// Body
	if src != "" {
		bodyResult := client.Download(ctx, src)
		if !bodyResult.OK {
			return wrapFailure(bodyResult, "Executor.moduleURI", "download src")
		}
		bodyBytes := bodyResult.Value.([]byte)
		curlOpts = append(curlOpts, "-d", sprintf("%q", string(bodyBytes)))
	} else if body := args["body"]; body != nil {
		bodyResult := renderURIBody(body, bodyFormat)
		if !bodyResult.OK {
			return wrapFailure(bodyResult, "Executor.moduleURI", "render body")
		}
		bodyText := bodyResult.Value.(string)
		if bodyText != "" {
			switch bodyFormat {
			case "form-multipart", "multipart", "multipart-form":
				multipartResult := renderURIBodyMultipart(body)
				if !multipartResult.OK {
					return wrapFailure(multipartResult, "Executor.moduleURI", "render multipart body")
				}
				multipartFields := multipartResult.Value.([]string)
				curlOpts = append(curlOpts, multipartFields...)
			default:
				curlOpts = append(curlOpts, "-d", sprintf("%q", bodyText))
				if !hasHeaderIgnoreCase(headersMap(args), "Content-Type") {
					switch bodyFormat {
					case "json":
						curlOpts = append(curlOpts, "-H", "\"Content-Type: application/json\"")
					case "form-urlencoded", "form_urlencoded", "form":
						curlOpts = append(curlOpts, "-H", "\"Content-Type: application/x-www-form-urlencoded\"")
					}
				}
			}
		}
	}

	if timeout > 0 {
		curlOpts = append(curlOpts, "--max-time", strconv.Itoa(timeout))
	}

	// Status code
	curlOpts = append(curlOpts, "-w", "\\n%{http_code}")

	cmd := sprintf("curl %s %q", join(" ", curlOpts), url)
	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}

	// Parse status code from last line
	lines := split(out.Stdout, "\n")
	statusCode := 0
	content := ""
	if len(lines) > 0 {
		statusText := corexTrimSpace(lines[len(lines)-1])
		parsedStatusCode, parseErr := strconv.Atoi(statusText)
		if parseErr == nil {
			statusCode = parsedStatusCode
		}
		if len(lines) > 1 {
			content = join("\n", lines[:len(lines)-1])
		}
	}

	// Check expected status codes.
	expectedStatuses := normalizeStatusCodes(args["status_code"], 200)
	failed := out.ExitCode != 0 || !containsInt(expectedStatuses, statusCode)

	data := map[string]any{"status": statusCode}
	if returnContent {
		data["content"] = content
	}

	if failed {
		return core.Ok(&TaskResult{
			Changed: false,
			Failed:  true,
			Stdout:  out.Stdout,
			Stderr:  out.Stderr,
			RC:      statusCode,
			Data:    data,
		})
	}

	if dest != "" {
		before, hasBefore := remoteFileText(ctx, client, dest)
		if !hasBefore || before != content {
			if r := client.Upload(ctx, newReader(content), dest, 0644); !r.OK {
				return wrapFailure(r, "Executor.moduleURI", "upload dest")
			}
			data["dest"] = dest
			return core.Ok(&TaskResult{
				Changed: true,
				Stdout:  out.Stdout,
				Stderr:  out.Stderr,
				RC:      statusCode,
				Data:    data,
			})
		}

		data["dest"] = dest
	}

	return core.Ok(&TaskResult{
		Changed: false,
		Stdout:  out.Stdout,
		Stderr:  out.Stderr,
		RC:      statusCode,
		Data:    data,
	})
}

func appendURIFollowRedirects(opts []string, method, followRedirects string) []string {
	if len(opts) == 0 {
		return opts
	}

	switch lower(corexTrimSpace(followRedirects)) {
	case "", "safe":
		if method == "GET" || method == "HEAD" {
			return append(opts, "-L")
		}
	case "all", "yes", "true":
		return append(opts, "-L")
	case "none", "no", "false":
		return append(opts, "--max-redirs", "0")
	case "urllib2":
		if method == "GET" || method == "HEAD" {
			return append(opts, "-L")
		}
	}

	return opts
}

func renderURIBody(body any, bodyFormat string) core.Result {
	switch bodyFormat {
	case "", "raw":
		return core.Ok(sprintf("%v", body))
	case "json":
		switch v := body.(type) {
		case string:
			return core.Ok(v)
		case []byte:
			return core.Ok(string(v))
		default:
			result := core.JSONMarshal(v)
			if !result.OK {
				return wrapFailure(result, "Executor.moduleURI", "marshal uri body")
			}
			return core.Ok(string(result.Value.([]byte)))
		}
	case "form-urlencoded", "form_urlencoded", "form":
		return core.Ok(renderURIBodyFormEncoded(body))
	default:
		return core.Ok(sprintf("%v", body))
	}
}

func renderURIBodyMultipart(body any) core.Result {
	fields := multipartBodyFields(body)
	if len(fields) == 0 {
		return core.Fail(coreerr.E("Executor.moduleURI", "multipart body requires structured data", nil))
	}

	opts := make([]string, 0, len(fields))
	for _, field := range fields {
		opts = append(opts, "-F", sprintf("%q", field))
	}

	return core.Ok(opts)
}

func multipartBodyFields(body any) []string {
	switch v := body.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fields := make([]string, 0, len(keys))
		for _, key := range keys {
			fields = append(fields, multipartFieldValues(key, v[key])...)
		}
		return fields
	case map[any]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			if s, ok := key.(string); ok {
				keys = append(keys, s)
			}
		}
		sort.Strings(keys)
		fields := make([]string, 0, len(keys))
		for _, key := range keys {
			fields = append(fields, multipartFieldValues(key, v[key])...)
		}
		return fields
	case map[string]string:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fields := make([]string, 0, len(keys))
		for _, key := range keys {
			fields = append(fields, multipartFieldValues(key, v[key])...)
		}
		return fields
	case []any:
		fields := make([]string, 0, len(v))
		for _, item := range v {
			if pair, ok := item.(map[string]any); ok {
				key := getStringArg(pair, "key", "")
				if key == "" {
					key = getStringArg(pair, "name", "")
				}
				if key != "" {
					fields = append(fields, multipartFieldValues(key, pair["value"])...)
				}
			}
		}
		return fields
	case []string:
		fields := make([]string, 0, len(v))
		for _, item := range v {
			fields = append(fields, item)
		}
		return fields
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	default:
		s := sprintf("%v", body)
		if s == "" || s == "<nil>" {
			return nil
		}
		return []string{s}
	}
}

func multipartFieldValues(key string, value any) []string {
	switch v := value.(type) {
	case nil:
		return []string{key + "="}
	case string:
		return []string{key + "=" + v}
	case []string:
		fields := make([]string, 0, len(v))
		for _, item := range v {
			fields = append(fields, key+"="+item)
		}
		return fields
	case []any:
		fields := make([]string, 0, len(v))
		for _, item := range v {
			fields = append(fields, key+"="+sprintf("%v", item))
		}
		return fields
	default:
		return []string{key + "=" + sprintf("%v", v)}
	}
}

func renderURIBodyFormEncoded(body any) string {
	values := url.Values{}

	switch v := body.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			appendFormValue(values, key, v[key])
		}
	case map[any]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			if s, ok := key.(string); ok {
				keys = append(keys, s)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			appendFormValue(values, key, v[key])
		}
	case map[string]string:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			values.Add(key, v[key])
		}
	case []any:
		for _, item := range v {
			if pair, ok := item.(map[string]any); ok {
				key := getStringArg(pair, "key", "")
				if key == "" {
					key = getStringArg(pair, "name", "")
				}
				if key != "" {
					appendFormValue(values, key, pair["value"])
				}
			}
		}
	case string:
		return v
	default:
		return sprintf("%v", body)
	}

	return values.Encode()
}

func appendFormValue(values url.Values, key string, value any) {
	switch v := value.(type) {
	case nil:
		values.Add(key, "")
	case string:
		values.Add(key, v)
	case []string:
		for _, item := range v {
			values.Add(key, item)
		}
	case []any:
		for _, item := range v {
			values.Add(key, sprintf("%v", item))
		}
	default:
		values.Add(key, sprintf("%v", v))
	}
}

func headersMap(args map[string]any) map[string]any {
	headers, _ := args["headers"].(map[string]any)
	return headers
}

func hasHeaderIgnoreCase(headers map[string]any, name string) bool {
	for key := range headers {
		if lower(key) == lower(name) {
			return true
		}
	}
	return false
}

// --- Misc Modules ---

func (e *Executor) moduleDebug(host string, task *Task, args map[string]any) core.Result {
	msg := getStringArg(args, "msg", "")
	if v, ok := args["var"]; ok {
		name := sprintf("%v", v)
		if value, ok := e.lookupConditionValue(name, host, task, nil); ok {
			msg = sprintf("%s = %v", name, value)
		} else {
			msg = sprintf("%s = <undefined>", name)
		}
	}

	return core.Ok(&TaskResult{
		Changed: false,
		Msg:     msg,
	})
}

func (e *Executor) moduleFail(args map[string]any) core.Result {
	msg := getStringArg(args, "msg", "Failed as requested")
	return core.Ok(&TaskResult{
		Failed: true,
		Msg:    msg,
	})
}

func (e *Executor) modulePing(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	data := getStringArg(args, "data", "pong")
	run := client.Run(ctx, "true")
	out := commandRunValue(run)
	if !run.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}
	if out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{
		Msg:  data,
		Data: map[string]any{"ping": data},
	})
}

func (e *Executor) moduleAssert(args map[string]any, host string) core.Result {
	that, ok := args["that"]
	if !ok {
		return core.Fail(coreerr.E("Executor.moduleAssert", "'that' required", nil))
	}

	conditions := normalizeConditions(that)
	for _, cond := range conditions {
		if !e.evalCondition(cond, host) {
			msg := getStringArg(args, "fail_msg", sprintf("Assertion failed: %s", cond))
			return core.Ok(&TaskResult{Failed: true, Msg: msg})
		}
	}

	return core.Ok(&TaskResult{Changed: false, Msg: "All assertions passed"})
}

func (e *Executor) moduleSetFact(host string, args map[string]any) core.Result {
	values := make(map[string]any, len(args))
	for k, v := range args {
		if k == "cacheable" {
			continue
		}
		values[k] = v
	}
	e.setHostVars(host, values)
	e.setHostFacts(host, values)
	return core.Ok(&TaskResult{
		Changed: true,
		Data:    map[string]any{"ansible_facts": values},
	})
}

func (e *Executor) moduleAddHost(args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	if name == "" {
		name = getStringArg(args, "hostname", "")
	}
	if name == "" {
		return core.Fail(coreerr.E("Executor.moduleAddHost", "name required", nil))
	}

	groups := normalizeStringList(args["groups"])
	if len(groups) == 0 {
		groups = normalizeStringList(args["group"])
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.inventory == nil {
		e.inventory = &Inventory{}
	}
	if e.inventory.All == nil {
		e.inventory.All = &InventoryGroup{}
	}

	host := findInventoryHost(e.inventory.All, name)
	changed := false
	if host == nil {
		host = &Host{}
		changed = true
	}
	if host.Vars == nil {
		host.Vars = make(map[string]any)
	}

	if v := getStringArg(args, "ansible_host", ""); v != "" {
		if host.AnsibleHost != v {
			changed = true
		}
		host.AnsibleHost = v
	}
	switch v := args["ansible_port"].(type) {
	case int:
		host.AnsiblePort = v
	case int8:
		host.AnsiblePort = int(v)
	case int16:
		host.AnsiblePort = int(v)
	case int32:
		host.AnsiblePort = int(v)
	case int64:
		host.AnsiblePort = int(v)
	case uint:
		host.AnsiblePort = int(v)
	case uint8:
		host.AnsiblePort = int(v)
	case uint16:
		host.AnsiblePort = int(v)
	case uint32:
		host.AnsiblePort = int(v)
	case uint64:
		host.AnsiblePort = int(v)
	case string:
		if port, err := strconv.Atoi(v); err == nil {
			if host.AnsiblePort != port {
				changed = true
			}
			host.AnsiblePort = port
		}
	}
	if v := getStringArg(args, "ansible_user", ""); v != "" {
		if host.AnsibleUser != v {
			changed = true
		}
		host.AnsibleUser = v
	}
	if v := getStringArg(args, "ansible_password", ""); v != "" {
		if host.AnsiblePassword != v {
			changed = true
		}
		host.AnsiblePassword = v
	}
	if v := getStringArg(args, "ansible_ssh_private_key_file", ""); v != "" {
		if host.AnsibleSSHPrivateKeyFile != v {
			changed = true
		}
		host.AnsibleSSHPrivateKeyFile = v
	}
	if v := getStringArg(args, "ansible_connection", ""); v != "" {
		if host.AnsibleConnection != v {
			changed = true
		}
		host.AnsibleConnection = v
	}
	if v := getStringArg(args, "ansible_become_password", ""); v != "" {
		if host.AnsibleBecomePassword != v {
			changed = true
		}
		host.AnsibleBecomePassword = v
	}

	reserved := map[string]bool{
		"name": true, "hostname": true, "groups": true, "group": true,
		"ansible_host": true, "ansible_port": true, "ansible_user": true,
		"ansible_password": true, "ansible_ssh_private_key_file": true,
		"ansible_connection": true, "ansible_become_password": true,
	}
	for key, val := range args {
		if reserved[key] {
			continue
		}
		if existing, ok := host.Vars[key]; !ok || !reflect.DeepEqual(existing, val) {
			changed = true
		}
		host.Vars[key] = val
	}

	if e.inventory.All.Hosts == nil {
		e.inventory.All.Hosts = make(map[string]*Host)
	}
	if existing, ok := e.inventory.All.Hosts[name]; !ok || existing != host {
		changed = true
	}
	e.inventory.All.Hosts[name] = host

	for _, groupName := range groups {
		if groupName == "" {
			continue
		}

		group := ensureInventoryGroup(e.inventory.All, groupName)
		if group.Hosts == nil {
			group.Hosts = make(map[string]*Host)
		}
		if existing, ok := group.Hosts[name]; !ok || existing != host {
			changed = true
		}
		group.Hosts[name] = host
	}

	msg := sprintf("host %s added", name)
	if len(groups) > 0 {
		msg += " to groups: " + join(", ", groups)
	}

	data := map[string]any{"host": name}
	if len(groups) > 0 {
		data["groups"] = groups
	}

	return core.Ok(&TaskResult{Changed: changed, Msg: msg, Data: data})
}

func (e *Executor) moduleGroupBy(host string, args map[string]any) core.Result {
	key := getStringArg(args, "key", "")
	if key == "" {
		key = getStringArg(args, "_raw_params", "")
	}
	if key == "" {
		return core.Fail(coreerr.E("Executor.moduleGroupBy", "key required", nil))
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.inventory == nil {
		e.inventory = &Inventory{}
	}
	if e.inventory.All == nil {
		e.inventory.All = &InventoryGroup{}
	}

	group := ensureInventoryGroup(e.inventory.All, key)
	if group.Hosts == nil {
		group.Hosts = make(map[string]*Host)
	}

	hostEntry := findInventoryHost(e.inventory.All, host)
	if hostEntry == nil {
		hostEntry = &Host{}
		if e.inventory.All.Hosts == nil {
			e.inventory.All.Hosts = make(map[string]*Host)
		}
		e.inventory.All.Hosts[host] = hostEntry
	}

	_, alreadyMember := group.Hosts[host]
	group.Hosts[host] = hostEntry

	msg := sprintf("host %s grouped by %s", host, key)
	return core.Ok(&TaskResult{
		Changed: !alreadyMember,
		Msg:     msg,
		Data:    map[string]any{"host": host, "group": key},
	})
}

func (e *Executor) modulePause(ctx context.Context, args map[string]any) core.Result {
	prompt := getStringArg(args, "prompt", "")
	echo := getBoolArg(args, "echo", true)

	duration := time.Duration(0)
	if s, ok := args["seconds"].(int); ok {
		duration += time.Duration(s) * time.Second
	}
	if s, ok := args["seconds"].(string); ok {
		if seconds, err := strconv.Atoi(s); err == nil {
			duration += time.Duration(seconds) * time.Second
		}
	}
	if m, ok := args["minutes"].(int); ok {
		duration += time.Duration(m) * time.Minute
	}
	if s, ok := args["minutes"].(string); ok {
		if minutes, err := strconv.Atoi(s); err == nil {
			duration += time.Duration(minutes) * time.Minute
		}
	}

	if prompt != "" {
		stdin := core.Stdin()
		statter, ok := stdin.(interface {
			Stat() (core.FsFileInfo, error)
		})
		if ok {
			if stat, err := statter.Stat(); err == nil && (stat.Mode()&core.ModeCharDevice) != 0 {
				if echo {
					if r := core.WriteString(core.Stdout(), prompt+"\n"); !r.OK {
						return r
					}
				} else {
					if r := core.WriteString(core.Stdout(), prompt); !r.OK {
						return r
					}
				}

				reader := bufio.NewReader(stdin)
				select {
				case <-ctx.Done():
					return core.Fail(ctx.Err())
				default:
					if _, readErr := reader.ReadString('\n'); readErr != nil {
						break
					}
				}
			}
		}
	}

	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return core.Fail(ctx.Err())
		case <-timer.C:
		}
	}

	result := &TaskResult{Changed: false}
	if prompt != "" {
		result.Msg = prompt
	}
	return core.Ok(result)
}

func normalizeStringList(value any) []string {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if v == "" {
			return nil
		}
		parts := corexSplit(v, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if trimmed := corexTrimSpace(part); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) == 0 && corexTrimSpace(v) != "" {
			return []string{corexTrimSpace(v)}
		}
		return out
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if trimmed := corexTrimSpace(item); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				if trimmed := corexTrimSpace(s); trimmed != "" {
					out = append(out, trimmed)
				}
			}
		}
		return out
	default:
		s := corexTrimSpace(corexSprint(v))
		if s == "" {
			return nil
		}
		return []string{s}
	}
}

// normalizeStringArgs collects one or more string values from a scalar or list
// input without splitting comma-separated content.
func normalizeStringArgs(value any) []string {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		if trimmed := corexTrimSpace(v); trimmed != "" {
			return []string{trimmed}
		}
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if trimmed := corexTrimSpace(item); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				if trimmed := corexTrimSpace(s); trimmed != "" {
					out = append(out, trimmed)
				}
				continue
			}
			s := corexTrimSpace(corexSprint(item))
			if s != "" && s != "<nil>" {
				out = append(out, s)
			}
		}
		return out
	default:
		s := corexTrimSpace(corexSprint(v))
		if s != "" && s != "<nil>" {
			return []string{s}
		}
	}
	return nil
}

func ensureInventoryGroup(parent *InventoryGroup, name string) *InventoryGroup {
	if parent == nil {
		return nil
	}
	if parent.Children == nil {
		parent.Children = make(map[string]*InventoryGroup)
	}
	if group, ok := parent.Children[name]; ok && group != nil {
		return group
	}

	group := &InventoryGroup{}
	parent.Children[name] = group
	return group
}

func findInventoryHost(group *InventoryGroup, name string) *Host {
	if group == nil {
		return nil
	}

	if host, ok := group.Hosts[name]; ok {
		return host
	}

	for _, child := range group.Children {
		if host := findInventoryHost(child, name); host != nil {
			return host
		}
	}

	return nil
}

func (e *Executor) moduleWaitFor(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	port := getIntArg(args, "port", 0)
	path := getStringArg(args, pathArgKey, "")
	host := getStringArg(args, "host", "127.0.0.1")
	state := getStringArg(args, "state", "started")
	searchRegex := getStringArg(args, "search_regex", "")
	timeoutMsg := getStringArg(args, "msg", "wait_for timed out")
	delay := getIntArg(args, "delay", 0)
	sleep := getIntArg(args, "sleep", 1)
	timeout := getIntArg(args, "timeout", 300)
	pollInterval := time.Duration(sleep) * time.Second
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}
	var compiledRegex *regexp.Regexp
	if searchRegex != "" {
		var err error
		compiledRegex, err = regexp.Compile(searchRegex)
		if err != nil {
			return core.Fail(coreerr.E("Executor.moduleWaitFor", "compile search_regex", err))
		}
	}

	if delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return core.Fail(ctx.Err())
		case <-timer.C:
		}
	}

	if path != "" {
		deadline := time.NewTimer(time.Duration(timeout) * time.Second)
		ticker := time.NewTicker(pollInterval)
		defer deadline.Stop()
		defer ticker.Stop()

		for {
			existsResult := client.FileExists(ctx, path)
			if !existsResult.OK {
				return core.Ok(&TaskResult{Failed: true, Msg: existsResult.Error()})
			}
			exists := existsResult.Value.(bool)

			satisfied := false
			switch state {
			case "absent":
				satisfied = !exists
				if exists && compiledRegex != nil {
					dataResult := client.Download(ctx, path)
					if dataResult.OK {
						data := dataResult.Value.([]byte)
						satisfied = !compiledRegex.Match(data)
					}
				}
			default:
				satisfied = exists
				if satisfied && compiledRegex != nil {
					dataResult := client.Download(ctx, path)
					if !dataResult.OK {
						satisfied = false
					} else {
						data := dataResult.Value.([]byte)
						satisfied = compiledRegex.Match(data)
					}
				}
			}
			if satisfied {
				return core.Ok(&TaskResult{Changed: false})
			}

			select {
			case <-ctx.Done():
				return core.Fail(ctx.Err())
			case <-deadline.C:
				return core.Ok(&TaskResult{Failed: true, Msg: timeoutMsg, RC: 1})
			case <-ticker.C:
			}
		}
	}

	if port > 0 {
		switch state {
		case "started", "present":
			cmd := sprintf("timeout %d bash -c 'until nc -z %s %d; do sleep %d; done'",
				timeout, host, port, sleep)
			run := client.Run(ctx, cmd)
			out := commandRunValue(run)
			if !run.OK || out.ExitCode != 0 {
				return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
			}
			return core.Ok(&TaskResult{Changed: false})
		case "stopped", "absent":
			cmd := sprintf("timeout %d bash -c 'until ! nc -z %s %d; do sleep %d; done'",
				timeout, host, port, sleep)
			run := client.Run(ctx, cmd)
			out := commandRunValue(run)
			if !run.OK || out.ExitCode != 0 {
				return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
			}
			return core.Ok(&TaskResult{Changed: false})
		case "drained":
			cmd := sprintf("timeout %d bash -c 'until ! ss -Htan state established \"( sport = :%d or dport = :%d )\" | grep -q .; do sleep %d; done'",
				timeout, port, port, sleep)
			run := client.Run(ctx, cmd)
			out := commandRunValue(run)
			if !run.OK || out.ExitCode != 0 {
				return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
			}
			return core.Ok(&TaskResult{Changed: false})
		}
	}

	return core.Ok(&TaskResult{Changed: false})
}

func (e *Executor) moduleWaitForConnection(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	timeout := getIntArg(args, "timeout", 300)
	delay := getIntArg(args, "delay", 0)
	sleep := getIntArg(args, "sleep", 1)
	connectTimeout := getIntArg(args, "connect_timeout", 5)
	timeoutMsg := getStringArg(args, "msg", "wait_for_connection timed out")

	if delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return core.Fail(ctx.Err())
		case <-timer.C:
		}
	}

	runCheck := func() (*TaskResult, bool) {
		runCtx := ctx
		if connectTimeout > 0 {
			var cancel context.CancelFunc
			runCtx, cancel = context.WithTimeout(ctx, time.Duration(connectTimeout)*time.Second)
			defer cancel()
		}

		run := client.Run(runCtx, "true")
		out := commandRunValue(run)
		if run.OK && out.ExitCode == 0 {
			return &TaskResult{Changed: false}, true
		}
		if timeout <= 0 {
			if !run.OK {
				return &TaskResult{Failed: true, Msg: resultErrorMessage(run), Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode}, true
			}
			return &TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode}, true
		}
		return &TaskResult{Stdout: out.Stdout, Stderr: out.Stderr, RC: out.ExitCode}, false
	}

	if timeout <= 0 {
		result, done := runCheck()
		if done {
			return core.Ok(result)
		}
		return core.Ok(&TaskResult{Failed: true, Msg: timeoutMsg})
	}

	deadline := time.NewTimer(time.Duration(timeout) * time.Second)
	defer deadline.Stop()

	sleepDuration := time.Duration(sleep) * time.Second
	if sleepDuration < 0 {
		sleepDuration = 0
	}

	for {
		result, done := runCheck()
		if done {
			return core.Ok(result)
		}

		select {
		case <-ctx.Done():
			return core.Fail(ctx.Err())
		case <-deadline.C:
			return core.Ok(&TaskResult{Failed: true, Msg: timeoutMsg, Stdout: result.Stdout, Stderr: result.Stderr, RC: result.RC})
		default:
		}

		if sleepDuration > 0 {
			timer := time.NewTimer(sleepDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return core.Fail(ctx.Err())
			case <-deadline.C:
				timer.Stop()
				return core.Ok(&TaskResult{Failed: true, Msg: timeoutMsg, Stdout: result.Stdout, Stderr: result.Stderr, RC: result.RC})
			case <-timer.C:
			}
		}
	}
}

func (e *Executor) moduleGit(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	repo := getStringArg(args, "repo", "")
	dest := getStringArg(args, "dest", "")
	version := getStringArg(args, "version", "HEAD")

	if repo == "" || dest == "" {
		return core.Fail(coreerr.E("Executor.moduleGit", "repo and dest required", nil))
	}

	// Check if dest exists
	existsResult := client.FileExists(ctx, dest+"/.git")
	exists, _ := existsResult.Value.(bool)

	var cmd string
	if exists {
		// Fetch and checkout (force to ensure clean state)
		cmd = sprintf("cd %q && git fetch --all && git checkout --force %q", dest, version)
	} else {
		cmd = sprintf("git clone %q %q && cd %q && git checkout %q",
			repo, dest, dest, version)
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleUnarchive(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	remote := getBoolArg(args, "remote_src", false)

	if src == "" || dest == "" {
		return core.Fail(coreerr.E("Executor.moduleUnarchive", "src and dest required", nil))
	}

	// Create dest directory (best-effort)
	runBestEffort(ctx, client, sprintf("mkdir -p %q", dest))

	var cmd string
	if !remote {
		// Upload local file first
		src = e.resolveLocalPath(src)
		data, err := coreio.Local.Read(src)
		if err != nil {
			return core.Fail(coreerr.E("Executor.moduleUnarchive", "read src", err))
		}
		tmpPath := "/tmp/ansible_unarchive_" + pathBase(src)
		if r := client.Upload(ctx, newReader(data), tmpPath, 0644); !r.OK {
			return r
		}
		src = tmpPath
		defer runBestEffort(ctx, client, sprintf("rm -f %q", tmpPath))
	}

	// Detect archive type and extract
	if hasSuffix(src, ".tar.gz") || hasSuffix(src, ".tgz") {
		cmd = sprintf("tar -xzf %q -C %q", src, dest)
	} else if hasSuffix(src, ".tar.xz") {
		cmd = sprintf("tar -xJf %q -C %q", src, dest)
	} else if hasSuffix(src, ".tar.bz2") {
		cmd = sprintf("tar -xjf %q -C %q", src, dest)
	} else if hasSuffix(src, ".tar") {
		cmd = sprintf("tar -xf %q -C %q", src, dest)
	} else if hasSuffix(src, ".zip") {
		cmd = sprintf("unzip -o %q -d %q", src, dest)
	} else {
		cmd = sprintf("tar -xf %q -C %q", src, dest) // Guess tar
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleArchive(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	dest := getStringArg(args, "dest", "")
	format := lower(getStringArg(args, "format", ""))
	paths := archivePaths(args)

	if dest == "" || len(paths) == 0 {
		return core.Fail(coreerr.E("Executor.moduleArchive", "path and dest required", nil))
	}

	// Create the parent directory first so archive creation does not fail.
	runBestEffort(ctx, client, sprintf("mkdir -p %q", pathDir(dest)))

	var cmd string
	var deleteOnSuccess bool

	switch {
	case format == "zip" || hasSuffix(dest, ".zip"):
		cmd = sprintf("zip -r %q %s", dest, join(" ", quoteArgs(paths)))
	case format == "gz" || format == "tgz" || hasSuffix(dest, ".tar.gz") || hasSuffix(dest, ".tgz"):
		cmd = sprintf("tar -czf %q %s", dest, join(" ", quoteArgs(paths)))
	case format == "bz2" || hasSuffix(dest, ".tar.bz2"):
		cmd = sprintf("tar -cjf %q %s", dest, join(" ", quoteArgs(paths)))
	case format == "xz" || hasSuffix(dest, ".tar.xz"):
		cmd = sprintf("tar -cJf %q %s", dest, join(" ", quoteArgs(paths)))
	default:
		cmd = sprintf("tar -cf %q %s", dest, join(" ", quoteArgs(paths)))
	}

	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	deleteOnSuccess = getBoolArg(args, "remove", false)
	if deleteOnSuccess {
		runBestEffort(ctx, client, sprintf("rm -rf %s", join(" ", quoteArgs(paths))))
	}

	return core.Ok(&TaskResult{Changed: true})
}

func archivePaths(args map[string]any) []string {
	raw, ok := args[pathArgKey]
	if !ok {
		raw, ok = args["paths"]
	}
	if !ok {
		return nil
	}

	switch v := raw.(type) {
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		s := sprintf("%v", v)
		if s == "" || s == "<nil>" {
			return nil
		}
		return []string{s}
	}
}

func quoteArgs(values []string) []string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, sprintf("%q", value))
	}
	return quoted
}

func prefixCommandStdin(cmd, stdin string, addNewline bool) string {
	if stdin == "" {
		return cmd
	}
	if addNewline {
		stdin += "\n"
	}
	return sprintf("printf %%s %s | %s", shellSingleQuote(stdin), cmd)
}

func shellSingleQuote(value string) string {
	return "'" + replaceAll(value, "'", `'"'"'`) + "'"
}

// --- Helpers ---

func getStringArg(args map[string]any, key, def string) string {
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return sprintf("%v", v)
	}
	return def
}

func getBoolArg(args map[string]any, key string, def bool) bool {
	if v, ok := args[key]; ok {
		switch b := v.(type) {
		case bool:
			return b
		case string:
			lowered := lower(b)
			return lowered == "true" || lowered == "yes" || lowered == "1"
		}
	}
	return def
}

func getIntArg(args map[string]any, key string, def int) int {
	if v, ok := args[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int8:
			return int(n)
		case int16:
			return int(n)
		case int32:
			return int(n)
		case int64:
			return int(n)
		case uint:
			return int(n)
		case uint8:
			return int(n)
		case uint16:
			return int(n)
		case uint32:
			return int(n)
		case uint64:
			return int(n)
		case string:
			if parsed, err := strconv.Atoi(n); err == nil {
				return parsed
			}
		}
	}
	return def
}

func normalizeStatusCodes(value any, def int) []int {
	switch v := value.(type) {
	case nil:
		return []int{def}
	case int:
		return []int{v}
	case int8:
		return []int{int(v)}
	case int16:
		return []int{int(v)}
	case int32:
		return []int{int(v)}
	case int64:
		return []int{int(v)}
	case uint:
		return []int{int(v)}
	case uint8:
		return []int{int(v)}
	case uint16:
		return []int{int(v)}
	case uint32:
		return []int{int(v)}
	case uint64:
		return []int{int(v)}
	case string:
		if parsed, err := strconv.Atoi(v); err == nil {
			return []int{parsed}
		}
	case []int:
		return v
	case []any:
		out := make([]int, 0, len(v))
		for _, item := range v {
			out = append(out, normalizeStatusCodes(item, def)...)
		}
		if len(out) > 0 {
			return out
		}
	case []string:
		out := make([]int, 0, len(v))
		for _, item := range v {
			if parsed, err := strconv.Atoi(item); err == nil {
				out = append(out, parsed)
			}
		}
		if len(out) > 0 {
			return out
		}
	}

	return []int{def}
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// --- Additional Modules ---

func (e *Executor) moduleHostname(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	if name == "" {
		name = getStringArg(args, "hostname", "")
	}
	if name == "" {
		return core.Fail(coreerr.E("Executor.moduleHostname", "name required", nil))
	}

	currentRun := client.Run(ctx, "hostname")
	currentOut := commandRunValue(currentRun)
	if currentRun.OK && currentOut.ExitCode == 0 && corexTrimSpace(currentOut.Stdout) == name {
		return core.Ok(&TaskResult{Changed: false, Msg: "hostname already set"})
	}

	// Set hostname
	cmd := sprintf("hostnamectl set-hostname %q || hostname %q", name, name)
	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	// Update /etc/hosts if needed (best-effort)
	runBestEffort(ctx, client, sprintf("sed -i 's/127.0.1.1.*/127.0.1.1\t%s/' /etc/hosts", name))

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleSysctl(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	value := getStringArg(args, "value", "")
	state := getStringArg(args, "state", "present")
	reload := getBoolArg(args, "reload", false)
	ignoreErrors := getBoolArg(args, "ignoreerrors", false)
	sysctlFile := getStringArg(args, "sysctl_file", "/etc/sysctl.conf")
	escapedName := regexp.QuoteMeta(name)
	sysctlFlags := ""
	if ignoreErrors {
		sysctlFlags = " -e"
	}

	if name == "" {
		return core.Fail(coreerr.E("Executor.moduleSysctl", "name required", nil))
	}

	if state == "absent" {
		// Remove from the configured sysctl file.
		cmd := sprintf("sed -i '/%s/d' %q", escapedName, sysctlFile)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
		}

		if reload {
			run = client.Run(ctx, "sysctl"+sysctlFlags+" -p")
			out = commandRunValue(run)
			if !run.OK || out.ExitCode != 0 {
				return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
			}
		}
		return core.Ok(&TaskResult{Changed: true})
	}

	// Set value
	cmd := sprintf("sysctl%s -w %s=%s", sysctlFlags, name, value)
	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	// Persist if requested (best-effort)
	if getBoolArg(args, "sysctl_set", true) {
		cmd = sprintf("grep -q '^%s' %q && sed -i 's/^%s.*/%s=%s/' %q || echo '%s=%s' >> %q",
			escapedName, sysctlFile, escapedName, name, value, sysctlFile, name, value, sysctlFile)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
		}
	}

	if reload {
		run := client.Run(ctx, "sysctl"+sysctlFlags+" -p")
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
		}
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleCron(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	name := getStringArg(args, "name", "")
	job := getStringArg(args, "job", "")
	state := getStringArg(args, "state", "present")
	user := getStringArg(args, "user", "root")
	disabled := getBoolArg(args, "disabled", false)
	specialTime := getStringArg(args, "special_time", "")
	backup := getBoolArg(args, "backup", false)

	minute := getStringArg(args, "minute", "*")
	hour := getStringArg(args, "hour", "*")
	day := getStringArg(args, "day", "*")
	month := getStringArg(args, "month", "*")
	weekday := getStringArg(args, "weekday", "*")

	var backupPath string
	if backup {
		backupResult := backupCronTab(ctx, client, user, name)
		if !backupResult.OK {
			return backupResult
		}
		backupPath = backupResult.Value.(string)
	}

	if state == "absent" {
		if name != "" {
			// Remove by name (comment marker)
			cmd := sprintf("crontab -u %s -l 2>/dev/null | grep -v '# %s' | grep -v '%s' | crontab -u %s -",
				user, name, job, user)
			runBestEffort(ctx, client, cmd)
		}
		result := &TaskResult{Changed: true}
		if backupPath != "" {
			result.Data = map[string]any{"backup_file": backupPath}
		}
		return core.Ok(result)
	}

	// Build cron entry
	schedule := sprintf("%s %s %s %s %s", minute, hour, day, month, weekday)
	if specialTime != "" {
		schedule = "@" + specialTime
	}
	entry := sprintf("%s %s # %s", schedule, job, name)
	if disabled {
		entry = "# " + entry
	}

	// Add to crontab
	cmd := sprintf("(crontab -u %s -l 2>/dev/null | grep -v '# %s' ; echo %q) | crontab -u %s -",
		user, name, entry, user)
	run := client.Run(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	result := &TaskResult{Changed: true}
	if backupPath != "" {
		result.Data = map[string]any{"backup_file": backupPath}
	}
	return core.Ok(result)
}

func (e *Executor) moduleBlockinfile(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	path := getStringArg(args, pathArgKey, "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return core.Fail(coreerr.E("Executor.moduleBlockinfile", "path required", nil))
	}

	before, _ := remoteFileText(ctx, client, path)

	block := getStringArg(args, "block", "")
	marker := getStringArg(args, "marker", "# {mark} ANSIBLE MANAGED BLOCK")
	markerBegin := getStringArg(args, "marker_begin", "BEGIN")
	markerEnd := getStringArg(args, "marker_end", "END")
	state := getStringArg(args, "state", "present")
	create := getBoolArg(args, "create", false)
	backup := getBoolArg(args, "backup", false)
	prependNewline := getBoolArg(args, "prepend_newline", false)
	appendNewline := getBoolArg(args, "append_newline", false)

	beginMarker := renderBlockinfileMarker(marker, markerBegin)
	endMarker := renderBlockinfileMarker(marker, markerEnd)

	var backupPath string
	if backup {
		backupResult := backupRemoteFile(ctx, client, path)
		backupData, _ := backupResult.Value.(backupRemoteFileResult)
		backupPath = backupData.Path
		if !backupData.HadBefore {
			backupPath = ""
		}
	}

	if state == "absent" {
		// Remove block
		cmd := sprintf("sed -i '/%s/,/%s/d' %q",
			replaceAll(beginMarker, "/", "\\/"),
			replaceAll(endMarker, "/", "\\/"),
			path)
		run := client.Run(ctx, cmd)
		out := commandRunValue(run)
		if !run.OK || out.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
		}

		result := &TaskResult{Changed: true}
		if backupPath != "" {
			result.Data = map[string]any{"backup_file": backupPath}
		}
		if e.Diff {
			if after, ok := remoteFileText(ctx, client, path); ok && before != after {
				result.Data = ensureTaskResultData(result.Data)
				result.Data["diff"] = fileDiffData(path, before, after)
			}
		}
		return core.Ok(result)
	}

	// Create file if needed (best-effort)
	if create {
		runBestEffort(ctx, client, sprintf("touch %q", path))
	}

	// Remove existing block and add new one
	escapedBlock := replaceAll(block, "'", "'\\''")
	blockContent := beginMarker + "\n" + escapedBlock + "\n" + endMarker
	if prependNewline {
		blockContent = "\n" + blockContent
	}
	if appendNewline {
		blockContent += "\n"
	}
	cmd := sprintf(`
sed -i '/%s/,/%s/d' %q 2>/dev/null || true
cat >> %q << 'BLOCK_EOF'
%s
BLOCK_EOF
`, replaceAll(beginMarker, "/", "\\/"),
		replaceAll(endMarker, "/", "\\/"),
		path, path, blockContent)

	run := client.RunScript(ctx, cmd)
	out := commandRunValue(run)
	if !run.OK || out.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: out.Stderr, Stdout: out.Stdout, RC: out.ExitCode})
	}

	result := &TaskResult{Changed: true}
	if backupPath != "" {
		result.Data = map[string]any{"backup_file": backupPath}
	}
	if e.Diff {
		if after, ok := remoteFileText(ctx, client, path); ok && before != after {
			if result.Data == nil {
				result.Data = make(map[string]any)
			}
			result.Data["diff"] = fileDiffData(path, before, after)
		}
	}

	return core.Ok(result)
}

func renderBlockinfileMarker(marker, mark string) string {
	if marker == "" {
		marker = "# {mark} ANSIBLE MANAGED BLOCK"
	}
	if mark == "" {
		mark = "BEGIN"
	}
	return replaceN(marker, "{mark}", mark, 1)
}

func (e *Executor) moduleIncludeVars(args map[string]any) core.Result {
	file := getStringArg(args, "file", "")
	if file == "" {
		file = getStringArg(args, "_raw_params", "")
	}
	dir := getStringArg(args, "dir", "")
	name := getStringArg(args, "name", "")
	filesMatching := getStringArg(args, "files_matching", "")
	ignoreFiles := normalizeStringList(args["ignore_files"])
	extensions := normalizeIncludeVarsExtensions(args["extensions"])
	hashBehaviour := lower(getStringArg(args, "hash_behaviour", "replace"))
	depth := getIntArg(args, "depth", 0)

	if file == "" && dir == "" {
		return core.Ok(&TaskResult{Changed: false})
	}

	loaded := make(map[string]any)
	var sources []string
	loadFile := func(path string) core.Result {
		data, err := coreio.Local.Read(path)
		if err != nil {
			return core.Fail(coreerr.E("Executor.moduleIncludeVars", "read vars file", err))
		}

		var vars map[string]any
		if err := yaml.Unmarshal([]byte(data), &vars); err != nil {
			return core.Fail(coreerr.E("Executor.moduleIncludeVars", "parse vars file", err))
		}

		mergeVars(loaded, vars, hashBehaviour == "merge")
		return core.Ok(nil)
	}

	if file != "" {
		file = e.resolveLocalPath(file)
		sources = append(sources, file)
		if r := loadFile(file); !r.OK {
			return r
		}
	}

	if dir != "" {
		dir = e.resolveLocalPath(dir)
		filesResult := collectIncludeVarsFiles(dir, depth, filesMatching, extensions, ignoreFiles)
		if !filesResult.OK {
			return filesResult
		}
		files := filesResult.Value.([]string)

		for _, path := range files {
			sources = append(sources, path)
			if r := loadFile(path); !r.OK {
				return r
			}
		}
	}

	if name != "" {
		e.vars[name] = loaded
	} else {
		mergeVars(e.vars, loaded, hashBehaviour == "merge")
	}

	msg := "include_vars"
	if len(sources) > 0 {
		msg += ": " + join(", ", sources)
	}

	result := &TaskResult{Changed: true, Msg: msg}
	if len(sources) > 0 {
		result.Data = map[string]any{
			"ansible_included_var_files": append([]string(nil), sources...),
		}
	}

	return core.Ok(result)
}

func normalizeIncludeVarsExtensions(value any) []string {
	switch v := value.(type) {
	case nil:
		return []string{".json", ".yml", ".yaml"}
	case string:
		return normalizeIncludeVarsExtensionList([]string{v})
	case []string:
		return normalizeIncludeVarsExtensionList(v)
	case []any:
		values := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				values = append(values, s)
			}
		}
		return normalizeIncludeVarsExtensionList(values)
	default:
		return normalizeIncludeVarsExtensionList([]string{corexSprint(v)})
	}
}

func normalizeIncludeVarsExtensionList(values []string) []string {
	if len(values) == 0 {
		return []string{".json", ".yml", ".yaml"}
	}

	extensions := make([]string, 0, len(values))
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		trimmed := corexTrimSpace(value)
		ext := lower(trimmed)
		if trimmed == "" {
			ext = ""
		} else if ext == "" {
			continue
		}
		if ext != "" && !corexHasPrefix(ext, ".") {
			ext = "." + ext
		}
		if seen[ext] {
			continue
		}
		seen[ext] = true
		extensions = append(extensions, ext)
	}
	return extensions
}

func collectIncludeVarsFiles(dir string, depth int, filesMatching string, extensions []string, ignoreFiles []string) core.Result {
	stat := core.Stat(dir)
	if !stat.OK {
		return wrapFailure(stat, "Executor.moduleIncludeVars", "read vars dir")
	}
	info := stat.Value.(core.FsFileInfo)
	if !info.IsDir() {
		return core.Fail(coreerr.E("Executor.moduleIncludeVars", "read vars dir: not a directory", nil))
	}

	type dirEntry struct {
		path  string
		depth int
	}

	var matcher *regexp.Regexp
	if filesMatching != "" {
		compiled, compileErr := regexp.Compile(filesMatching)
		if compileErr != nil {
			return core.Fail(coreerr.E("Executor.moduleIncludeVars", "compile files_matching", compileErr))
		}
		matcher = compiled
	}

	var files []string
	allowed := make(map[string]bool, len(extensions))
	for _, ext := range extensions {
		allowed[ext] = true
	}
	ignored := make(map[string]bool, len(ignoreFiles))
	for _, name := range ignoreFiles {
		if name = corexTrimSpace(name); name != "" {
			ignored[name] = true
		}
	}
	stack := []dirEntry{{path: dir, depth: 0}}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		entriesResult := core.ReadDir(core.DirFS(current.path), ".")
		if !entriesResult.OK {
			return wrapFailure(entriesResult, "Executor.moduleIncludeVars", "read vars dir")
		}
		entries := entriesResult.Value.([]core.FsDirEntry)

		for i := len(entries) - 1; i >= 0; i-- {
			entry := entries[i]
			fullPath := joinPath(current.path, entry.Name())

			if entry.IsDir() {
				if depth == 0 || current.depth < depth {
					stack = append(stack, dirEntry{path: fullPath, depth: current.depth + 1})
				}
				continue
			}

			if ignored[entry.Name()] {
				continue
			}
			ext := lower(core.PathExt(entry.Name()))
			if !allowed[ext] {
				continue
			}
			if matcher != nil && !matcher.MatchString(entry.Name()) {
				continue
			}
			files = append(files, fullPath)
		}
	}

	sort.Strings(files)
	return core.Ok(files)
}

func mergeVars(dst, src map[string]any, mergeMaps bool) {
	if dst == nil || src == nil {
		return
	}

	for key, val := range src {
		if !mergeMaps {
			dst[key] = val
			continue
		}

		if existing, ok := dst[key].(map[string]any); ok {
			if next, ok := val.(map[string]any); ok {
				mergeVars(existing, next, true)
				continue
			}
		}

		dst[key] = val
	}
}

func (e *Executor) moduleMeta(args map[string]any) core.Result {
	// meta module controls play execution
	// Most actions are no-ops for us, but we preserve the requested action so
	// the executor can apply side effects such as handler flushing.
	action := getStringArg(args, "_raw_params", "")
	if action == "" {
		action = getStringArg(args, "free_form", "")
	}
	if action == "" {
		action = getStringArg(args, "action", "")
	}

	result := &TaskResult{Changed: action == "clear_facts"}
	if action != "" {
		result.Data = map[string]any{"action": action}
	}

	return core.Ok(result)
}

func (e *Executor) moduleSetup(ctx context.Context, host string, client sshFactsRunner, args map[string]any) core.Result {
	gatherTimeout := getIntArg(args, "gather_timeout", 0)
	if gatherTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(gatherTimeout)*time.Second)
		defer cancel()
	}

	gatherSubset := normalizeStringList(args["gather_subset"])
	includeVirtual := containsString(gatherSubset, "all") || containsString(gatherSubset, "virtual")

	factsResult := e.collectFacts(ctx, client, includeVirtual)
	if !factsResult.OK {
		return core.Ok(&TaskResult{Failed: true, Msg: factsResult.Error()})
	}
	facts := factsResult.Value.(*Facts)

	factMap := factsToMap(facts)
	factMap = applyGatherSubsetFilter(factMap, gatherSubset)
	filteredFactMap := filterFactsMap(factMap, normalizeStringList(args["filter"]))
	filteredFacts := factsFromMap(filteredFactMap)

	e.mu.Lock()
	e.facts[host] = filteredFacts
	e.mu.Unlock()

	return core.Ok(&TaskResult{
		Changed: false,
		Msg:     "facts gathered",
		Data:    map[string]any{"ansible_facts": filteredFactMap},
	})
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if lower(corexTrimSpace(value)) == target {
			return true
		}
	}
	return false
}

func applyGatherSubsetFilter(facts map[string]any, subsets []string) map[string]any {
	if len(facts) == 0 || len(subsets) == 0 {
		return facts
	}

	normalized := make([]string, 0, len(subsets))
	for _, subset := range subsets {
		if trimmed := lower(corexTrimSpace(subset)); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	if len(normalized) == 0 {
		return facts
	}

	includeAll := false
	excludeAll := false
	excludeMin := false
	positives := make([]string, 0, len(normalized))
	exclusions := make([]string, 0, len(normalized))
	for _, subset := range normalized {
		if corexHasPrefix(subset, "!") {
			name := corexTrimPrefix(subset, "!")
			if name != "" {
				exclusions = append(exclusions, name)
			}
			switch name {
			case "all":
				excludeAll = true
			case "min":
				excludeMin = true
			}
			continue
		}

		positives = append(positives, subset)
		switch subset {
		case "all":
			includeAll = true
		case "min":
			// handled below
		}
	}

	if includeAll && !excludeAll {
		return facts
	}

	selected := make(map[string]bool)
	if len(positives) == 0 {
		if !excludeAll {
			for key := range facts {
				selected[key] = true
			}
		} else if !excludeMin {
			addSubsetKeys(selected, "min")
		}
	} else {
		if !excludeMin {
			addSubsetKeys(selected, "min")
		}
	}

	for _, subset := range positives {
		addSubsetKeys(selected, subset)
	}
	for _, subset := range exclusions {
		removeSubsetKeys(selected, subset)
	}

	if len(selected) == 0 {
		return map[string]any{}
	}

	filtered := make(map[string]any)
	for key, value := range facts {
		if selected[key] {
			filtered[key] = value
		}
	}

	return filtered
}

func addSubsetKeys(selected map[string]bool, subset string) {
	for _, key := range gatherSubsetKeys(subset) {
		selected[key] = true
	}
}

func removeSubsetKeys(selected map[string]bool, subset string) {
	if subset == "all" {
		return
	}
	for _, key := range gatherSubsetKeys(subset) {
		delete(selected, key)
	}
	delete(selected, subset)
}

func gatherSubsetKeys(subset string) []string {
	switch subset {
	case "all":
		return []string{
			"ansible_hostname",
			"ansible_fqdn",
			"ansible_os_family",
			"ansible_distribution",
			"ansible_distribution_version",
			"ansible_architecture",
			"ansible_kernel",
			"ansible_memtotal_mb",
			"ansible_processor_vcpus",
			"ansible_default_ipv4_address",
		}
	case "min":
		return []string{
			"ansible_hostname",
			"ansible_fqdn",
			"ansible_os_family",
			"ansible_distribution",
			"ansible_distribution_version",
			"ansible_architecture",
			"ansible_kernel",
		}
	case "hardware":
		return []string{
			"ansible_architecture",
			"ansible_kernel",
			"ansible_memtotal_mb",
			"ansible_processor_vcpus",
		}
	case "network":
		return []string{
			"ansible_default_ipv4_address",
		}
	case "distribution":
		return []string{
			"ansible_os_family",
			"ansible_distribution",
			"ansible_distribution_version",
		}
	case "virtual":
		return []string{
			"ansible_virtualization_role",
			"ansible_virtualization_type",
		}
	default:
		return nil
	}
}

func (e *Executor) collectFacts(ctx context.Context, client sshFactsRunner, includeVirtual bool) core.Result {
	facts := &Facts{}
	read := func(cmd string) core.Result {
		run := client.Run(ctx, cmd)
		if !run.OK {
			if ctx.Err() != nil {
				return core.Fail(ctx.Err())
			}
			return core.Ok("")
		}
		return core.Ok(commandRunValue(run).Stdout)
	}

	stdoutResult := read("hostname -f 2>/dev/null || hostname")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout := stdoutResult.Value.(string)
	if stdout != "" {
		facts.FQDN = corexTrimSpace(stdout)
	}

	stdoutResult = read("hostname -s 2>/dev/null || hostname")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		facts.Hostname = corexTrimSpace(stdout)
	}

	stdoutResult = read("cat /etc/os-release 2>/dev/null | grep -E '^(ID|VERSION_ID|NAME)=' | head -3")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		for _, line := range split(stdout, "\n") {
			switch {
			case corexHasPrefix(line, "ID="):
				id := trimCutset(corexTrimPrefix(line, "ID="), "\"'")
				if facts.Distribution == "" {
					facts.Distribution = id
				}
				if facts.OS == "" {
					facts.OS = osFamilyFromReleaseID(id)
				}
			case corexHasPrefix(line, "NAME="):
				name := trimCutset(corexTrimPrefix(line, "NAME="), "\"'")
				if facts.OS == "" {
					facts.OS = osFamilyFromReleaseID(name)
				}
			case corexHasPrefix(line, "VERSION_ID="):
				facts.Version = trimCutset(corexTrimPrefix(line, "VERSION_ID="), "\"'")
			}
		}
	}

	stdoutResult = read("uname -m")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		facts.Architecture = corexTrimSpace(stdout)
	}

	stdoutResult = read("uname -r")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		facts.Kernel = corexTrimSpace(stdout)
	}

	stdoutResult = read("nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		if n, parseErr := strconv.Atoi(corexTrimSpace(stdout)); parseErr == nil {
			facts.CPUs = n
		}
	}

	stdoutResult = read("free -m 2>/dev/null | awk '/^Mem:/ {print $2}'")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		if n, parseErr := strconv.ParseInt(corexTrimSpace(stdout), 10, 64); parseErr == nil {
			facts.Memory = n
		}
	}

	stdoutResult = read("hostname -I 2>/dev/null | awk '{print $1}'")
	if !stdoutResult.OK {
		return stdoutResult
	}
	stdout = stdoutResult.Value.(string)
	if stdout != "" {
		facts.IPv4 = corexTrimSpace(stdout)
	}

	if includeVirtual {
		stdoutResult = read("systemd-detect-virt 2>/dev/null")
		if !stdoutResult.OK {
			return stdoutResult
		}
		stdout = stdoutResult.Value.(string)
		virtType := corexTrimSpace(stdout)
		if virtType == "" || virtType == "none" {
			facts.VirtualizationRole = "host"
			facts.VirtualizationType = "none"
		} else {
			facts.VirtualizationRole = "guest"
			facts.VirtualizationType = virtType
		}
	}

	return core.Ok(facts)
}

func factsToMap(facts *Facts) map[string]any {
	if facts == nil {
		return nil
	}

	return map[string]any{
		"ansible_hostname":             facts.Hostname,
		"ansible_fqdn":                 facts.FQDN,
		"ansible_os_family":            facts.OS,
		"ansible_distribution":         facts.Distribution,
		"ansible_distribution_version": facts.Version,
		"ansible_architecture":         facts.Architecture,
		"ansible_kernel":               facts.Kernel,
		"ansible_virtualization_role":  facts.VirtualizationRole,
		"ansible_virtualization_type":  facts.VirtualizationType,
		"ansible_memtotal_mb":          facts.Memory,
		"ansible_processor_vcpus":      facts.CPUs,
		"ansible_default_ipv4_address": facts.IPv4,
	}
}

func filterFactsMap(facts map[string]any, patterns []string) map[string]any {
	if len(facts) == 0 || len(patterns) == 0 {
		return facts
	}

	filtered := make(map[string]any)
	for key, value := range facts {
		for _, pattern := range patterns {
			matchResult := pathMatch(pattern, key)
			matched, _ := matchResult.Value.(bool)
			if !matchResult.OK {
				matched = pattern == key
			}
			if matched {
				filtered[key] = value
				break
			}
		}
	}

	return filtered
}

func factsFromMap(values map[string]any) *Facts {
	if len(values) == 0 {
		return &Facts{}
	}

	facts := &Facts{}
	if v, ok := values["ansible_hostname"].(string); ok {
		facts.Hostname = v
	}
	if v, ok := values["ansible_fqdn"].(string); ok {
		facts.FQDN = v
	}
	if v, ok := values["ansible_os_family"].(string); ok {
		facts.OS = v
	}
	if v, ok := values["ansible_distribution"].(string); ok {
		facts.Distribution = v
	}
	if v, ok := values["ansible_distribution_version"].(string); ok {
		facts.Version = v
	}
	if v, ok := values["ansible_architecture"].(string); ok {
		facts.Architecture = v
	}
	if v, ok := values["ansible_kernel"].(string); ok {
		facts.Kernel = v
	}
	if v, ok := values["ansible_virtualization_role"].(string); ok {
		facts.VirtualizationRole = v
	}
	if v, ok := values["ansible_virtualization_type"].(string); ok {
		facts.VirtualizationType = v
	}
	if v, ok := values["ansible_memtotal_mb"].(int64); ok {
		facts.Memory = v
	}
	if v, ok := values["ansible_memtotal_mb"].(int); ok {
		facts.Memory = int64(v)
	}
	if v, ok := values["ansible_processor_vcpus"].(int); ok {
		facts.CPUs = v
	}
	if v, ok := values["ansible_processor_vcpus"].(int64); ok {
		facts.CPUs = int(v)
	}
	if v, ok := values["ansible_default_ipv4_address"].(string); ok {
		facts.IPv4 = v
	}

	return facts
}

func osFamilyFromReleaseID(id string) string {
	switch lower(corexTrimSpace(id)) {
	case "debian", "ubuntu":
		return "Debian"
	case "rhel", "redhat", "centos", "fedora", "rocky", "almalinux", "oracle":
		return "RedHat"
	case "arch", "manjaro":
		return "Archlinux"
	case "alpine":
		return "Alpine"
	default:
		return ""
	}
}

func (e *Executor) moduleReboot(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	preRebootDelay := getIntArg(args, "pre_reboot_delay", 0)
	postRebootDelay := getIntArg(args, "post_reboot_delay", 0)
	rebootTimeout := getIntArg(args, "reboot_timeout", 600)
	testCommand := getStringArg(args, "test_command", "whoami")
	rebootCommand := getStringArg(args, "reboot_command", "")

	msg := getStringArg(args, "msg", "Reboot initiated by Ansible")
	runReboot := func(cmd string) core.Result {
		runResult := client.Run(ctx, cmd)
		run := commandRunValue(runResult)
		if !runResult.OK || run.ExitCode != 0 {
			msg := run.Stderr
			if msg == "" && !runResult.OK {
				msg = resultErrorMessage(runResult)
			}
			return core.Ok(&TaskResult{Failed: true, Msg: msg, Stdout: run.Stdout, Stderr: run.Stderr, RC: run.ExitCode})
		}
		return core.Ok(nil)
	}

	if rebootCommand != "" {
		if preRebootDelay > 0 {
			if result := runReboot(sprintf("sleep %d && %s", preRebootDelay, rebootCommand)); !result.OK || result.Value != nil {
				return result
			}
		} else {
			if result := runReboot(rebootCommand); !result.OK || result.Value != nil {
				return result
			}
		}
	} else if preRebootDelay > 0 {
		cmd := sprintf("sleep %d && shutdown -r now '%s' &", preRebootDelay, msg)
		if result := runReboot(cmd); !result.OK || result.Value != nil {
			return result
		}
	} else {
		if result := runReboot(sprintf("shutdown -r now '%s' &", msg)); !result.OK || result.Value != nil {
			return result
		}
	}

	if postRebootDelay > 0 {
		runResult := client.Run(ctx, sprintf("sleep %d", postRebootDelay))
		run := commandRunValue(runResult)
		if !runResult.OK || run.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: run.Stderr, Stdout: run.Stdout, RC: run.ExitCode})
		}
	}

	if testCommand == "" {
		return core.Ok(&TaskResult{Changed: true, Msg: "Reboot initiated"})
	}

	deadline := time.NewTimer(time.Duration(rebootTimeout) * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer deadline.Stop()
	defer ticker.Stop()

	var lastStdout, lastStderr string
	var lastRC int
	for {
		runResult := client.Run(ctx, testCommand)
		run := commandRunValue(runResult)
		lastStdout = run.Stdout
		lastStderr = run.Stderr
		lastRC = run.ExitCode
		if runResult.OK && run.ExitCode == 0 {
			break
		}

		select {
		case <-ctx.Done():
			return core.Fail(ctx.Err())
		case <-deadline.C:
			return core.Ok(&TaskResult{
				Failed: true,
				Msg:    "reboot timed out waiting for host to become ready",
				Stdout: lastStdout,
				Stderr: lastStderr,
				RC:     lastRC,
			})
		case <-ticker.C:
		}
	}

	return core.Ok(&TaskResult{Changed: true, Msg: "Reboot initiated"})
}

func (e *Executor) moduleUFW(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	rule := getStringArg(args, "rule", "")
	port := getStringArg(args, "port", "")
	proto := getStringArg(args, "proto", "tcp")
	state := getStringArg(args, "state", "")
	logging := getStringArg(args, "logging", "")
	deleteRule := getBoolArg(args, "delete", false)

	var cmd string

	// Handle logging configuration.
	if logging != "" {
		cmd = sprintf("ufw logging %s", logging)
		runResult := client.Run(ctx, cmd)
		run := commandRunValue(runResult)
		if !runResult.OK || run.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: run.Stderr, Stdout: run.Stdout, RC: run.ExitCode})
		}
		return core.Ok(&TaskResult{Changed: true})
	}

	// Handle state (enable/disable)
	if state != "" {
		switch state {
		case "enabled":
			cmd = "ufw --force enable"
		case "disabled":
			cmd = "ufw disable"
		case "reloaded":
			cmd = "ufw reload"
		case "reset":
			cmd = "ufw --force reset"
		}
		if cmd != "" {
			runResult := client.Run(ctx, cmd)
			run := commandRunValue(runResult)
			if !runResult.OK || run.ExitCode != 0 {
				return core.Ok(&TaskResult{Failed: true, Msg: run.Stderr, Stdout: run.Stdout, RC: run.ExitCode})
			}
			return core.Ok(&TaskResult{Changed: true})
		}
	}

	// Handle rule
	if rule != "" && port != "" {
		switch rule {
		case "allow":
			cmd = sprintf("ufw allow %s/%s", port, proto)
		case "deny":
			cmd = sprintf("ufw deny %s/%s", port, proto)
		case "reject":
			cmd = sprintf("ufw reject %s/%s", port, proto)
		case "limit":
			cmd = sprintf("ufw limit %s/%s", port, proto)
		}
		if deleteRule && cmd != "" {
			cmd = "ufw delete " + corexTrimPrefix(cmd, "ufw ")
		}

		runResult := client.Run(ctx, cmd)
		run := commandRunValue(runResult)
		if !runResult.OK || run.ExitCode != 0 {
			return core.Ok(&TaskResult{Failed: true, Msg: run.Stderr, Stdout: run.Stdout, RC: run.ExitCode})
		}
	}

	return core.Ok(&TaskResult{Changed: true})
}

func (e *Executor) moduleAuthorizedKey(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	user := getStringArg(args, "user", "")
	key := getStringArg(args, "key", "")
	state := getStringArg(args, "state", "present")
	exclusive := getBoolArg(args, "exclusive", false)
	manageDir := getBoolArg(args, "manage_dir", true)
	pathArg := getStringArg(args, pathArgKey, "")
	keyOptions := getStringArg(args, "key_options", "")
	comment := getStringArg(args, "comment", "")

	if user == "" || key == "" {
		return core.Fail(coreerr.E("Executor.moduleAuthorizedKey", "user and key required", nil))
	}

	// Get user's home directory
	homeResult := client.Run(ctx, sprintf("getent passwd %s | cut -d: -f6", user))
	if !homeResult.OK {
		return wrapFailure(homeResult, "Executor.moduleAuthorizedKey", "get home dir")
	}
	home := corexTrimSpace(commandRunValue(homeResult).Stdout)
	if home == "" {
		home = "/root"
		if user != "root" {
			home = "/home/" + user
		}
	}

	authKeysPath := pathArg
	if authKeysPath == "" {
		authKeysPath = joinPath(home, ".ssh", "authorized_keys")
	} else if corexHasPrefix(authKeysPath, "~/") {
		authKeysPath = joinPath(home, corexTrimPrefix(authKeysPath, "~/"))
	} else if authKeysPath == "~" {
		authKeysPath = home
	}
	if authKeysPath == "" {
		authKeysPath = joinPath(home, ".ssh", "authorized_keys")
	}

	line := authorizedKeyLine(key, keyOptions, comment)
	base := authorizedKeyBase(line)

	if state == "absent" {
		content, ok := remoteFileText(ctx, client, authKeysPath)
		if !ok || !authorizedKeyContainsBase(content, base) {
			return core.Ok(&TaskResult{Changed: false})
		}

		updated, changed := rewriteAuthorizedKeyContent(content, base, "")
		if !changed {
			return core.Ok(&TaskResult{Changed: false})
		}
		if uploadResult := client.Upload(ctx, newReader(updated), authKeysPath, 0600); !uploadResult.OK {
			return wrapFailure(uploadResult, "Executor.moduleAuthorizedKey", "upload authorised keys")
		}
		return core.Ok(&TaskResult{Changed: true})
	}

	if content, ok := remoteFileText(ctx, client, authKeysPath); ok {
		updated, changed := rewriteAuthorizedKeyContent(content, base, line)
		if !changed {
			return core.Ok(&TaskResult{Changed: false, Msg: sprintf("already up to date: %s", authKeysPath)})
		}
		if uploadResult := client.Upload(ctx, newReader(updated), authKeysPath, 0600); !uploadResult.OK {
			return wrapFailure(uploadResult, "Executor.moduleAuthorizedKey", "upload authorised keys")
		}
		chmodResult := client.Run(ctx, sprintf("chmod 600 %q && chown %s:%s %q",
			authKeysPath, user, user, authKeysPath))
		if !chmodResult.OK {
			return core.Ok(&TaskResult{Changed: true})
		}
		return core.Ok(&TaskResult{Changed: true})
	}

	if manageDir {
		// Ensure the parent directory exists (best-effort).
		mkdirResult := client.Run(ctx, sprintf("mkdir -p %q && chmod 700 %q && chown %s:%s %q",
			pathDir(authKeysPath), pathDir(authKeysPath), user, user, pathDir(authKeysPath)))
		if !mkdirResult.OK {
			return core.Ok(&TaskResult{Changed: false, Msg: "failed to prepare authorized_keys directory"})
		}
	}

	if exclusive {
		if uploadResult := client.Upload(ctx, newReader(line+"\n"), authKeysPath, 0600); !uploadResult.OK {
			return wrapFailure(uploadResult, "Executor.moduleAuthorizedKey", "upload authorised keys")
		}
		chmodResult := client.Run(ctx, sprintf("chmod 600 %q && chown %s:%s %q",
			authKeysPath, user, user, authKeysPath))
		if !chmodResult.OK {
			return core.Ok(&TaskResult{Changed: true})
		}
		return core.Ok(&TaskResult{Changed: true})
	}

	var updated string
	if content, ok := remoteFileText(ctx, client, authKeysPath); ok {
		updatedValue, changed := rewriteAuthorizedKeyContent(content, base, line)
		if !changed {
			return core.Ok(&TaskResult{Changed: false})
		}
		updated = updatedValue
	} else {
		updated = line + "\n"
	}
	if uploadResult := client.Upload(ctx, newReader(updated), authKeysPath, 0600); !uploadResult.OK {
		return wrapFailure(uploadResult, "Executor.moduleAuthorizedKey", "upload authorised keys")
	}
	chmodResult := client.Run(ctx, sprintf("chmod 600 %q && chown %s:%s %q",
		authKeysPath, user, user, authKeysPath))
	if !chmodResult.OK {
		return core.Ok(&TaskResult{Changed: true})
	}

	return core.Ok(&TaskResult{Changed: true})
}

func authorizedKeyLine(key, keyOptions, comment string) string {
	key = corexTrimSpace(key)
	keyOptions = corexTrimSpace(keyOptions)
	comment = corexTrimSpace(comment)

	if keyOptions == "" && comment == "" {
		return key
	}

	base := authorizedKeyBase(key)
	if base == "" {
		base = key
	}

	parts := make([]string, 0, 3)
	if keyOptions != "" {
		parts = append(parts, keyOptions)
	}
	if base != "" {
		parts = append(parts, base)
	}
	if comment != "" {
		parts = append(parts, comment)
	}
	return join(" ", parts)
}

func authorizedKeyBase(line string) string {
	line = corexTrimSpace(line)
	if line == "" {
		return ""
	}

	fields := fields(line)
	for i, field := range fields {
		if isAuthorizedKeyType(field) {
			if i+1 >= len(fields) {
				return field
			}
			return field + " " + fields[i+1]
		}
	}

	return line
}

func isAuthorizedKeyType(value string) bool {
	return hasPrefix(value, "ssh-") ||
		hasPrefix(value, "ecdsa-") ||
		hasPrefix(value, "sk-")
}

func authorizedKeyContainsBase(content, base string) bool {
	if content == "" || base == "" {
		return false
	}

	for _, line := range split(content, "\n") {
		if authorizedKeyBase(line) == base {
			return true
		}
	}

	return false
}

func sedExactLinePattern(value string) string {
	pattern := regexp.QuoteMeta(value)
	return replaceAll(pattern, "|", "\\|")
}

func rewriteAuthorizedKeyContent(content, base, line string) (string, bool) {
	if base == "" {
		base = authorizedKeyBase(line)
	}

	lines := split(content, "\n")
	matches := 0
	exactMatches := 0
	for _, current := range lines {
		if current == "" {
			continue
		}
		if authorizedKeyBase(current) != base {
			continue
		}
		matches++
		if current == line {
			exactMatches++
		}
	}

	if line != "" && matches == 1 && exactMatches == 1 {
		return content, false
	}
	if line == "" && matches == 0 {
		return content, false
	}

	kept := make([]string, 0, len(lines)+1)
	for _, current := range lines {
		if current == "" {
			continue
		}
		if authorizedKeyBase(current) == base {
			continue
		}
		kept = append(kept, current)
	}

	if line != "" {
		kept = append(kept, line)
	}
	if len(kept) == 0 {
		return "", true
	}

	return join("\n", kept) + "\n", true
}

func (e *Executor) moduleDockerCompose(ctx context.Context, client sshExecutorClient, args map[string]any) core.Result {
	projectSrc := getStringArg(args, "project_src", "")
	state := getStringArg(args, "state", "present")
	projectName := getStringArg(args, "project_name", "")
	files := normalizeStringArgs(args["files"])

	if projectSrc == "" {
		return core.Fail(coreerr.E("Executor.moduleDockerCompose", "project_src required", nil))
	}

	var cmdParts []string
	cmdParts = append(cmdParts, "cd", shellQuote(projectSrc), "&&", "docker", "compose")
	if projectName != "" {
		cmdParts = append(cmdParts, "-p", shellQuote(projectName))
	}
	for _, file := range files {
		cmdParts = append(cmdParts, "-f", shellQuote(file))
	}

	switch state {
	case "present":
		cmdParts = append(cmdParts, "up", "-d")
	case "absent":
		cmdParts = append(cmdParts, "down")
	case "stopped":
		cmdParts = append(cmdParts, "stop")
	case "restarted":
		cmdParts = append(cmdParts, "restart")
	default:
		cmdParts = append(cmdParts, "up", "-d")
	}

	cmd := join(" ", cmdParts)

	runResult := client.Run(ctx, cmd)
	run := commandRunValue(runResult)
	if !runResult.OK || run.ExitCode != 0 {
		return core.Ok(&TaskResult{Failed: true, Msg: run.Stderr, Stdout: run.Stdout, RC: run.ExitCode})
	}

	// Heuristic for changed
	changed := true
	if contains(run.Stdout, "Up to date") || contains(run.Stderr, "Up to date") {
		changed = false
	}

	return core.Ok(&TaskResult{Changed: changed, Stdout: run.Stdout})
}
