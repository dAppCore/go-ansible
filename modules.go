package ansible

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	coreio "dappco.re/go/core/io"
	coreerr "dappco.re/go/core/log"
	"gopkg.in/yaml.v3"
)

type sshFactsRunner interface {
	Run(ctx context.Context, cmd string) (string, string, int, error)
}

// executeModule dispatches to the appropriate module handler.
func (e *Executor) executeModule(ctx context.Context, host string, client sshExecutorClient, task *Task, play *Play) (*TaskResult, error) {
	module := NormalizeModule(task.Module)

	// Apply task-level become
	if task.Become != nil && *task.Become {
		// Save old state to restore
		oldBecome, oldUser, oldPass := client.BecomeState()

		client.SetBecome(true, task.BecomeUser, "")

		defer client.SetBecome(oldBecome, oldUser, oldPass)
	}

	if prefix := e.buildEnvironmentPrefix(host, task, play); prefix != "" {
		client = &environmentSSHClient{
			sshExecutorClient: client,
			prefix:            prefix,
		}
	}

	// Template the args
	args := e.templateArgs(task.Args, host, task)

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
		return e.moduleDebug(args)
	case "ansible.builtin.fail":
		return e.moduleFail(args)
	case "ansible.builtin.assert":
		return e.moduleAssert(args, host)
	case "ansible.builtin.set_fact":
		return e.moduleSetFact(args)
	case "ansible.builtin.add_host":
		return e.moduleAddHost(args)
	case "ansible.builtin.group_by":
		return e.moduleGroupBy(host, args)
	case "ansible.builtin.pause":
		return e.modulePause(ctx, args)
	case "ansible.builtin.wait_for":
		return e.moduleWaitFor(ctx, client, args)
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
		return e.moduleSetup(ctx, host, client, args)
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
		return nil, coreerr.E("Executor.executeModule", "unsupported module: "+module, nil)
	}
}

func remoteFileText(ctx context.Context, client sshExecutorClient, path string) (string, bool) {
	data, err := client.Download(ctx, path)
	if err != nil {
		return "", false
	}
	return string(data), true
}

func fileDiffData(path, before, after string) map[string]any {
	return map[string]any{
		"path":   path,
		"before": before,
		"after":  after,
	}
}

// templateArgs templates all string values in args.
func (e *Executor) templateArgs(args map[string]any, host string, task *Task) map[string]any {
	// Set inventory_hostname for templating
	e.vars["inventory_hostname"] = host

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

func (e *Executor) moduleShell(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		cmd = getStringArg(args, "cmd", "")
	}
	if cmd == "" {
		return nil, coreerr.E("Executor.moduleShell", "no command specified", nil)
	}

	skip, err := shouldSkipCommandModule(ctx, client, args)
	if err != nil {
		return nil, err
	}
	if skip {
		return &TaskResult{Changed: false}, nil
	}

	// Handle chdir
	if chdir := getStringArg(args, "chdir", ""); chdir != "" {
		cmd = sprintf("cd %q && %s", chdir, cmd)
	}

	stdout, stderr, rc, err := client.RunScript(ctx, cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error(), Stdout: stdout, Stderr: stderr, RC: rc}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
		Failed:  rc != 0,
	}, nil
}

func (e *Executor) moduleCommand(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		cmd = getStringArg(args, "cmd", "")
	}
	if cmd == "" {
		return nil, coreerr.E("Executor.moduleCommand", "no command specified", nil)
	}

	skip, err := shouldSkipCommandModule(ctx, client, args)
	if err != nil {
		return nil, err
	}
	if skip {
		return &TaskResult{Changed: false}, nil
	}

	// Handle chdir
	if chdir := getStringArg(args, "chdir", ""); chdir != "" {
		cmd = sprintf("cd %q && %s", chdir, cmd)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
		Failed:  rc != 0,
	}, nil
}

func shouldSkipCommandModule(ctx context.Context, client sshExecutorClient, args map[string]any) (bool, error) {
	if path := getStringArg(args, "creates", ""); path != "" {
		exists, err := client.FileExists(ctx, path)
		if err != nil {
			return false, coreerr.E("Executor.shouldSkipCommandModule", "creates check", err)
		}
		if exists {
			return true, nil
		}
	}

	if path := getStringArg(args, "removes", ""); path != "" {
		exists, err := client.FileExists(ctx, path)
		if err != nil {
			return false, coreerr.E("Executor.shouldSkipCommandModule", "removes check", err)
		}
		if !exists {
			return true, nil
		}
	}

	return false, nil
}

func (e *Executor) moduleRaw(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	cmd := getStringArg(args, "_raw_params", "")
	if cmd == "" {
		return nil, coreerr.E("Executor.moduleRaw", "no command specified", nil)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
	}, nil
}

func (e *Executor) moduleScript(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	script := getStringArg(args, "_raw_params", "")
	if script == "" {
		return nil, coreerr.E("Executor.moduleScript", "no script specified", nil)
	}

	// Read local script
	data, err := coreio.Local.Read(script)
	if err != nil {
		return nil, coreerr.E("Executor.moduleScript", "read script", err)
	}

	stdout, stderr, rc, err := client.RunScript(ctx, data)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	return &TaskResult{
		Changed: true,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      rc,
		Failed:  rc != 0,
	}, nil
}

// --- File Modules ---

func (e *Executor) moduleCopy(ctx context.Context, client sshExecutorClient, args map[string]any, host string, task *Task) (*TaskResult, error) {
	dest := getStringArg(args, "dest", "")
	if dest == "" {
		return nil, coreerr.E("Executor.moduleCopy", "dest required", nil)
	}

	var content string
	var err error

	if src := getStringArg(args, "src", ""); src != "" {
		content, err = coreio.Local.Read(src)
		if err != nil {
			return nil, coreerr.E("Executor.moduleCopy", "read src", err)
		}
	} else if c := getStringArg(args, "content", ""); c != "" {
		content = c
	} else {
		return nil, coreerr.E("Executor.moduleCopy", "src or content required", nil)
	}

	mode := fs.FileMode(0644)
	if m := getStringArg(args, "mode", ""); m != "" {
		if parsed, err := strconv.ParseInt(m, 8, 32); err == nil {
			mode = fs.FileMode(parsed)
		}
	}

	before, hasBefore := remoteFileText(ctx, client, dest)
	if hasBefore && before == content {
		if getStringArg(args, "owner", "") == "" && getStringArg(args, "group", "") == "" {
			return &TaskResult{Changed: false, Msg: sprintf("already up to date: %s", dest)}, nil
		}
	}

	err = client.Upload(ctx, newReader(content), dest, mode)
	if err != nil {
		return nil, err
	}

	// Handle owner/group (best-effort, errors ignored)
	if owner := getStringArg(args, "owner", ""); owner != "" {
		_, _, _, _ = client.Run(ctx, sprintf("chown %s %q", owner, dest))
	}
	if group := getStringArg(args, "group", ""); group != "" {
		_, _, _, _ = client.Run(ctx, sprintf("chgrp %s %q", group, dest))
	}

	result := &TaskResult{Changed: true, Msg: sprintf("copied to %s", dest)}
	if e.Diff {
		if hasBefore {
			result.Data = map[string]any{"diff": fileDiffData(dest, before, content)}
		}
	}
	return result, nil
}

func (e *Executor) moduleTemplate(ctx context.Context, client sshExecutorClient, args map[string]any, host string, task *Task) (*TaskResult, error) {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	if src == "" || dest == "" {
		return nil, coreerr.E("Executor.moduleTemplate", "src and dest required", nil)
	}

	// Process template
	content, err := e.TemplateFile(src, host, task)
	if err != nil {
		return nil, coreerr.E("Executor.moduleTemplate", "template", err)
	}

	mode := fs.FileMode(0644)
	if m := getStringArg(args, "mode", ""); m != "" {
		if parsed, err := strconv.ParseInt(m, 8, 32); err == nil {
			mode = fs.FileMode(parsed)
		}
	}

	before, hasBefore := remoteFileText(ctx, client, dest)
	if hasBefore && before == content {
		return &TaskResult{Changed: false, Msg: sprintf("already up to date: %s", dest)}, nil
	}

	err = client.Upload(ctx, newReader(content), dest, mode)
	if err != nil {
		return nil, err
	}

	result := &TaskResult{Changed: true, Msg: sprintf("templated to %s", dest)}
	if e.Diff {
		if hasBefore {
			result.Data = map[string]any{"diff": fileDiffData(dest, before, content)}
		}
	}
	return result, nil
}

func (e *Executor) moduleFile(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return nil, coreerr.E("Executor.moduleFile", "path required", nil)
	}

	state := getStringArg(args, "state", "file")

	switch state {
	case "directory":
		mode := getStringArg(args, "mode", "0755")
		cmd := sprintf("mkdir -p %q && chmod %s %q", path, mode, path)
		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}

	case "absent":
		cmd := sprintf("rm -rf %q", path)
		_, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
		}

	case "touch":
		cmd := sprintf("touch %q", path)
		_, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
		}

	case "link":
		src := getStringArg(args, "src", "")
		if src == "" {
			return nil, coreerr.E("Executor.moduleFile", "src required for link state", nil)
		}
		cmd := sprintf("ln -sf %q %q", src, path)
		_, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
		}

	case "file":
		// Ensure file exists and set permissions
		if mode := getStringArg(args, "mode", ""); mode != "" {
			_, _, _, _ = client.Run(ctx, sprintf("chmod %s %q", mode, path))
		}
	}

	// Handle owner/group (best-effort, errors ignored)
	if owner := getStringArg(args, "owner", ""); owner != "" {
		_, _, _, _ = client.Run(ctx, sprintf("chown %s %q", owner, path))
	}
	if group := getStringArg(args, "group", ""); group != "" {
		_, _, _, _ = client.Run(ctx, sprintf("chgrp %s %q", group, path))
	}
	if recurse := getBoolArg(args, "recurse", false); recurse {
		if owner := getStringArg(args, "owner", ""); owner != "" {
			_, _, _, _ = client.Run(ctx, sprintf("chown -R %s %q", owner, path))
		}
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleLineinfile(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return nil, coreerr.E("Executor.moduleLineinfile", "path required", nil)
	}

	line := getStringArg(args, "line", "")
	regexp := getStringArg(args, "regexp", "")
	state := getStringArg(args, "state", "present")
	backrefs := getBoolArg(args, "backrefs", false)

	if state == "absent" {
		if regexp != "" {
			cmd := sprintf("sed -i '/%s/d' %q", regexp, path)
			_, stderr, rc, _ := client.Run(ctx, cmd)
			if rc != 0 {
				return &TaskResult{Failed: true, Msg: stderr, RC: rc}, nil
			}
		}
	} else {
		// state == present
		if regexp != "" {
			// Replace line matching regexp.
			escapedLine := replaceAll(line, "/", "\\/")
			sedFlags := "-i"
			if backrefs {
				// When backrefs is enabled, Ansible only replaces matching lines
				// and does not append a new line when the pattern is absent.
				matchCmd := sprintf("grep -Eq %q %q", regexp, path)
				_, _, matchRC, _ := client.Run(ctx, matchCmd)
				if matchRC != 0 {
					return &TaskResult{Changed: false}, nil
				}
				sedFlags = "-E -i"
			}
			cmd := sprintf("sed %s 's/%s/%s/' %q", sedFlags, regexp, escapedLine, path)
			_, _, rc, _ := client.Run(ctx, cmd)
			if rc != 0 {
				if backrefs {
					return &TaskResult{Changed: false}, nil
				}
				// Line not found, append.
				cmd = sprintf("echo %q >> %q", line, path)
				_, _, _, _ = client.Run(ctx, cmd)
			}
		} else if line != "" {
			// Ensure line is present
			cmd := sprintf("grep -qxF %q %q || echo %q >> %q", line, path, line, path)
			_, _, _, _ = client.Run(ctx, cmd)
		}
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleStat(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		return nil, coreerr.E("Executor.moduleStat", "path required", nil)
	}

	stat, err := client.Stat(ctx, path)
	if err != nil {
		return nil, err
	}

	return &TaskResult{
		Changed: false,
		Data:    map[string]any{"stat": stat},
	}, nil
}

func (e *Executor) moduleSlurp(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "src", "")
	}
	if path == "" {
		return nil, coreerr.E("Executor.moduleSlurp", "path required", nil)
	}

	content, err := client.Download(ctx, path)
	if err != nil {
		return nil, err
	}

	encoded := base64.StdEncoding.EncodeToString(content)

	return &TaskResult{
		Changed: false,
		Data:    map[string]any{"content": encoded, "encoding": "base64"},
	}, nil
}

func (e *Executor) moduleFetch(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	if src == "" || dest == "" {
		return nil, coreerr.E("Executor.moduleFetch", "src and dest required", nil)
	}

	content, err := client.Download(ctx, src)
	if err != nil {
		return nil, err
	}

	// Create dest directory
	if err := coreio.Local.EnsureDir(pathDir(dest)); err != nil {
		return nil, err
	}

	if err := coreio.Local.Write(dest, string(content)); err != nil {
		return nil, err
	}

	return &TaskResult{Changed: true, Msg: sprintf("fetched %s to %s", src, dest)}, nil
}

func (e *Executor) moduleGetURL(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	url := getStringArg(args, "url", "")
	dest := getStringArg(args, "dest", "")
	if url == "" || dest == "" {
		return nil, coreerr.E("Executor.moduleGetURL", "url and dest required", nil)
	}

	// Use curl or wget
	cmd := sprintf("curl -fsSL -o %q %q || wget -q -O %q %q", dest, url, dest, url)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Set mode if specified (best-effort)
	if mode := getStringArg(args, "mode", ""); mode != "" {
		_, _, _, _ = client.Run(ctx, sprintf("chmod %s %q", mode, dest))
	}

	return &TaskResult{Changed: true}, nil
}

// --- Package Modules ---

func (e *Executor) moduleApt(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	updateCache := getBoolArg(args, "update_cache", false)

	var cmd string

	if updateCache {
		_, _, _, _ = client.Run(ctx, "apt-get update -qq")
	}

	switch state {
	case "present", "installed":
		if name != "" {
			cmd = sprintf("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq %s", name)
		}
	case "absent", "removed":
		cmd = sprintf("DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq %s", name)
	case "latest":
		cmd = sprintf("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --only-upgrade %s", name)
	}

	if cmd == "" {
		return &TaskResult{Changed: false}, nil
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleAptKey(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	url := getStringArg(args, "url", "")
	keyring := getStringArg(args, "keyring", "")
	state := getStringArg(args, "state", "present")

	if state == "absent" {
		if keyring != "" {
			_, _, _, _ = client.Run(ctx, sprintf("rm -f %q", keyring))
		}
		return &TaskResult{Changed: true}, nil
	}

	if url == "" {
		return nil, coreerr.E("Executor.moduleAptKey", "url required", nil)
	}

	var cmd string
	if keyring != "" {
		cmd = sprintf("curl -fsSL %q | gpg --dearmor -o %q", url, keyring)
	} else {
		cmd = sprintf("curl -fsSL %q | apt-key add -", url)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleAptRepository(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	repo := getStringArg(args, "repo", "")
	filename := getStringArg(args, "filename", "")
	state := getStringArg(args, "state", "present")

	if repo == "" {
		return nil, coreerr.E("Executor.moduleAptRepository", "repo required", nil)
	}

	if filename == "" {
		// Generate filename from repo
		filename = replaceAll(repo, " ", "-")
		filename = replaceAll(filename, "/", "-")
		filename = replaceAll(filename, ":", "")
	}

	path := sprintf("/etc/apt/sources.list.d/%s.list", filename)

	if state == "absent" {
		_, _, _, _ = client.Run(ctx, sprintf("rm -f %q", path))
		return &TaskResult{Changed: true}, nil
	}

	cmd := sprintf("echo %q > %q", repo, path)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Update apt cache (best-effort)
	if getBoolArg(args, "update_cache", true) {
		_, _, _, _ = client.Run(ctx, "apt-get update -qq")
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) modulePackage(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	// Detect package manager and delegate
	stdout, _, _, _ := client.Run(ctx, "which apt-get yum dnf 2>/dev/null | head -1")
	stdout = corexTrimSpace(stdout)

	switch {
	case contains(stdout, "dnf"):
		return e.moduleDnf(ctx, client, args)
	case contains(stdout, "yum"):
		return e.moduleYum(ctx, client, args)
	default:
		return e.moduleApt(ctx, client, args)
	}
}

func (e *Executor) moduleYum(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return e.moduleRPM(ctx, client, args, "yum")
}

func (e *Executor) moduleDnf(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	return e.moduleRPM(ctx, client, args, "dnf")
}

func (e *Executor) moduleRPM(ctx context.Context, client sshExecutorClient, args map[string]any, manager string) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	updateCache := getBoolArg(args, "update_cache", false)

	if updateCache {
		_, _, _, _ = client.Run(ctx, sprintf("%s makecache -y", manager))
	}

	var cmd string
	switch state {
	case "present", "installed":
		if name != "" {
			cmd = sprintf("%s install -y -q %s", manager, name)
		}
	case "absent", "removed":
		if name != "" {
			cmd = sprintf("%s remove -y -q %s", manager, name)
		}
	case "latest":
		if name != "" {
			if manager == "dnf" {
				cmd = sprintf("%s upgrade -y -q %s", manager, name)
			} else {
				cmd = sprintf("%s update -y -q %s", manager, name)
			}
		}
	}

	if cmd == "" {
		return &TaskResult{Changed: false}, nil
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) modulePip(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")
	executable := getStringArg(args, "executable", "pip3")

	var cmd string
	switch state {
	case "present", "installed":
		cmd = sprintf("%s install %s", executable, name)
	case "absent", "removed":
		cmd = sprintf("%s uninstall -y %s", executable, name)
	case "latest":
		cmd = sprintf("%s install --upgrade %s", executable, name)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- Service Modules ---

func (e *Executor) moduleService(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "")
	enabled := args["enabled"]

	if name == "" {
		return nil, coreerr.E("Executor.moduleService", "name required", nil)
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
		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
	}

	return &TaskResult{Changed: len(cmds) > 0}, nil
}

func (e *Executor) moduleSystemd(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	// systemd is similar to service
	if getBoolArg(args, "daemon_reload", false) {
		_, _, _, _ = client.Run(ctx, "systemctl daemon-reload")
	}

	return e.moduleService(ctx, client, args)
}

// --- User/Group Modules ---

func (e *Executor) moduleUser(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")

	if name == "" {
		return nil, coreerr.E("Executor.moduleUser", "name required", nil)
	}

	if state == "absent" {
		cmd := sprintf("userdel -r %s 2>/dev/null || true", name)
		_, _, _, _ = client.Run(ctx, cmd)
		return &TaskResult{Changed: true}, nil
	}

	// Build useradd/usermod command
	var opts []string

	if uid := getStringArg(args, "uid", ""); uid != "" {
		opts = append(opts, "-u", uid)
	}
	if group := getStringArg(args, "group", ""); group != "" {
		opts = append(opts, "-g", group)
	}
	if groups := getStringArg(args, "groups", ""); groups != "" {
		opts = append(opts, "-G", groups)
	}
	if home := getStringArg(args, "home", ""); home != "" {
		opts = append(opts, "-d", home)
	}
	if shell := getStringArg(args, "shell", ""); shell != "" {
		opts = append(opts, "-s", shell)
	}
	if getBoolArg(args, "system", false) {
		opts = append(opts, "-r")
	}
	if getBoolArg(args, "create_home", true) {
		opts = append(opts, "-m")
	}

	// Try usermod first, then useradd
	optsStr := join(" ", opts)
	var cmd string
	if optsStr == "" {
		cmd = sprintf("id %s >/dev/null 2>&1 || useradd %s", name, name)
	} else {
		cmd = sprintf("id %s >/dev/null 2>&1 && usermod %s %s || useradd %s %s",
			name, optsStr, name, optsStr, name)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleGroup(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	state := getStringArg(args, "state", "present")

	if name == "" {
		return nil, coreerr.E("Executor.moduleGroup", "name required", nil)
	}

	if state == "absent" {
		cmd := sprintf("groupdel %s 2>/dev/null || true", name)
		_, _, _, _ = client.Run(ctx, cmd)
		return &TaskResult{Changed: true}, nil
	}

	var opts []string
	if gid := getStringArg(args, "gid", ""); gid != "" {
		opts = append(opts, "-g", gid)
	}
	if getBoolArg(args, "system", false) {
		opts = append(opts, "-r")
	}

	cmd := sprintf("getent group %s >/dev/null 2>&1 || groupadd %s %s",
		name, join(" ", opts), name)

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

// --- HTTP Module ---

func (e *Executor) moduleURI(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	url := getStringArg(args, "url", "")
	method := getStringArg(args, "method", "GET")
	bodyFormat := lower(getStringArg(args, "body_format", ""))
	returnContent := getBoolArg(args, "return_content", false)

	if url == "" {
		return nil, coreerr.E("Executor.moduleURI", "url required", nil)
	}

	var curlOpts []string
	curlOpts = append(curlOpts, "-s", "-S")
	curlOpts = append(curlOpts, "-X", method)

	// Headers
	if headers, ok := args["headers"].(map[string]any); ok {
		for k, v := range headers {
			curlOpts = append(curlOpts, "-H", sprintf("%q", sprintf("%s: %v", k, v)))
		}
	}

	// Body
	if body := args["body"]; body != nil {
		bodyText, err := renderURIBody(body, bodyFormat)
		if err != nil {
			return nil, coreerr.E("Executor.moduleURI", "render body", err)
		}
		if bodyText != "" {
			curlOpts = append(curlOpts, "-d", sprintf("%q", bodyText))
			if bodyFormat == "json" && !hasHeaderIgnoreCase(headersMap(args), "Content-Type") {
				curlOpts = append(curlOpts, "-H", "\"Content-Type: application/json\"")
			}
		}
	}

	// Status code
	curlOpts = append(curlOpts, "-w", "\\n%{http_code}")

	cmd := sprintf("curl %s %q", join(" ", curlOpts), url)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil {
		return &TaskResult{Failed: true, Msg: err.Error()}, nil
	}

	// Parse status code from last line
	lines := split(stdout, "\n")
	statusCode := 0
	content := ""
	if len(lines) > 0 {
		statusText := corexTrimSpace(lines[len(lines)-1])
		statusCode, _ = strconv.Atoi(statusText)
		if len(lines) > 1 {
			content = join("\n", lines[:len(lines)-1])
		}
	}

	// Check expected status codes.
	expectedStatuses := normalizeStatusCodes(args["status_code"], 200)
	failed := rc != 0 || !containsInt(expectedStatuses, statusCode)

	data := map[string]any{"status": statusCode}
	if returnContent {
		data["content"] = content
	}

	return &TaskResult{
		Changed: false,
		Failed:  failed,
		Stdout:  stdout,
		Stderr:  stderr,
		RC:      statusCode,
		Data:    data,
	}, nil
}

func renderURIBody(body any, bodyFormat string) (string, error) {
	switch bodyFormat {
	case "", "raw":
		return sprintf("%v", body), nil
	case "json":
		switch v := body.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			data, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(data), nil
		}
	default:
		return sprintf("%v", body), nil
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

func (e *Executor) moduleDebug(args map[string]any) (*TaskResult, error) {
	msg := getStringArg(args, "msg", "")
	if v, ok := args["var"]; ok {
		msg = sprintf("%v = %v", v, e.vars[sprintf("%v", v)])
	}

	return &TaskResult{
		Changed: false,
		Msg:     msg,
	}, nil
}

func (e *Executor) moduleFail(args map[string]any) (*TaskResult, error) {
	msg := getStringArg(args, "msg", "Failed as requested")
	return &TaskResult{
		Failed: true,
		Msg:    msg,
	}, nil
}

func (e *Executor) moduleAssert(args map[string]any, host string) (*TaskResult, error) {
	that, ok := args["that"]
	if !ok {
		return nil, coreerr.E("Executor.moduleAssert", "'that' required", nil)
	}

	conditions := normalizeConditions(that)
	for _, cond := range conditions {
		if !e.evalCondition(cond, host) {
			msg := getStringArg(args, "fail_msg", sprintf("Assertion failed: %s", cond))
			return &TaskResult{Failed: true, Msg: msg}, nil
		}
	}

	return &TaskResult{Changed: false, Msg: "All assertions passed"}, nil
}

func (e *Executor) moduleSetFact(args map[string]any) (*TaskResult, error) {
	for k, v := range args {
		if k != "cacheable" {
			e.vars[k] = v
		}
	}
	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleAddHost(args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	if name == "" {
		name = getStringArg(args, "hostname", "")
	}
	if name == "" {
		return nil, coreerr.E("Executor.moduleAddHost", "name required", nil)
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
	if host == nil {
		host = &Host{}
	}
	if host.Vars == nil {
		host.Vars = make(map[string]any)
	}

	if v := getStringArg(args, "ansible_host", ""); v != "" {
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
			host.AnsiblePort = port
		}
	}
	if v := getStringArg(args, "ansible_user", ""); v != "" {
		host.AnsibleUser = v
	}
	if v := getStringArg(args, "ansible_password", ""); v != "" {
		host.AnsiblePassword = v
	}
	if v := getStringArg(args, "ansible_ssh_private_key_file", ""); v != "" {
		host.AnsibleSSHPrivateKeyFile = v
	}
	if v := getStringArg(args, "ansible_connection", ""); v != "" {
		host.AnsibleConnection = v
	}
	if v := getStringArg(args, "ansible_become_password", ""); v != "" {
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
		host.Vars[key] = val
	}

	if e.inventory.All.Hosts == nil {
		e.inventory.All.Hosts = make(map[string]*Host)
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

	return &TaskResult{Changed: true, Msg: msg, Data: data}, nil
}

func (e *Executor) moduleGroupBy(host string, args map[string]any) (*TaskResult, error) {
	key := getStringArg(args, "key", "")
	if key == "" {
		key = getStringArg(args, "_raw_params", "")
	}
	if key == "" {
		return nil, coreerr.E("Executor.moduleGroupBy", "key required", nil)
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
	return &TaskResult{
		Changed: !alreadyMember,
		Msg:     msg,
		Data:    map[string]any{"host": host, "group": key},
	}, nil
}

func (e *Executor) modulePause(ctx context.Context, args map[string]any) (*TaskResult, error) {
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

	if duration > 0 {
		timer := time.NewTimer(duration)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return &TaskResult{Changed: false}, nil
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

func (e *Executor) moduleWaitFor(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	port := 0
	if p, ok := args["port"].(int); ok {
		port = p
	}
	path := getStringArg(args, "path", "")
	host := getStringArg(args, "host", "127.0.0.1")
	state := getStringArg(args, "state", "started")
	searchRegex := getStringArg(args, "search_regex", "")
	timeout := 300
	if t, ok := args["timeout"].(int); ok {
		timeout = t
	}
	var compiledRegex *regexp.Regexp
	if searchRegex != "" {
		var err error
		compiledRegex, err = regexp.Compile(searchRegex)
		if err != nil {
			return nil, coreerr.E("Executor.moduleWaitFor", "compile search_regex", err)
		}
	}

	if path != "" {
		deadline := time.NewTimer(time.Duration(timeout) * time.Second)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer deadline.Stop()
		defer ticker.Stop()

		for {
			exists, err := client.FileExists(ctx, path)
			if err != nil {
				return &TaskResult{Failed: true, Msg: err.Error()}, nil
			}

			satisfied := false
			switch state {
			case "absent":
				satisfied = !exists
				if exists && compiledRegex != nil {
					data, err := client.Download(ctx, path)
					if err == nil {
						satisfied = !compiledRegex.Match(data)
					}
				}
			default:
				satisfied = exists
				if satisfied && compiledRegex != nil {
					data, err := client.Download(ctx, path)
					if err != nil {
						satisfied = false
					} else {
						satisfied = compiledRegex.Match(data)
					}
				}
			}
			if satisfied {
				return &TaskResult{Changed: false}, nil
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-deadline.C:
				return &TaskResult{Failed: true, Msg: "wait_for timed out", RC: 1}, nil
			case <-ticker.C:
			}
		}
	}

	if port > 0 && state == "started" {
		cmd := sprintf("timeout %d bash -c 'until nc -z %s %d; do sleep 1; done'",
			timeout, host, port)
		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
		return &TaskResult{Changed: false}, nil
	}

	if port > 0 && state == "absent" {
		cmd := sprintf("timeout %d bash -c 'until ! nc -z %s %d; do sleep 1; done'",
			timeout, host, port)
		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
		return &TaskResult{Changed: false}, nil
	}

	return &TaskResult{Changed: false}, nil
}

func (e *Executor) moduleGit(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	repo := getStringArg(args, "repo", "")
	dest := getStringArg(args, "dest", "")
	version := getStringArg(args, "version", "HEAD")

	if repo == "" || dest == "" {
		return nil, coreerr.E("Executor.moduleGit", "repo and dest required", nil)
	}

	// Check if dest exists
	exists, _ := client.FileExists(ctx, dest+"/.git")

	var cmd string
	if exists {
		// Fetch and checkout (force to ensure clean state)
		cmd = sprintf("cd %q && git fetch --all && git checkout --force %q", dest, version)
	} else {
		cmd = sprintf("git clone %q %q && cd %q && git checkout %q",
			repo, dest, dest, version)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleUnarchive(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	src := getStringArg(args, "src", "")
	dest := getStringArg(args, "dest", "")
	remote := getBoolArg(args, "remote_src", false)

	if src == "" || dest == "" {
		return nil, coreerr.E("Executor.moduleUnarchive", "src and dest required", nil)
	}

	// Create dest directory (best-effort)
	_, _, _, _ = client.Run(ctx, sprintf("mkdir -p %q", dest))

	var cmd string
	if !remote {
		// Upload local file first
		data, err := coreio.Local.Read(src)
		if err != nil {
			return nil, coreerr.E("Executor.moduleUnarchive", "read src", err)
		}
		tmpPath := "/tmp/ansible_unarchive_" + pathBase(src)
		err = client.Upload(ctx, newReader(data), tmpPath, 0644)
		if err != nil {
			return nil, err
		}
		src = tmpPath
		defer func() { _, _, _, _ = client.Run(ctx, sprintf("rm -f %q", tmpPath)) }()
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

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleArchive(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	dest := getStringArg(args, "dest", "")
	format := lower(getStringArg(args, "format", ""))
	paths := archivePaths(args)

	if dest == "" || len(paths) == 0 {
		return nil, coreerr.E("Executor.moduleArchive", "path and dest required", nil)
	}

	// Create the parent directory first so archive creation does not fail.
	_, _, _, _ = client.Run(ctx, sprintf("mkdir -p %q", pathDir(dest)))

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

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	deleteOnSuccess = getBoolArg(args, "remove", false)
	if deleteOnSuccess {
		_, _, _, _ = client.Run(ctx, sprintf("rm -rf %s", join(" ", quoteArgs(paths))))
	}

	return &TaskResult{Changed: true}, nil
}

func archivePaths(args map[string]any) []string {
	raw, ok := args["path"]
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

func (e *Executor) moduleHostname(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	if name == "" {
		return nil, coreerr.E("Executor.moduleHostname", "name required", nil)
	}

	// Set hostname
	cmd := sprintf("hostnamectl set-hostname %q || hostname %q", name, name)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Update /etc/hosts if needed (best-effort)
	_, _, _, _ = client.Run(ctx, sprintf("sed -i 's/127.0.1.1.*/127.0.1.1\t%s/' /etc/hosts", name))

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleSysctl(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	value := getStringArg(args, "value", "")
	state := getStringArg(args, "state", "present")
	reload := getBoolArg(args, "reload", false)

	if name == "" {
		return nil, coreerr.E("Executor.moduleSysctl", "name required", nil)
	}

	if state == "absent" {
		// Remove from sysctl.conf
		cmd := sprintf("sed -i '/%s/d' /etc/sysctl.conf", name)
		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}

		if reload {
			stdout, stderr, rc, err = client.Run(ctx, "sysctl -p")
			if err != nil || rc != 0 {
				return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
			}
		}
		return &TaskResult{Changed: true}, nil
	}

	// Set value
	cmd := sprintf("sysctl -w %s=%s", name, value)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Persist if requested (best-effort)
	if getBoolArg(args, "sysctl_set", true) {
		cmd = sprintf("grep -q '^%s' /etc/sysctl.conf && sed -i 's/^%s.*/%s=%s/' /etc/sysctl.conf || echo '%s=%s' >> /etc/sysctl.conf",
			name, name, name, value, name, value)
		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
	}

	if reload {
		stdout, stderr, rc, err := client.Run(ctx, "sysctl -p")
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleCron(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	name := getStringArg(args, "name", "")
	job := getStringArg(args, "job", "")
	state := getStringArg(args, "state", "present")
	user := getStringArg(args, "user", "root")

	minute := getStringArg(args, "minute", "*")
	hour := getStringArg(args, "hour", "*")
	day := getStringArg(args, "day", "*")
	month := getStringArg(args, "month", "*")
	weekday := getStringArg(args, "weekday", "*")

	if state == "absent" {
		if name != "" {
			// Remove by name (comment marker)
			cmd := sprintf("crontab -u %s -l 2>/dev/null | grep -v '# %s' | grep -v '%s' | crontab -u %s -",
				user, name, job, user)
			_, _, _, _ = client.Run(ctx, cmd)
		}
		return &TaskResult{Changed: true}, nil
	}

	// Build cron entry
	schedule := sprintf("%s %s %s %s %s", minute, hour, day, month, weekday)
	entry := sprintf("%s %s # %s", schedule, job, name)

	// Add to crontab
	cmd := sprintf("(crontab -u %s -l 2>/dev/null | grep -v '# %s' ; echo %q) | crontab -u %s -",
		user, name, entry, user)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleBlockinfile(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	path := getStringArg(args, "path", "")
	if path == "" {
		path = getStringArg(args, "dest", "")
	}
	if path == "" {
		return nil, coreerr.E("Executor.moduleBlockinfile", "path required", nil)
	}

	block := getStringArg(args, "block", "")
	marker := getStringArg(args, "marker", "# {mark} ANSIBLE MANAGED BLOCK")
	state := getStringArg(args, "state", "present")
	create := getBoolArg(args, "create", false)

	beginMarker := replaceN(marker, "{mark}", "BEGIN", 1)
	endMarker := replaceN(marker, "{mark}", "END", 1)

	if state == "absent" {
		// Remove block
		cmd := sprintf("sed -i '/%s/,/%s/d' %q",
			replaceAll(beginMarker, "/", "\\/"),
			replaceAll(endMarker, "/", "\\/"),
			path)
		_, _, _, _ = client.Run(ctx, cmd)
		return &TaskResult{Changed: true}, nil
	}

	// Create file if needed (best-effort)
	if create {
		_, _, _, _ = client.Run(ctx, sprintf("touch %q", path))
	}

	// Remove existing block and add new one
	escapedBlock := replaceAll(block, "'", "'\\''")
	cmd := sprintf(`
sed -i '/%s/,/%s/d' %q 2>/dev/null || true
cat >> %q << 'BLOCK_EOF'
%s
%s
%s
BLOCK_EOF
`, replaceAll(beginMarker, "/", "\\/"),
		replaceAll(endMarker, "/", "\\/"),
		path, path, beginMarker, escapedBlock, endMarker)

	stdout, stderr, rc, err := client.RunScript(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleIncludeVars(args map[string]any) (*TaskResult, error) {
	file := getStringArg(args, "file", "")
	if file == "" {
		file = getStringArg(args, "_raw_params", "")
	}
	dir := getStringArg(args, "dir", "")
	name := getStringArg(args, "name", "")
	hashBehaviour := lower(getStringArg(args, "hash_behaviour", "replace"))

	if file == "" && dir == "" {
		return &TaskResult{Changed: false}, nil
	}

	loaded := make(map[string]any)
	var sources []string
	loadFile := func(path string) error {
		data, err := coreio.Local.Read(path)
		if err != nil {
			return coreerr.E("Executor.moduleIncludeVars", "read vars file", err)
		}

		var vars map[string]any
		if err := yaml.Unmarshal([]byte(data), &vars); err != nil {
			return coreerr.E("Executor.moduleIncludeVars", "parse vars file", err)
		}

		mergeVars(loaded, vars, hashBehaviour == "merge")
		return nil
	}

	if file != "" {
		sources = append(sources, file)
		if err := loadFile(file); err != nil {
			return nil, err
		}
	}

	if dir != "" {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, coreerr.E("Executor.moduleIncludeVars", "read vars dir", err)
		}

		var files []string
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			ext := lower(filepath.Ext(entry.Name()))
			if ext == ".yml" || ext == ".yaml" {
				files = append(files, joinPath(dir, entry.Name()))
			}
		}
		sort.Strings(files)

		for _, path := range files {
			sources = append(sources, path)
			if err := loadFile(path); err != nil {
				return nil, err
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

	return &TaskResult{Changed: true, Msg: msg}, nil
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

func (e *Executor) moduleMeta(args map[string]any) (*TaskResult, error) {
	// meta module controls play execution
	// Most actions are no-ops for us, but we preserve the requested action so
	// the executor can apply side effects such as handler flushing.
	action := getStringArg(args, "_raw_params", "")
	if action == "" {
		action = getStringArg(args, "free_form", "")
	}

	result := &TaskResult{Changed: action == "clear_facts"}
	if action != "" {
		result.Data = map[string]any{"action": action}
	}

	return result, nil
}

func (e *Executor) moduleSetup(ctx context.Context, host string, client sshFactsRunner, args map[string]any) (*TaskResult, error) {
	facts, err := e.collectFacts(ctx, client)
	if err != nil {
		return nil, err
	}

	factMap := factsToMap(facts)
	factMap = applyGatherSubsetFilter(factMap, normalizeStringList(args["gather_subset"]))
	filteredFactMap := filterFactsMap(factMap, normalizeStringList(args["filter"]))
	filteredFacts := factsFromMap(filteredFactMap)

	e.mu.Lock()
	e.facts[host] = filteredFacts
	e.mu.Unlock()

	return &TaskResult{
		Changed: false,
		Msg:     "facts gathered",
		Data:    map[string]any{"ansible_facts": filteredFactMap},
	}, nil
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
		return nil
	default:
		return nil
	}
}

func (e *Executor) collectFacts(ctx context.Context, client sshFactsRunner) (*Facts, error) {
	facts := &Facts{}

	stdout, _, _, err := client.Run(ctx, "hostname -f 2>/dev/null || hostname")
	if err == nil {
		facts.FQDN = corexTrimSpace(stdout)
	}

	stdout, _, _, err = client.Run(ctx, "hostname -s 2>/dev/null || hostname")
	if err == nil {
		facts.Hostname = corexTrimSpace(stdout)
	}

	stdout, _, _, err = client.Run(ctx, "cat /etc/os-release 2>/dev/null | grep -E '^(ID|VERSION_ID|NAME)=' | head -3")
	if err == nil {
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

	stdout, _, _, err = client.Run(ctx, "uname -m")
	if err == nil {
		facts.Architecture = corexTrimSpace(stdout)
	}

	stdout, _, _, err = client.Run(ctx, "uname -r")
	if err == nil {
		facts.Kernel = corexTrimSpace(stdout)
	}

	stdout, _, _, err = client.Run(ctx, "nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null")
	if err == nil {
		if n, parseErr := strconv.Atoi(corexTrimSpace(stdout)); parseErr == nil {
			facts.CPUs = n
		}
	}

	stdout, _, _, err = client.Run(ctx, "free -m 2>/dev/null | awk '/^Mem:/ {print $2}'")
	if err == nil {
		if n, parseErr := strconv.ParseInt(corexTrimSpace(stdout), 10, 64); parseErr == nil {
			facts.Memory = n
		}
	}

	stdout, _, _, err = client.Run(ctx, "hostname -I 2>/dev/null | awk '{print $1}'")
	if err == nil {
		facts.IPv4 = corexTrimSpace(stdout)
	}

	return facts, nil
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
			matched, err := path.Match(pattern, key)
			if err != nil {
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

func (e *Executor) moduleReboot(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	preRebootDelay := 0
	if d, ok := args["pre_reboot_delay"].(int); ok {
		preRebootDelay = d
	}

	msg := getStringArg(args, "msg", "Reboot initiated by Ansible")

	if preRebootDelay > 0 {
		cmd := sprintf("sleep %d && shutdown -r now '%s' &", preRebootDelay, msg)
		_, _, _, _ = client.Run(ctx, cmd)
	} else {
		_, _, _, _ = client.Run(ctx, sprintf("shutdown -r now '%s' &", msg))
	}

	return &TaskResult{Changed: true, Msg: "Reboot initiated"}, nil
}

func (e *Executor) moduleUFW(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	rule := getStringArg(args, "rule", "")
	port := getStringArg(args, "port", "")
	proto := getStringArg(args, "proto", "tcp")
	state := getStringArg(args, "state", "")

	var cmd string

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
			stdout, stderr, rc, err := client.Run(ctx, cmd)
			if err != nil || rc != 0 {
				return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
			}
			return &TaskResult{Changed: true}, nil
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

		stdout, stderr, rc, err := client.Run(ctx, cmd)
		if err != nil || rc != 0 {
			return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
		}
	}

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleAuthorizedKey(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	user := getStringArg(args, "user", "")
	key := getStringArg(args, "key", "")
	state := getStringArg(args, "state", "present")

	if user == "" || key == "" {
		return nil, coreerr.E("Executor.moduleAuthorizedKey", "user and key required", nil)
	}

	// Get user's home directory
	stdout, _, _, err := client.Run(ctx, sprintf("getent passwd %s | cut -d: -f6", user))
	if err != nil {
		return nil, coreerr.E("Executor.moduleAuthorizedKey", "get home dir", err)
	}
	home := corexTrimSpace(stdout)
	if home == "" {
		home = "/root"
		if user != "root" {
			home = "/home/" + user
		}
	}

	authKeysPath := joinPath(home, ".ssh", "authorized_keys")

	if state == "absent" {
		// Remove key
		escapedKey := replaceAll(key, "/", "\\/")
		cmd := sprintf("sed -i '/%s/d' %q 2>/dev/null || true", escapedKey[:40], authKeysPath)
		_, _, _, _ = client.Run(ctx, cmd)
		return &TaskResult{Changed: true}, nil
	}

	// Ensure .ssh directory exists (best-effort)
	_, _, _, _ = client.Run(ctx, sprintf("mkdir -p %q && chmod 700 %q && chown %s:%s %q",
		pathDir(authKeysPath), pathDir(authKeysPath), user, user, pathDir(authKeysPath)))

	// Add key if not present
	cmd := sprintf("grep -qF %q %q 2>/dev/null || echo %q >> %q",
		key[:40], authKeysPath, key, authKeysPath)
	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Fix permissions (best-effort)
	_, _, _, _ = client.Run(ctx, sprintf("chmod 600 %q && chown %s:%s %q",
		authKeysPath, user, user, authKeysPath))

	return &TaskResult{Changed: true}, nil
}

func (e *Executor) moduleDockerCompose(ctx context.Context, client sshExecutorClient, args map[string]any) (*TaskResult, error) {
	projectSrc := getStringArg(args, "project_src", "")
	state := getStringArg(args, "state", "present")

	if projectSrc == "" {
		return nil, coreerr.E("Executor.moduleDockerCompose", "project_src required", nil)
	}

	var cmd string
	switch state {
	case "present":
		cmd = sprintf("cd %q && docker compose up -d", projectSrc)
	case "absent":
		cmd = sprintf("cd %q && docker compose down", projectSrc)
	case "restarted":
		cmd = sprintf("cd %q && docker compose restart", projectSrc)
	default:
		cmd = sprintf("cd %q && docker compose up -d", projectSrc)
	}

	stdout, stderr, rc, err := client.Run(ctx, cmd)
	if err != nil || rc != 0 {
		return &TaskResult{Failed: true, Msg: stderr, Stdout: stdout, RC: rc}, nil
	}

	// Heuristic for changed
	changed := true
	if contains(stdout, "Up to date") || contains(stderr, "Up to date") {
		changed = false
	}

	return &TaskResult{Changed: changed, Stdout: stdout}, nil
}
